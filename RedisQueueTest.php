<?php
use PHPUnit\Framework\Testcase;

require_once('RedisQueue.php');

class RedisQueueMock extends RedisQueue{
    public function getRange($start = '-', $end = '+', $count = null){
        return parent::getRange($start, $end, $count);
    }

    public function getPendingInfo(){
        return parent::getPendingInfo();
    }


    public function getPending($count = 1, $start='-', $end='+', $consumer = null){
        return parent::getPending($count, $start, $end, $consumer);
    }
};

class RedisQueueTest extends TestCase{
    protected $_config = [
        'server' => '10.160.75.237:6379',
        'stream' => 'balltube',    
        'consumer' => 'normal',
    ];

    public function setUp():void{
        $this->_q = new RedisQueueMock($this->_config); 
    }

    public function testAddTask(){
        $task = ['task'=>1];

        $taskID = $this->_q->addTask($task);

        $allTask = $this->_q->getRange($taskID, $taskID);
        $realTask = $allTask[$taskID];

        $this->assertEquals($task, $realTask);
    }

    //普通取任务
    public function testGetTask(){
        $task = ['task'=>0];
        $taskID = $this->_q->addTask($task);

        $allTask = $this->_q->getTask();
        $realTask = $allTask[$taskID];  

        $this->assertEquals($task, $realTask);
    }

    //取特定条数
    public function testGetTask2(){
        for($i=0; $i<3; $i++){
            $task = ['task'=>$i];
            $taskID = $this->_q->addTask($task);
        }

        $count = 2;
        $realTask = $this->_q->getTask(null, $count);
        $realCount = count($realTask);

        $this->assertEquals($count, $realCount);
    }

    //block的情况
    public function testGetTask3(){
        $start = gettimeofday(true);

        $block = 1000;
        $this->_q->getTask($block);
        $end = gettimeofday(true);

        $diff = $end - $start;

        $this->assertTrue($diff>$block/1000);
    }

    public function testGetPending(){
        for($i=0; $i<3; $i++){
            $task = ['task'=>$i];
            $taskID = $this->_q->addTask($task);
        }

        $count = 2;
        $realTask = $this->_q->getTask(null, $count);

        //按条数取
        $task = $this->_q->getPending($count);
        $realTaskNum = count($task);

        $this->assertEquals($count, $realTaskNum);

        //按条数+特定consumer, 不存在的consumer
        $consumer = 'nothing';
        $task = $this->_q->getPending($count, '-', '+', $consumer);
        $realTaskNum = count($task);

        $this->assertEquals(0, $realTaskNum);

        //按条数+特定consumer, 存在的consumer
        $task = $this->_q->getPending($count, '-', '+', $this->_config['consumer']);
        $realTaskNum = count($task);

        $this->assertEquals($count, $realTaskNum);
    }

    public function testDelTask(){
        $task = ['task'=>0];
        $taskID = $this->_q->addTask($task);

        $this->_q->getTask();
        $this->_q->delTask($taskID);

        //stream为空
        $task = $this->_q->getRange($taskID, $taskID);
        $expect = 0;
        $real = count($task);
        $this->assertEquals($expect, $real);
        //pending为空
        $task = $this->_q->getPending(1, $taskID, $taskID);
        $expect = 0;
        $real = count($task);
        $this->assertEquals($expect, $real);
    }

    public function testPendingRestore(){
        $taskNum = 100;
        $idleTime = 2;
        //第一次读的任务数
        $firstNum = 65;
        $perPage = 17;

        for($i=0; $i<$taskNum; $i++){
            $task = ['task'=>$i];
            $taskID = $this->_q->addTask($task);
        }
    
        $firstTasks = $this->_q->getTask(null, $firstNum);
        sleep($idleTime);
        $this->_q->getTask(null, $taskNum - $firstNum);
        $num = $this->_q->pendingRestore($idleTime*1000, $perPage);
        //restoreNum正常
        $this->assertEquals($firstNum, $num);

        //pending剩余数据正常
        $info = $this->_q->getPendingInfo();
        $left = $info[0];
        $this->assertEquals($taskNum-$firstNum, $left);
    
        
        //从队列中取出pending数据正常
        $tasks = $this->_q->getTask(null, $firstNum*2);
        $realTasks = array_values($tasks);
        $expectTasks = array_values($firstTasks);

        $this->assertEquals($expectTasks, $realTasks);
    }

    
    public function testPendingClaim(){
        $taskNum = 67;
        $idleTime = 1;

        for($i=0; $i<$taskNum; $i++){
            $task = ['task'=>$i];
            $taskID = $this->_q->addTask($task);
        }

        $tasks = $this->_q->getTask(null, $taskNum);
        sleep($idleTime);

        //没传consumer
        $res = $this->_q->pendingClaim($idleTime*1000);
        $this->assertFalse($res);

        //正常claim
        $newConsumer = 'pendingConsumer';
        $perPage = 20;
        $claimNum = $this->_q->pendingClaim($idleTime*1000, $newConsumer, $perPage);
        $this->assertEquals($taskNum, $claimNum);

        $conf = $this->_config;
        $conf['consumer'] = $newConsumer;
        $newQ = new RedisQueue($conf);

        //新消费者可以取出claim后的历史未完成任务
        $pendingTasks = $newQ->getTask(null, $taskNum, 0);
        $this->assertEquals($tasks, $pendingTasks);

        //老的consumer pending无任务
        $oldPending = $this->_q->getPending($taskNum, '-', '+', $this->_config['consumer']);

        $res = $this->_q->getTask(null, $taskNum, 0);
        $this->assertEquals([], $res);
    }

    /*
     * 部分pending数据满足claim条件
     * */
    public function testPendingClaim2(){
        $taskNum = 100;
        $idleTime = 2;

        for($i=0; $i<$taskNum; $i++){
            $task = ['task'=>$i];
            $taskID = $this->_q->addTask($task);
        }
            
        $firstNum = 56;
        $tasks = $this->_q->getTask(null, $firstNum);
        sleep($idleTime);
        $tasks = $this->_q->getTask(null, $taskNum - $firstNum);
        $newConsumer = 'pendingConsumer';
        $claimNum = $this->_q->pendingClaim($idleTime*1000, $newConsumer);
        $this->assertEquals($firstNum, $claimNum);
    }

    public function tearDown():void{
        $this->_q->destoryStream();
    }
}
