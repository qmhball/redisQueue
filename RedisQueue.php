<?php
/*
 * 需要redis-server5.0以上 
 * php-redis扩展版本要适配redis-5.0
 * 
 * 使用redis stream仿照beanstalk封装的队列服务
 */
class RedisQueue{
    protected $_mRedis = null;
    protected $_mStream = '';
    protected $_mGroup = '';
    protected $_mConsumer = '';

    //默认0 不限制长度
    protected $_mMaxLength = 0;

    /* 
     * 创建队列, stream+group确认唯一队列
     * $config必须包括:
     * stream: stream名
     * server: 格式ip:port[:auth]
     * 
     * 可选参数：
     * maxLength:队列最大长度
     * group:分组名, 默认与stream相同. stream+group相当于beanstalk的tube
     * consumer:消费者名, 默认与stream相同. 
     * */
    public function __construct(array $config){
        if(!isset($config['stream'])){
            throw new Exception("you must config the stream");
        }

        $this->_mStream = $config['stream'];

        if(!isset($config['server'])){
            throw new Exception("you must config the server");
        }

        $tmp = explode(':', $config['server']);
        $host = $tmp[0];
        $port = $tmp[1];
        $auth = $tmp[2] ?? null;

        if ($host && $port){
            $this->_mRedis = new Redis();
            $this->_mRedis->connect($host,$port,1);
            if($auth){
                $this->_mRedis->auth($auth);
            }
        }
        else{
            throw new Exception("can not get redis server conf");
        }

        if(isset($config['maxLength'])){
            $this->_mMaxLength = $config['maxLength'];
        }

        $this->_mGroup = $config['group'] ?? $config['stream'];       
        $this->_mConsumer = $config['consumer'] ?? $config['stream'];

        $this->creatGroup();
    }

    /*
     * 删除当前流(队列)
     * */
    public function destoryStream(){
        $this->_mRedis->del($this->_mStream);
    }

    /*
     * 向流中添加任务
     * $data: array
     * */
    public function addTask(array $data){
        return $this->_mRedis->xAdd($this->_mStream, "*", $data , $this->_mMaxLength);
    }

    /*
     * 从group中获取任务
     * $block:阻塞时间，毫秒. null不阻塞
     * $count:读取条数, 只要有数据，条数不够也会立刻返回，即使设置了block
     * $start:'>'接受最新数据. 若设置id，则读取大于该id，且未被ack的历史任务
     *
     * return [
     *      'id1' => taskdata1,
     *      'id2' => taskdata2,
     *      ... ...
     * ]
     *
     * 无数据返回[]
     * */
    public function getTask($block=null, $count = 1, $start = '>'){
        $d = $this->_mRedis->xReadGroup($this->_mGroup, $this->_mConsumer, [$this->_mStream => $start], $count, $block);

        if (is_array($d) && count($d) > 0){
            return $d[$this->_mStream];
        }

        return $d;
    }
    
    /*
     * 根据id
     * ack任务--从pending中删除
     * 同时从stream中删除
     */
    public function delTask($ids){
        if(!is_array($ids)){
            $ids = array($ids);
        }
        $multi = $this->_mRedis->multi(Redis::PIPELINE);

        $multi->xAck($this->_mStream, $this->_mGroup, $ids);
        $multi->xDel($this->_mStream, $ids);        
        $res = $this->_mRedis->exec();        
        return $res;
    }

    protected function creatGroup($startID = 0){
        return $this->_mRedis->xGroup('CREATE', $this->_mStream, $this->_mGroup, $startID, true);
    }
    
    /*
     * 获取pending队列概要信息
     * return 
     * 
     array(4) {
         [0] => int(2) //未收到ack的总条数
             [1] => string(15) "1604908088616-0" //startID
             [2] => string(15) "1604908088616-1" //endID
             [3] =>  //每个消费者的概要
             array(1) {
                 [0] =>
                     array(2) {
                         [0] => string(6) "normal"   //消费者名子
                             [1] => string(1) "2"    //条数
                    }
            }
        } 
     */
    protected function getPendingInfo(){
        return $this->_mRedis->xPending($this->_mStream, $this->_mGroup);
    }

    /*
     * 获取pending数据
     * $count: 条数
     * $start: 起始id
     * $end: 终止id. - +表示全部
     * $consumer：获取特定consumer的pending数据
     *
     * return 
     *array(1) {
         [0] =>  //单条数据
             array(4) {
                 [0] => string(15) "1604908973008-0" //id
                     [1] =>string(6) "normal"    //消费者名
                     [2] =>int(0)    //被取走后经历的毫秒数
                     [3] =>int(1)    //投递次数
        }
    } 
    */
    protected function getPending($count = 1, $start='-', $end='+', $consumer = null){
        if (!$consumer){
            return $this->_mRedis->xPending($this->_mStream, $this->_mGroup, $start, $end, $count);
        }

        return $this->_mRedis->xPending($this->_mStream, $this->_mGroup, $start, $end, $count, $consumer);
    }

    /*
     * 取[$start, $end]范围内的数据, 注意是闭区间
     *
     * $count:条数，null时表示取全部
     * */
    protected function getRange($start = '-', $end = '+', $count = null){
        if(is_null($count)){
            return $this->_mRedis->xRange($this->_mStream, $start, $end);
        }else{
            return $this->_mRedis->xRange($this->_mStream, $start, $end, $count);
        }
    }
    
    /*
     * 将pending队列中超时的数据重新放回队列
     * 
     * $idleTime: 超时时间, 毫秒
     * $perPage:每次取的任务数
     *
     * 注意：只能有一个进行执行pendingRestore
     *
     * 优点: consumer不需要做任何改动
     * 缺点: 
     * 先del再add, 成本上不划算，
     * 如果del和add中间断掉任务就丢了
     *
     * return: restore的数量
     * */
    public function pendingRestore($idleTime = 5000, $perPage = 20){
        /**
         * 比较简单粗暴的取pending数据方式
         * 依赖
         * 1.每次从pending取走/删除超时数据
         * 2.id是按时间排序，小id未超时，大id一定未超时
         *
         */
        $restoreNum = 0;
        while(1){
            $thisNum = 0;
            $data = $this->getPending($perPage);

            foreach($data as $one){
                $id = $one[0];
                $duration = $one[2];
                if ($duration > $idleTime){
                    $data = $this->getRange($id, $id);
                    $task = $data[$id];

                    $this->delTask($id);
                    $this->addTask($task);

                    $thisNum++;
                }
            }
            
            $restoreNum += $thisNum;
            
            if ($thisNum < $perPage){
                break;
            }
        }

        return $restoreNum;
    }

    /*
     * 另一种恢复超时任务的方法
     * 思路：将超时任务放入newConsumer的pending中，后续可以从newConsume的历史中取出数据并处理
     *
     * 优点：恢复数据没有重复读，删，插，效率高
     * 缺点：
     * consumer需要做改动，至少要改变consumer的名子
     * 只能用单进程从历史数据中读数据，然后处理。
     *
     * 详见PendingProcessor类
     *
     * $idleTime: 超时时间, 毫秒
     * $newConsumer: 之后处理pending任务的消费者名称
     * $perPage: 每次取pending任务的条数
     *
     * return: 满足条件且成功claim的条数
     * */
    public function pendingClaim($idleTime = 5000, $newConsumer=null, $perPage = 20){
        if (!$newConsumer){
            return false;
        }
    
        $info = $this->getPendingInfo();
        $startID = $info[1];
        $endID = $info[2];
    
        $claimNum = 0;
        /*
         * 使用startid, endid遍历pending列表
         * 因为getpending取的是[startid, endid]
         * 所以边界处的id可能被重复取出，但不影响结果的正确性
         * perPage越大/符合xclaim条件的id越多，重复的可能性越小
         * */
        while($startID != $endID){
            //var_dump([$startID, $endID]);
            $data = $this->getPending($perPage, $startID, $endID, $this->_mConsumer);
        
            foreach($data as $one){
                $ids[] = $one[0];
                $startID = $one[0];
            }

            $res = $this->_mRedis->xClaim($this->_mStream, $this->_mGroup, $newConsumer, $idleTime, $ids, ['JUSTID']);
            
            $thisNum = count($res);   
            $claimNum += $thisNum;
            
            //id是按时间排列，小id未超时，则后面不会超时
            //在所有id都有相同的投递次数的基础上
            //var_dump($thisNum);
            if ($thisNum < $perPage){
                break;
            }
        }

        return $claimNum;
    }
}

/* demo
$config = [
        'server' => '10.160.75.237:6379:auth',
        'stream' => 'balltube', 
        'consumer' => 'normalprocessor'//可以不设置
    ];

//创建队列
$q = new RedisQueue($config);

//添加任务
$task = ['task'=>1];
$q->addTask($task);

//获取
$timeout = 1000;
$task = $q->getTask($timeout);

//确认并删除
$taskid = key($task);
$q->delTask($taskid);

//处理pending
$q->pendingRestore();
$q->pendingClaim();
 */
