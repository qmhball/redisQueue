<?php
require_once('RedisQueue.php');
/*
 *
 * 使用pendingClaim恢复超时任务后
 * 可以用类似如下方法处理超时任务
 * */
class PendingProcessor{
    protected $_config;
        /*
        $config = [
            'server' => '10.160.75.237:6379',
            'stream' => 'balltube',    
            'consumer' => 'pendingProcessor',//pendingClaim中的newConsumer
        ];*/

    public function __construct($config){
        $this->_config = $config;
    }
    
    /*
     * 只能以单进程方式运行
     * 因为并发getTask可能拿到同一未处理的历史数据
     *
     * */
    public function process(){
        $q = new RedisQueue($this->_config);
        $info = $q->getPendingInfo();

        $block = 1000;
        $num = 1;

        while(1){
            $d = $q->getTask($block, $num, 0);
            if (empty($d)){
                break;
            }

            $id = key($d);
            $data = $d[$id];
            //yourTaskProcessFunc($data)
            
            $q->delTask($id);
        }
    }
}

