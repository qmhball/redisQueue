<?php
require_once('RedisQueue.php');
/*
 *
 * 使用pendingClaim恢复超时任务后
 * 可以用类似如下方法处理超时任务
 * */
$config = [
            'server' => '10.10.10.1:6379',
            'stream' => 'balltube',    
            'consumer' => 'pendingProcessor',//pendingClaim中的newConsumer
];

$q = new RedisQueue($this->_config);
$block = 1000;
$num = 1;

while(1){
    $d = $q->getTask($block, $num, 0);
    if (empty($d)){
        break;
    }

    $id = key($d);
    $data = $d[$id];
    $q->delTask($id);
    //处理任务逻辑
    yourTaskProcessFunc($data);
}

