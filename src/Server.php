<?php
/**
 * Created by PhpStorm.
 * User: pader
 * Date: 2019/7/21
 * Time: 13:01
 */

namespace xpader\beanstalkd;

use Workerman\Connection\TcpConnection;
use Workerman\Worker;

class Server
{

	/**
	 * @var Tube[]
	 */
	protected $tubes = [];
	protected $data = [];
	protected $nextId = 1;

	protected $stats = [
		'current-jobs-urgent' => 0,
        'current-jobs-ready' => 0,
        'current-jobs-reserved' => 0,
        'current-jobs-delayed' => 0,
        'current-jobs-bureid' => 0,
	    'cmd-put' => 0,
	    'cmd-peek' => '总共执行peek指令的次数',
	    'cmd-peek-ready' => '总共执行peek-ready指令的次数',
	    'cmd-peek-delayed' => '总共执行peek-delayed指令的次数',
	    'cmd-peek-buried' => '总共执行peek-buried指令的次数',
	    'cmd-reserve' => 0,
	    'cmd-use' => 0,
	    'cmd-watch' => 0,
	    'cmd-ignore' => 0,
	    'cmd-release' => '总共执行release指令的次数',
	    'cmd-bury' => '总共执行bury指令的次数',
	    'cmd-kick' => '总共执行kick指令的次数',
	    'cmd-stats' => 0,
	    'cmd-stats-job' => '总共执行stats-job指令的次数',
	    'cmd-stats-tube' => '总共执行stats-tube指令的次数',
	    'cmd-list-tubes' => 0,
	    'cmd-list-tube-used' => '总共执行list-tube-used指令的次数',
	    'cmd-list-tubes-watched' => '总共执行list-tubes-watched指令的次数',
	    'cmd-pause-tube' => '总共执行pause-tube指令的次数',
	    'job-timeouts' => '所有超时的job的总共数量',
	    'total-jobs' => '创建的所有job数量',
	    'max-job-size' => 'job的数据部分最大长度',
	    'current-tubes' => 0,
	    'current-connections' => 0,
	    'current-producers' => '当前所有的打开的连接中至少执行一次put指令的连接数量',
	    'current-workers' => '当前所有的打开的连接中至少执行一次reserve指令的连接数量',
	    'current-waiting' => '当前所有的打开的连接中执行reserve指令但是未响应的连接数量',
	    'total-connections' => 0,
        'pid' => 0,
        'version' => '0.1',
        'rusage-utime' => 0,
        'rusage-stime' => 0,
	    'uptime' => '服务器进程运行的秒数',
	    'binlog-oldest-index' => '开始储存jobs的binlog索引号',
	    'binlog-current-index' => '当前储存jobs的binlog索引号',
	    'binlog-max-size' => 'binlog的最大容量',
	    'binlog-records-written' => 'binlog累积写入的记录数',
	    'binlog-records-migrated'=> 0,
	    'id' => '',
	    'hostname' => ''
	];

	/**
	 * @var Worker
	 */
	protected $worker;

	public function __construct($host='127.0.0.1', $port=11302) {
		$worker = new Worker(sprintf('btp://%s:%d', $host, $port));
		$worker->name = 'beanstalkd';
		$worker->count = 1;
		$worker->onConnect = [$this, 'onConnect'];
		$worker->onMessage = [$this, 'onMessage'];
		$worker->onClose = [$this, 'onClose'];
		$this->worker = $worker;

		$this->stats['id'] = uniqid();
		$this->stats['hostname'] = gethostname();
		$this->stats['pid'] = getmypid();
	}

	/**
	 * @param $connection TcpConnection
	 */
	public function onConnect($connection)
	{
		$connection->qWatchs = [];
		$connection->qUsing = null;
		$connection->reserving = false;

		++$this->stats['total-connections'];

		$this->log("Client [{$connection->id}] ".$connection->getRemoteAddress().' connected.');
	}

	/**
	 * @param $connection \xpader\beanstalkd\Connection
	 * @param $data string
	 */
	public function onMessage($connection, $data) {
		if ($connection->reserving) {
			$this->log("Ignore reserving client message: $data");
			return;
		}

		//获取输入命令
		$cmdSplit = strpos($data, ' ');

		if ($cmdSplit) {
			$cmd = substr($data, 0, $cmdSplit);
			$arg = substr($data, $cmdSplit + 1);
		} else {
			$cmd = $data;
			$arg = null;
		}

		echo "Receive: $data\r\n";

		/**
		 * @var $connection \Workerman\Connection\TcpConnection
		 */

		global $queueData;

		switch ($cmd) {
			case 'use':
				$name = $arg;

				if ($connection->qUsing && $connection->qUsing != $name) {
					$this->getTube($connection->qUsing)->removeUse($connection);
					$this->touchTube($connection->qUsing);
				}

				$entity = $this->getTube($name);
				$connection->qUsing = $name;
				$entity->addUse($connection);

				++$this->stats['cmd-use'];

				$connection->send(sprintf('USING %s', $name));
				break;

			case 'put':
				++$this->stats['cmd-put'];

				if (!isset($connection->qUsing)) {
					$connection->send('NO_USING');
					return;
				}

				$tube = $this->getTube($connection->qUsing);

				$priority = (int)strtok($arg, ' ');
				$value = strtok('');

				$id = $this->addData($connection->qUsing, Queue::JOB_READY, $value);
				$tube->put($id, $priority);
				$connection->send(sprintf('OK %d', $id));

				$this->log('data: '.print_r($this->data));
				break;

			case 'watch':
				$name = $arg;

				if (!in_array($name, $connection->qWatchs)) {
					$entity = $this->getTube($name);
					$entity->addWatch($connection);
					$connection->qWatchs[] = $name;
				}

				++$this->stats['cmd-watch'];

				$connection->send(sprintf('WATCHING %d', count($connection->qWatchs)));
				break;

			case 'reserve':
				++$this->stats['cmd-reserve'];

				if (!$connection->qWatchs) {
					$connection->send('NO_WATCHING');
					return;
				}

				$connection->reserving = true;

				foreach ($connection->qWatchs as $name) {
					$this->getTube($name)->addReserve($connection);
				}

				$this->log("Client {$connection->id} is reserving..");
				break;

			case 'delete':
				$id = (int)$arg;

				if (isset($queueLinks[$id])) {

				}
				break;

			case 'ignore':
				$name = $arg;
				$index = array_search($name, $connection->qWatchs);
				if ($index !== false) {
					$tube = $this->getTube($name);
					$tube->removeWatch($connection);
					unset($connection->qWatchs[$index]);
					$connection->send(sprintf('WATCHING %d', count($connection->qWatchs)));
					$this->touchTube($name);
				} else {
					$connection->send('NOT_IGNORED');
				}
				++$this->stats['cmd-ignore'];
				break;

			case 'stats-tube':
				$tube = $arg;
				if (isset($this->tubes[$tube])) {
					$connection->send(print_r($this->tubes[$tube]->stats(), true));
				} else {
					$connection->send('NOT_FOUND');
				}
				break;

			case 'list-tubes':
				$connection->send(sprintf("OK %d\r\n%s", count($this->tubes), json_encode(array_keys($this->tubes))));
				++$this->stats['cmd-list-tubes'];
				break;

			case 'stats':
				++$this->stats['cmd-stats'];
				$this->stats['current-tubes'] = count($this->tubes);
				$this->stats['current-connections'] = count($this->worker->connections);

				$connection->send(sprintf("OK\r\n%s", print_r($this->stats, true)));
				break;

			case 'quit':
				$connection->close();
				break;

			default:
				$connection->send('UNKNOWN_COMMAND');
		}
	}

	/**
	 * @param $connection Connection
	 */
	public function onClose($connection) {
		if ($connection->qUsing) {
			$tube = $this->getTube($connection->qUsing);
			$tube->removeUse($connection);
			$this->touchTube($connection->qUsing);
		}

		if ($connection->qWatchs) {
			foreach ($connection->qWatchs as $name) {
				if (isset($this->tubes[$name])) {
					$tube = $this->tubes[$name];
					$tube->removeWatch($connection);
					if ($tube->isEmpty()) {
						unset($this->tubes[$name]);
					}
				}
			}
		}

		$this->log("Client [{$connection->id}] ".$connection->getRemoteAddress().' closed.');
	}

	/**
	 * @param string $name
	 * @return Tube
	 */
	protected function getTube($name)
	{
		if (!isset($this->tubes[$name])) {
			$this->tubes[$name] = new Tube($name, $this);
		}

		return $this->tubes[$name];
	}

	protected function touchTube($name)
	{
		if (isset($this->tubes[$name]) && $this->tubes[$name]->isEmpty()) {
			unset($this->tubes[$name]);
		}
	}

	/**
	 * @return int
	 */
	protected function genId() {
		$id = $this->nextId;
		++$this->nextId;
		return $id;
	}

	public function addData($tube, $status, $value)
	{
		$id = $this->nextId;
		++$this->nextId;
		$this->data[$id] = [$tube, $status, $value];
		return $id;
	}

	public function getData($id)
	{
		return isset($this->data[$id]) ? $this->data[$id] : null;
	}

	protected function log($message)
	{
		echo $message."\r\n";
	}

}