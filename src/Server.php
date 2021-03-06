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

	/**
	 * @var Job[]
	 */
	protected $jobs = [];

	protected $nextId = 1;

	protected $stats = [
		'current-jobs-urgent' => 0,
        'current-jobs-ready' => 0,
        'current-jobs-reserved' => 0,
        'current-jobs-delayed' => 0,
        'current-jobs-bureid' => 0,
		'cmd-delete' => 0,
	    'cmd-put' => 0,
	    'cmd-peek' => '总共执行peek指令的次数',
	    'cmd-peek-ready' => '总共执行peek-ready指令的次数',
	    'cmd-peek-delayed' => '总共执行peek-delayed指令的次数',
	    'cmd-peek-buried' => '总共执行peek-buried指令的次数',
	    'cmd-reserve' => 0,
	    'cmd-use' => 0,
	    'cmd-watch' => 0,
	    'cmd-ignore' => 0,
	    'cmd-release' => 0,
	    'cmd-bury' => 0,
	    'cmd-kick' => '总共执行kick指令的次数',
		'cmd-touch' => 0,
	    'cmd-stats' => 0,
	    'cmd-stats-job' => '总共执行stats-job指令的次数',
	    'cmd-stats-tube' => 0,
	    'cmd-list-tubes' => 0,
	    'cmd-list-tube-used' => '总共执行list-tube-used指令的次数',
	    'cmd-list-tubes-watched' => '总共执行list-tubes-watched指令的次数',
	    'cmd-pause-tube' => '总共执行pause-tube指令的次数',
	    'job-timeouts' => '所有超时的job的总共数量',
	    'total-jobs' => 0,
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
		$cmd = strtok($data, ' ');
		$arg = strtok('');

		echo "Receive: $data\r\n";

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
				$delay = (int)strtok(' ');
				$ttr = (int)strtok(' ');
				$bytes = (int)strtok(' ');
				$value = substr(strtok(''), 2, $bytes);

				$status = $delay > 0 ? Job::STATUS_DELAYED : Job::STATUS_READY;

				$id = $this->addJob($connection->qUsing, $priority, $status, $ttr, $value);
				$tube->put($id, $priority, $delay);
				$connection->send(sprintf('INSERTED %d', $id));

				$this->log('data: '.print_r($this->jobs));
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

				//Todo: DEADLINE_SOON Response

				$this->log("Client {$connection->id} is reserving..");
				break;

			case 'delete':
				$id = (int)$arg;
				$job = $this->getJob($id);
				if ($job) {
					$tube = $this->getTube($job->tube);
					$tube->delete($job);
					$connection->send('DELETED');
				} else {
					$connection->send('NOT_FOUND');
				}
				++$this->stats['cmd-delete'];
				break;

			case 'release':
				$id = (int)strtok($arg, ' ');
				$priority = (int)strtok(' ');
				$delay = (int)strtok(' ');

				$job = $this->getJob($id);

				if ($job && $job->status == Job::STATUS_RESERVED) {
					$job->pri = $priority;
					$tube = $this->getTube($job->tube);
					$tube->release($job, $delay);
					$connection->send('RELEASED');
				} else {
					$connection->send('NOT_FOUND');
				}

				++$this->stats['cmd-release'];
				break;

			case 'bury':
				$id = (int)strtok($arg, ' ');
				$priority = (int)strtok(' ');

				$job = $this->getJob($id);

				if ($job && $job->status == Job::STATUS_RESERVED) {
					$job->pri = $priority;
					$tube = $this->getTube($job->tube);
					$tube->bury($job);
					$connection->send('BURIED');
				} else {
					$connection->send('NOT_FOUND');
				}

				++$this->stats['cmd-bury'];
				break;

			case 'touch':
				$id = (int)$arg;
				$job = $this->getJob($id);

				if ($job && $job->status == Job::STATUS_RESERVED) {
					$tube = $this->getTube($job->tube);
					$tube->touch($job);
					$connection->send('TOUCHED');
				} else {
					$connection->send('NOT_FOUND');
				}

				++$this->stats['cmd-touch'];
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

			case 'peek':
				$id = (int)$arg;
				$job = $this->getJob($id);
				if ($job) {
					$connection->send(sprintf("FOUND %d %d\r\n%s", $job->id, strlen($job->value), $job->value));
				} else {
					$connection->send('NOT_FOUND');
				}
				++$this->stats['cmd-peek'];
				break;

			//Todo: peek-ready
			//Todo: peek-delayed
			//Todo: peek-buried
			//Todo: kick

			case 'kick-job':
				$id = (int)$arg;
				$job = $this->getJob($id);
				if ($job && ($job->status == Job::STATUS_BURIED || $job->status == Job::STATUS_DELAYED)) {
					$tube = $this->getTube($job->tube);
					$tube->kick($job);
					$connection->send('KICKED');
				} else {
					$connection->send('NOT_FOUND');
				}
				break;

			case 'stats-tube':
				$tube = $arg;
				if (isset($this->tubes[$tube])) {
					self::sendStats($connection, $this->tubes[$tube]->stats());
				} else {
					$connection->send('NOT_FOUND');
				}
				++$this->stats['cmd-stats-tube'];
				break;

			case 'list-tubes':
				self::sendStats($connection, array_keys($this->tubes));
				++$this->stats['cmd-list-tubes'];
				break;

			case 'stats':
				++$this->stats['cmd-stats'];
				$this->stats['total-jobs'] = $this->nextId - 1;
				$this->stats['current-tubes'] = count($this->tubes);
				$this->stats['current-connections'] = count($this->worker->connections);
				self::sendStats($connection, $this->stats);
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

	/**
	 * @param string $tube
	 * @param int $pri
	 * @param int $status
	 * @param int $ttr
	 * @param mixed $value
	 * @return int
	 */
	public function addJob($tube, $pri, $status, $ttr, $value)
	{
		$job = new Job($this->nextId, $tube, $pri, $status, $ttr, $value);
		++$this->nextId;
		$this->jobs[$job->id] = $job;
		return $job->id;
	}

	/**
	 * @param int $id
	 * @return null|Job
	 */
	public function getJob($id)
	{
		return isset($this->jobs[$id]) ? $this->jobs[$id] : null;
	}

	public function delJob($id)
	{
		if (isset($this->jobs[$id])) {
			unset($this->jobs[$id]);
		}
	}

	protected function log($message)
	{
		echo $message."\r\n";
	}

	/**
	 * @param Connection $connection
	 * @param $stats
	 */
	protected static function sendStats($connection, $stats)
	{
		$str = "---\n";
		foreach ($stats as $k => $v) {
			$str .= (is_int($k) ? '-' : $k.':')." $v\n";
		}
		$connection->send(sprintf("OK %d\r\n%s", strlen($str), $str));
	}

}