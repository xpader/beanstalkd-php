<?php
/**
 * Created by PhpStorm.
 * User: pader
 * Date: 2019/7/21
 * Time: 13:31
 */

namespace xpader\beanstalkd;
use Workerman\Lib\Timer;

/**
 * Class Tube
 * @package xpader\beanstalkd
 * 
 * @method void addWatch(Connection $connection)
 * @method void removeWatch(Connection $connection)
 * @method void addUse(Connection $connection)
 * @method void removeUse(Connection $connection)
 * @method void addReserve(Connection $connection)
 * @method void removeReserve(Connection $connection)
 */
class Tube
{

	/**
	 * @var string
	 */
	public $name;

	/**
	 * @var Server
	 */
	public $server;

	/**
	 * Ready queue
	 * @var \SplPriorityQueue
	 */
	protected $queue;

	protected $queueBuried = [];
	protected $queueDelayed = [];
	protected $queueReserved = [];

	/**
	 * @var Connection[]
	 */
	public $watchs = [];

	/**
	 * @var Connection[]
	 */
	public $uses = [];

	/**
	 * @var Connection[]
	 */
	public $reserves = [];

	private $totalJobs = 0;

	public function __construct($name, $server) {
		$this->name = $name;
		$this->server = $server;
		$this->queue = new Queue();
		$this->queue->setExtractFlags(\SplPriorityQueue::EXTR_DATA);
	}

	public function __call($name, $arguments) {
		if (substr($name, 0, 3) == 'add') {
			$cmd = 'add';
			$target = strtolower(substr($name, 3));
		} elseif (substr($name, 0, 6) == 'remove') {
			$cmd = 'remove';
			$target = strtolower(substr($name, 6));
		} else {
			throw new \BadMethodCallException("Call to undefined method '$name'.");
		}

		if (!in_array($target, ['watch', 'use', 'reserve'])) {
			throw new \BadMethodCallException("Call to undefined method '$name'.");
		}

		$target .= 's';

		if ($cmd == 'add') {
			$this->addClient($target, $arguments[0]);
		} else {
			$this->removeClient($target, $arguments[0]);
		}

		if ($name == 'addReserve') {
			$this->dispatch();
		}
	}

	public function put($id, $priority, $delay=0)
	{
		if ($delay == 0) {
			$this->queue->insert($id, $priority);
			$this->dispatch();
		} else {
			$this->queueDelayed[$id] = Timer::add($delay, function($tube, $id) {
				/* @var $tube Tube */
				$job = $tube->server->getJob($id);
				if (!$job || $job->status != Job::STATUS_DELAYED) {
					return;
				}
				if (isset($this->queueDelayed[$id])) {
					unset($this->queueDelayed[$id]);
				}
				$job->status = Job::STATUS_READY;
				$tube->queue->insert($id, $job->pri);
				$tube->dispatch();
			}, [$this, $id], false);
		}

		++$this->totalJobs;
	}

	/**
	 * @param Job $job
	 */
	public function delete($job)
	{
		switch ($job->status) {
			case Job::STATUS_RESERVED:
				if (isset($this->queueReserved[$job->id])) {
					Timer::del($this->queueReserved[$job->id]);
					unset($this->queueReserved[$job->id]);
				}
				break;
			case Job::STATUS_DELAYED:
				if (isset($this->queueDelayed[$job->id])) {
					Timer::del($this->queueDelayed[$job->id]);
					unset($this->queueDelayed[$job->id]);
				}
				break;
			case Job::STATUS_BURIED:
				if (isset($this->queueBuried[$job->id])) {
					unset($this->queueBuried[$job->id]);
				}
				break;
		}
		$this->server->delJob($job->id);
	}

	/**
	 * @param int|Job $id
	 * @param int $delay
	 */
	public function release($id, $delay=0)
	{
		if ($id instanceof Job) {
			$job = $id;
		} else {
			$job = $this->server->getJob($id);
			if (!$job) {
				return;
			}
		}

		if ($job->status != Job::STATUS_RESERVED) {
			return;
		}

		if (isset($this->queueReserved[$id])) {
			Timer::del($this->queueReserved[$id]);
			unset($this->queueReserved[$id]);
		}

		$job->status = Job::STATUS_READY;
		$this->put($job->id, $job->pri, $delay);
	}

	protected function dispatch()
	{
		if ($this->queue->isEmpty() || count($this->reserves) == 0) {
			return;
		}

		while ($this->queue->valid()) {
			$id = $this->queue->current();
			$job = $this->server->getJob($id);

			//The job maybe deleted or other status.
			if ($job === null || $job->status != Job::STATUS_READY) {
				$this->queue->next();
				continue;
			}

			while ($connection = array_shift($this->reserves)) {
				if ($connection->reserving) {
					$connection->send(sprintf('RESERVED %d %d %s', $id, strlen($job->value), $job->value));
					$connection->reserving = false;
					$job->status = Job::STATUS_RESERVED;
					$this->queueReserved[$job->id] = Timer::add($job->ttr, [$this, 'release'], [$job->id], false);
					$this->queue->next();
					break;
				}
			}

			if (count($this->reserves) == 0) {
				break;
			}
		}
	}

	/**
	 * @param Job $job
	 */
	public function bury($job)
	{
		if (isset($this->queueReserved[$job->id])) {
			unset($this->queueReserved[$job->id]);
		}
		$job->status = Job::STATUS_BURIED;
		$this->queueBuried[$job->id] = $job->id;
	}

	/**
	 * @param Job $job
	 */
	public function touch($job)
	{
		if (!isset($this->queueReserved[$job->id])) {
			return;
		}
		Timer::del($this->queueReserved[$job->id]);
		$this->queueReserved[$job->id] = Timer::add($job->ttr, [$this, 'release'], [$job->id], false);
	}

	public function kick($job)
	{

	}

	public function stats()
	{
		return [
			'name' => $this->name,
			'current-jobs-urgent' => 0,
			'current-jobs-ready' => $this->queue->count(),
			'current-jobs-reserved' => count($this->queueReserved),
			'current-jobs-delayed' => count($this->queueDelayed),
			'current-jobs-buried' => count($this->queueBuried),
			'total-jobs' => $this->totalJobs,
			'current-using' => count($this->uses),
			'current-watching' => count($this->watchs),
			'current-waiting' => count($this->reserves),
			'cmd-delete' => 0,
			'cmd-pause-tube' => 0,
			'pause' => 0,
			'pause-time-left' => 0
		];
	}

	/**
	 * @param string $to
	 * @param Connection $connection
	 */
	protected function addClient($to, $connection)
	{
		if (!in_array($connection, $this->$to, true)) {
			$this->{$to}[] = $connection;
		}
	}

	/**
	 * @param string $from
	 * @param Connection $connection
	 */
	protected function removeClient($from, $connection)
	{
		$key = array_search($connection, $this->$from, true);
		if ($key !== false) {
			unset($this->{$from}[$key]);
		}
	}

	/**
	 * 检查当前队列是否为空
	 *
	 * 无队列数据，且无任何 watching, use, reserve 的客户端即代表为空
	 *
	 * @return bool
	 */
	public function isEmpty()
	{
		return $this->queue->isEmpty() && empty($this->watchs) && empty($this->uses) && empty($this->reserves);
	}

	public function __destruct() {
		$this->queueReady = $this->watchs = $this->reserves = $this->uses = null;
	}

}