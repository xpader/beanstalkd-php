<?php
/**
 * Created by PhpStorm.
 * User: pader
 * Date: 2019/7/21
 * Time: 13:02
 */

use Workerman\Worker;
use Workerman\Connection\TcpConnection;
use xpader\beanstalkd\Server;

require_once __DIR__ . '/../vendor/autoload.php';

new Server();

class Queued {

	const STATUS_READY = 0;
	const STATUS_RESERVED = 0;

	/**
	 * @var QueueContainer[]
	 */
	public static $entities = [];
	public static $data = [];
	protected static $nextId = 1;

	/**
	 * @param $name
	 * @return QueueContainer
	 */
	public static function getEntity($name) {
		if (!isset(self::$entities[$name])) {
			self::$entities[$name] = new QueueContainer($name);
		}

		return self::$entities[$name];
	}

	public static function checkEntity($name)
	{
		if (isset(self::$entities[$name]) && self::$entities[$name]->isEmpty()) {
			unset(self::$entities[$name]);
		}
	}

	/**
	 * @return int
	 */
	public static function genId() {
		$id = self::$nextId;
		++self::$nextId;
		return $id;
	}

	public static function addData($tube, $status, $value)
	{
		$id = self::$nextId;
		++self::$nextId;
		self::$data[$id] = [$tube, $status, $value];
		return $id;
	}

	public static function getData($id)
	{
		return self::$data[$id] ?? null;
	}

}

class QueueEntity extends SplPriorityQueue {

	public function compare($l, $r)
	{
		if ($l === $r) return 0;
		return $l > $r ? -1 : 1;
	}

}

/**
 * Class QueueContainer
 *
 * @property \SplPriorityQueue $queue
 * @method void addWatch(TcpConnection $connection)
 * @method void removeWatch(TcpConnection $connection)
 * @method void addUse(TcpConnection $connection)
 * @method void removeUse(TcpConnection $connection)
 * @method void addReserve(TcpConnection $connection)
 * @method void removeReserve(TcpConnection $connection)
 */
class QueueContainer {

	/**
	 * @var string
	 */
	public $name;

	/**
	 * @var \SplPriorityQueue
	 */
	protected $queueReady;

	protected $queueBuried;
	protected $queueDelayed;
	protected $queueReserved;

	/**
	 * @var TcpConnection[]
	 */
	public $watchs = [];

	/**
	 * @var TcpConnection[]
	 */
	public $uses = [];

	/**
	 * @var TcpConnection[]
	 */
	public $reserves = [];

	private $totalJobs = 0;

	public function __construct($name) {
		$this->name = $name;
	}

	public function __get($name) {
		if ($name == 'queue') {
			if ($this->queueReady === null) {
				$this->queueReady = new QueueEntity();
				$this->queueReady->setExtractFlags(SplPriorityQueue::EXTR_DATA);
			}

			return $this->queueReady;
		} else {
			throw new \RuntimeException("Getting undefined property '$name'.");
		}
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

	public function put($id, $priority)
	{
		$this->queue->insert($id, $priority);
		++$this->totalJobs;
		$this->dispatch();
	}

	protected function dispatch()
	{
		if ($this->queue->isEmpty() || count($this->reserves) == 0) {
			return;
		}

		while ($this->queue->valid()) {
			$id = $this->queue->current();

			while ($connection = array_shift($this->reserves)) {
				if ($connection->reserving) {
					list(,, $value) = Queued::getData($id);
					$connection->send(sprintf('RESERVED %d %d %s', $id, strlen($value), $value));
					$connection->reserving = false;
					$this->queue->next();
					break;
				}
			}

			if (count($this->reserves) == 0) {
				break;
			}
		}
	}

	public function stats()
	{
		return [
			'name' => $this->name,
			'current-jobs-urgent' => 0,
			'current-jobs-ready' => $this->queue->count(),
			'current-jobs-reserved' => 0,
			'current-jobs-delayed' => 0,
			'current-jobs-buried' => 0,
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
	 * @param TcpConnection $connection
	 */
	protected function addClient($to, $connection)
	{
		if (!in_array($connection, $this->$to, true)) {
			$this->{$to}[] = $connection;
		}
	}

	/**
	 * @param string $from
	 * @param TcpConnection $connection
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

if (!defined('GLOBAL_START')) {
	Worker::runAll();
}
