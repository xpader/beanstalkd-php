<?php
/**
 * Created by PhpStorm.
 * User: pader
 * Date: 2019/7/21
 * Time: 16:26
 */

namespace xpader\beanstalkd;


class Job {

	const STATUS_READY = 0;
	const STATUS_RESERVED = 1;
	const STATUS_DELAYED = 2;
	const STATUS_BURIED = 3;

	public $id;
	public $tube;
	public $pri;
	public $status;
	public $ttr;
	public $value;

	public function __construct($id, $tube, $pri, $status, $ttr, $value) {
		$this->id = $id;
		$this->tube = $tube;
		$this->pri = $pri;
		$this->status = $status;
		$this->ttr = $ttr < 1 ? 1 : $ttr;
		$this->value = $value;
	}

}