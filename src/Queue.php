<?php
/**
 * Created by PhpStorm.
 * User: pader
 * Date: 2019/7/21
 * Time: 13:43
 */

namespace xpader\beanstalkd;

class Queue extends \SplPriorityQueue {

	public function compare($l, $r)
	{
		if ($l === $r) return 0;
		return $l > $r ? -1 : 1;
	}

}