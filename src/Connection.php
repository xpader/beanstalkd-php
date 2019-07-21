<?php
/**
 * Created by PhpStorm.
 * User: pader
 * Date: 2019/7/21
 * Time: 13:19
 */

namespace xpader\beanstalkd;

use Workerman\Connection\TcpConnection;

class Connection extends TcpConnection
{

	public $qWatchs = [];
	public $qUsing = null;
	public $reserving = false;

}