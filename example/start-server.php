<?php
/**
 * Created by PhpStorm.
 * User: pader
 * Date: 2019/7/21
 * Time: 13:02
 */

use Workerman\Worker;
use xpader\beanstalkd\Server;

require_once __DIR__ . '/../vendor/autoload.php';

new Server();

if (!defined('GLOBAL_START')) {
	Worker::runAll();
}
