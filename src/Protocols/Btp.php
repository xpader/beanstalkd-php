<?php
/**
 * Created by PhpStorm.
 * User: pader
 * Date: 2019/7/21
 * Time: 13:15
 */

namespace Protocols;

use Workerman\Connection\TcpConnection;

class Btp
{

	/**
	 * Check the integrity of the package.
	 *
	 * @param string        $buffer
	 * @param TcpConnection $connection
	 * @return int
	 */
	public static function input($buffer, TcpConnection $connection)
	{
		// Judge whether the package length exceeds the limit.
		if (strlen($buffer) >= $connection->maxPackageSize) {
			$connection->close();
			return 0;
		}
		//  Find the position of  "\n".
		$pos = strpos($buffer, "\n");
		// No "\n", packet length is unknown, continue to wait for the data so return 0.
		if ($pos === false) {
			return 0;
		}
		// Return the current package length.
		return $pos + 1;
	}

	/**
	 * 打包，当向客户端发送数据的时候会自动调用
	 * @param string $buffer
	 * @return string
	 */
	public static function encode($buffer)
	{
		return $buffer."\r\n";
	}

	/**
	 * Decode.
	 *
	 * @param string $buffer
	 * @return string
	 */
	public static function decode($buffer)
	{
		// Remove "\n"
		return rtrim($buffer, "\r\n");
	}

}