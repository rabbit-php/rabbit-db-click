<?php
declare(strict_types=1);

namespace Rabbit\DB\Click;

use Rabbit\Pool\ConnectionInterface;
use Throwable;

/**
 * Class ActiveRecord
 * @package Rabbit\DB\Click
 */
class ActiveRecord extends \Rabbit\DB\ClickHouse\ActiveRecord
{
    /**
     * Returns the connection used by this AR class.
     * @return mixed|Connection the database connection used by this AR class.
     * @throws Throwable
     */
    public static function getDb(): ConnectionInterface
    {
        return getDI('click')->get();
    }
}
