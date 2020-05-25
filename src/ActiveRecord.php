<?php

namespace rabbit\db\click;

use Exception;
use rabbit\db\ConnectionInterface;

/**
 * Class ActiveRecord
 * @package rabbit\db\click
 */
class ActiveRecord extends \rabbit\db\clickhouse\ActiveRecord
{

    public function __destruct()
    {
        ClickContext::release();
    }

    /**
     * Returns the connection used by this AR class.
     * @return mixed|Connection the database connection used by this AR class.
     * @throws Exception
     */
    public static function getDb(): ConnectionInterface
    {
        return getDI('click')->getConnection();
    }
}
