<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 9:36
 */

namespace rabbit\db\click\pool;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\core\ObjectFactory;
use rabbit\pool\ConnectionInterface;
use rabbit\pool\ConnectionPool;

/**
 * Class ClickPool
 * @package rabbit\db\click\pool
 */
class ClickPool extends ConnectionPool
{
    /**
     * @return ConnectionInterface
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function createConnection(): ConnectionInterface
    {
        $config = $this->getPoolConfig()->getConfig();
        $config['poolKey'] = $this->getPoolConfig()->getName();
        return ObjectFactory::createObject($config, [], false);
    }
}
