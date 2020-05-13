<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2019/1/23
 * Time: 14:52
 */

namespace rabbit\db\click;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\core\ObjectFactory;
use rabbit\db\click\pool\ClickPool;
use rabbit\db\click\pool\ClickPoolConfig;
use rabbit\db\ConnectionInterface;
use rabbit\db\Exception;
use rabbit\db\pool\PdoPool;
use rabbit\db\pool\PdoPoolConfig;
use rabbit\helper\ArrayHelper;

/**
 * Class Manager
 * @package rabbit\db\click
 */
class Manager
{
    /** @var ClickPool[] */
    private $connections = [];
    /** @var array */
    private $deferList = [];
    /** @var int */
    private $min = 5;
    /** @var int */
    private $max = 6;
    /** @var int */
    private $wait = 0;

    /**
     * Manager constructor.
     * @param array $configs
     */
    public function __construct(array $configs = [])
    {
        $this->addConnection($configs);
    }

    /**
     * @param array $configs
     */
    public function addConnection(array $configs): void
    {
        foreach ($configs as $name => $config) {
            if (!isset($this->connections[$name])) {
                /** @var ClickPool $pool */
                $pool = ArrayHelper::remove($config, 'pool');
                $config['poolName'] = $name;
                $pool->getPoolConfig()->setConfig($config);
                $this->connections[$name] = $pool;
            }
        }
    }

    /**
     * @param string $name
     * @return Connection|null
     * @throws DependencyException
     * @throws Exception
     * @throws NotFoundException
     */
    public function getConnection(string $name = 'db'): ?Connection
    {
        if (($connection = ClickContext::get($name)) === null) {
            $pool = $this->connections[$name];
            $connection = $pool->getConnection();
            ClickContext::set($name, $connection);
            if (($cid = \Co::getCid()) !== -1 && !in_array($cid, $this->deferList)) {
                defer(function () use ($cid) {
                    ClickContext::release();
                    $this->deferList = array_values(array_diff($this->deferList, [$cid]));
                });
                $this->deferList[] = $cid;
            }
        }
        return $connection;
    }

    /**
     * @param string $name
     * @return bool
     */
    public function hasConnection(string $name): bool
    {
        return isset($this->connections[$name]);
    }

    /**
     *
     */
    public function release(): void
    {
        ClickContext::release();
    }
}
