<?php

namespace rabbit\db\click;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\App;
use rabbit\core\ObjectFactory;
use rabbit\db\DbContext;
use rabbit\db\Exception;
use rabbit\db\QueryBuilder;
use rabbit\exception\NotSupportedException;
use rabbit\helper\ArrayHelper;
use rabbit\pool\ConnectionInterface;

/**
 * Class Connection
 * @package rabbit\db\click
 */
class Connection extends \rabbit\db\Connection implements ConnectionInterface
{
    public $schemaMap = [
        'clickhouse' => Schema::class
    ];
    /** @var string */
    public $database = 'default';

    protected $commandClass = Command::class;

    /**
     * Connection constructor.
     * @param string $dsn
     * @param string $poolKey
     * @throws Exception
     */
    public function __construct(string $dsn, string $poolKey)
    {
        parent::__construct($dsn);
        $this->poolKey = $poolKey;
        $this->driver = 'click';
    }

    /**
     * @param string $str
     * @return string
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function quoteValue($str)
    {
        return $this->getSchema()->quoteValue($str);
    }

    public function quoteSql($sql)
    {
        return $sql;
    }


    /**
     * @return array
     * @throws Exception
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }


    /**
     * @throws Exception
     */
    public function close()
    {
        if ($this->getIsActive()) {
            App::warning('Closing DB connection: ' . $this->shortDsn, 'clickhouse');
        }
    }

    /**
     * @return \PDO|\SeasClick
     */
    public function createPdoInstance()
    {
        $parsed = parse_url($this->dsn);
        isset($parsed['query']) ? parse_str($parsed['query'], $parsed['query']) : $parsed['query'] = [];
        [$_, $host, $port, $this->username, $this->password, $query] = ArrayHelper::getValueByArray(
            $parsed,
            ['scheme', 'host', 'port', 'user', 'pass', 'query'],
            null,
            ['clickhouse', 'localhost', '9000', '', '', []]
        );
        $this->database = ArrayHelper::remove($query, 'dbname');
        $compression = ArrayHelper::remove($query, 'compression');
        $client = new \SeasClick([
            "host" => $host,
            "port" => $port,
            "compression" => $compression,
            "database" => $this->database,
            "user" => $this->username,
            "passwd" => $this->password
        ]);
        return $client;
    }

    /**
     * @param $name
     * @param $arguments
     */
    public function __call($name, $arguments)
    {
        $attempt = 0;
        $this->open();
        while (true) {
            try {
                $conn = DbContext::get($this->poolName, $this->driver);
                return $conn->$name(...$arguments);
            } catch (\Throwable $exception) {
                if (($retryHandler = $this->getRetryHandler()) === null || !$retryHandler->handle($exception, $attempt++)) {
                    throw $exception;
                }
                $this->reconnect($attempt);
            }
        }
    }

    /**
     * @param int $attempt
     * @throws Exception
     */
    public function reconnect(int $attempt = 0): void
    {
        DbContext::delete($this->poolName, $this->driver);
        App::warning('Reconnect DB connection: ' . $this->shortDsn, 'db');
        $this->open($attempt);
    }

    /**
     * @return bool
     */
    public function check(): bool
    {
        return $this->getIsActive();
    }

    /**
     * @param float $timeout
     * @return mixed|void
     * @throws NotSupportedException
     */
    public function receive(float $timeout = -1)
    {
        throw new NotSupportedException('can not call ' . __METHOD__);
    }

    /**
     * @return mixed|\rabbit\db\Schema
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function getSchema()
    {
        return $this->_schema = ObjectFactory::createObject([
            'class' => Schema::class,
            'db' => $this
        ]);
    }

    public function quoteTableName($name)
    {
        return $name;
    }

    public function getDriverName()
    {
        return 'clickhouse';
    }

    public function quoteColumnName($name)
    {
        return $name;
    }

    /**
     * @return QueryBuilder
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function getQueryBuilder()
    {
        return $this->getSchema()->getQueryBuilder();
    }

    /**
     * @param $conn
     */
    protected function setInsertId($conn): void
    {

    }
}
