<?php

namespace rabbit\db\click;

use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use rabbit\App;
use rabbit\core\ObjectFactory;
use rabbit\db\ConnectionTrait;
use rabbit\db\QueryBuilder;
use rabbit\exception\InvalidArgumentException;
use rabbit\exception\NotSupportedException;
use rabbit\helper\ArrayHelper;
use rabbit\pool\ConnectionInterface;
use rabbit\pool\PoolManager;

/**
 * Class Connection
 * @package rabbit\db\click
 */
class Connection extends \rabbit\db\Connection implements ConnectionInterface
{
    use ConnectionTrait;

    public $schemaMap = [
        'clickhouse' => Schema::class
    ];
    /** @var string */
    public $database = 'default';

    /**
     * Connection constructor.
     * @param string $dsn
     * @param string $poolKey
     * @throws Exception
     */
    public function __construct(string $dsn, string $poolKey)
    {
        parent::__construct($dsn);
        $this->lastTime = time();
        $this->connectionId = uniqid();
        $this->poolKey = $poolKey;
        $this->createConnection();
    }

    /**
     * @param null $sql
     * @param array $params
     * @return Command|\rabbit\db\Command
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function createCommand($sql = null, $params = [])
    {
        /** @var Command $command */
        $command = ObjectFactory::createObject(Command::class, [
            'db' => $this,
            'sql' => $sql,
        ], false);

        return $command->bindValues($params);
    }

    /**
     * @throws Exception
     */
    public function createConnection(): void
    {
        $this->open();
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
     * @param int $attempt
     * @throws Exception
     */
    public function open(int $attempt = 0)
    {
        if ($this->getIsActive()) {
            return;
        }

        if (empty($this->dsn)) {
            throw new InvalidArgumentException('Connection::dsn cannot be empty.');
        }

        $token = 'Opening Clickhouse connection: ' . $this->shortDsn;
        App::info($token, "Clickhouse");
        $this->pdo = $this->createPdoInstance();
    }

    /**
     * @return \PDO|\SeasClick
     */
    protected function createPdoInstance()
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
     * @param int $attempt
     * @throws Exception
     */
    public function reconnect(int $attempt = 0): void
    {
        unset($this->pdo);
        $this->pdo = null;
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
     * @param bool $release
     * @param string $name
     */
    public function release($release = false, string $name = 'db'): void
    {
        $transaction = $this->getTransaction();
        if (!empty($transaction) && $transaction->getIsActive()) {//事务里面不释放连接
            return;
        }
        if ($this->isAutoRelease() || $release) {
            PoolManager::getPool($this->poolKey)->release($this);
            ClickContext::delete($name);
        }
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
}
