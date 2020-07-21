<?php
declare(strict_types=1);

namespace Rabbit\DB\Click;

use DI\DependencyException;
use DI\NotFoundException;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\App;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\DbContext;
use Throwable;

/**
 * Class Connection
 * @package Rabbit\DB\Click
 */
class Connection extends \Rabbit\DB\Connection
{
    /** @var array|string[] */
    public array $schemaMap = [
        'click' => Schema::class
    ];
    /** @var string */
    public string $database = 'default';
    /** @var string */
    protected string $commandClass = Command::class;
    /** @var bool */
    protected bool $compression;
    /** @var string */
    protected string $host;
    /** @var int */
    protected int $port;

    /**
     * Connection constructor.
     * @param string $dsn
     * @param string $poolKey
     */
    public function __construct(string $dsn, string $poolKey)
    {
        parent::__construct($dsn);
        $this->poolKey = $poolKey;
        $this->driver = 'click';
        $parsed = $this->parseDsn;
        isset($parsed['query']) ? parse_str($parsed['query'], $parsed['query']) : $parsed['query'] = [];
        [$this->host, $this->port, $this->username, $this->password, $query] = ArrayHelper::getValueByArray(
            $parsed,
            ['host', 'port', 'user', 'pass', 'query'],
            ['127.0.0.1', 9000, '', '', []]
        );
        $this->database = (string)ArrayHelper::remove($query, 'dbname');
        $this->compression = (bool)ArrayHelper::remove($query, 'compression', true);
    }

    /**
     * @param string $value
     * @return string
     * @throws DependencyException
     * @throws NotFoundException
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function quoteValue(string $value): string
    {
        return $this->getSchema()->quoteValue($value);
    }

    /**
     * @param string $sql
     * @return string
     */
    public function quoteSql(string $sql): string
    {
        return $sql;
    }


    /**
     * @return array
     * @throws Throwable
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }


    /**
     * @throws Throwable
     */
    public function close(): void
    {
        if ($this->getIsActive()) {
            App::warning('Closing DB connection: ' . $this->shortDsn, 'clickhouse');
        }
    }

    /**
     * @return \SeasClick
     */
    public function createPdoInstance()
    {
        $client = new \SeasClick([
            "host" => $this->host,
            "port" => $this->port,
            "compression" => $this->compression,
            "database" => $this->database,
            "user" => $this->username,
            "passwd" => $this->password
        ]);
        return $client;
    }

    /**
     * @param $name
     * @param $arguments
     * @return mixed
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function __call($name, $arguments)
    {
        $attempt = 0;
        $this->open();
        while (true) {
            try {
                $conn = DbContext::get($this->poolName, $this->driver);
                return $conn->$name(...$arguments);
            } catch (Throwable $exception) {
                if (($retryHandler = $this->getRetryHandler()) === null || !$retryHandler->handle($exception, $attempt++)) {
                    App::error($exception->getMessage());
                    throw $exception;
                }
                $this->reconnect($attempt);
            }
        }
    }

    /**
     * @return mixed|\rabbit\db\Schema
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function getSchema(): \Rabbit\DB\Schema
    {
        return $this->_schema = create([
            'class' => Schema::class,
            'db' => $this
        ]);
    }

    public function quoteTableName(string $name): string
    {
        return $name;
    }

    public function quoteColumnName(string $name): string
    {
        return $name;
    }

    /**
     * @param null $conn
     */
    public function setInsertId($conn = null): void
    {
        return;
    }
}
