<?php

declare(strict_types=1);

namespace Rabbit\DB\Click;

use OneCk\Client;
use Rabbit\Base\App;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\ClickHouse\Query;
use Rabbit\DB\DbContext;
use Rabbit\DB\QueryInterface;
use SeasClick;
use Throwable;

class Connection extends \Rabbit\DB\Connection
{
    public readonly string $database;
    protected string $commandClass = Command::class;
    protected bool $compression;
    protected string $host;
    protected int $port;
    protected bool $isExt = false;
    protected int $timeout = 3;

    public function __construct(protected string $dsn, string $poolKey)
    {
        parent::__construct($dsn);
        $this->poolKey = $poolKey;
        $this->driver = 'clickhouse';
        $parsed = $this->parseDsn;
        isset($parsed['query']) ? parse_str($parsed['query'], $parsed['query']) : $parsed['query'] = [];
        [$this->host, $this->port, $this->username, $this->password, $query] = ArrayHelper::getValueByArray(
            $parsed,
            ['host', 'port', 'user', 'pass', 'query'],
            ['127.0.0.1', 9000, '', '', []]
        );
        $this->database = (string)ArrayHelper::remove($query, 'dbname', 'default');
        $this->compression = (bool)ArrayHelper::remove($query, 'compression', true);
        $this->timeout = (int)ArrayHelper::remove($query, 'timeout', $this->getPool()?->getTimeout() ?? $this->timeout);
        $this->isExt = (bool)ArrayHelper::remove($query, 'isext', false);
        $this->canTransaction = false;
    }

    public function quoteValue(string $value): string
    {
        return $this->getSchema()->quoteValue($value);
    }

    public function quoteSql(string $sql): string
    {
        return $sql;
    }

    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }

    public function close(): void
    {
        if ($this->getIsActive()) {
            App::warning('Closing DB connection: ' . $this->shortDsn, 'clickhouse');
        }
    }

    public function createPdoInstance(): object
    {
        if ($this->isExt) {
            $client = new SeasClick([
                "host" => $this->host,
                "port" => $this->port,
                "compression" => $this->compression,
                "database" => $this->database,
                "user" => $this->username,
                "passwd" => $this->password
            ]);
        } else {
            $client = new Client("tcp://$this->host:$this->port", $this->username, $this->password, $this->database, ['socket_timeout' => $this->timeout]);
        }

        return $client;
    }

    public function __call($name, $arguments)
    {
        $attempt = 0;
        $this->open();
        while (true) {
            try {
                $conn = DbContext::get($this->poolKey)->pdo;
                if ($conn instanceof SeasClick && $name === 'query') {
                    return $conn->execute(...$arguments);
                }
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

    public function getSchema(): \Rabbit\DB\Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }
        return $this->schema = new Schema($this);
    }

    public function quoteTableName(string $name): string
    {
        return $name;
    }

    public function quoteColumnName(string $name): string
    {
        return $name;
    }

    public function setInsertId(object $conn = null): void
    {
    }

    public function buildQuery(): QueryInterface
    {
        return new Query($this);
    }

    public function getIsExt(): bool
    {
        return $this->isExt;
    }
}
