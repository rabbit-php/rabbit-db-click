<?php

declare(strict_types=1);

namespace Rabbit\DB\Click;

use Exception;
use Generator;
use OneCk\Client;
use Psr\SimpleCache\CacheInterface;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\App;
use Rabbit\DB\DataReader;
use Rabbit\DB\Query;
use Rabbit\Server\ProcessShare;
use Throwable;

/**
 * Class Command
 * @package Rabbit\DB\Click
 */
class Command extends \Rabbit\DB\Command
{
    const FETCH = 'fetch';
    const FETCH_ALL = 'fetchAll';
    const FETCH_COLUMN = 'fetchColumn';
    const FETCH_SCALAR = 'fetchScalar';

    public int $fetchMode = 0;
    private ?int $executed = null;

    /**
     * @param array $values
     * @return $this
     */
    public function bindValues(array $values): self
    {
        if (empty($values)) {
            return $this;
        }
        //$schema = $this->db->getSchema();
        foreach ($values as $name => $value) {
            if (is_array($value)) {
                $this->params[$name] = $value[0];
            } else {
                $this->params[$name] = $value;
            }
        }

        return $this;
    }

    /**
     * @return int
     * @throws Throwable
     */
    public function execute(): int
    {
        if ($this->executed === null) {
            $rawSql = $this->getRawSql();

            $this->logQuery($rawSql, 'clickhouse');
            $res = $this->db->query($rawSql);
        } else {
            $log = "Inserted with SeasClick";
            $this->logQuery($log, 'clickhouse');
            $res = $this->executed;
            $this->executed = null;
        }
        return (int)$res;
    }


    /**
     * @return array|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function queryColumn(): ?array
    {
        return $this->queryInternal(self::FETCH_COLUMN);
    }

    /**
     * @return string|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function queryScalar(): null|string|bool|int|float|array
    {
        return $this->queryInternal(self::FETCH_SCALAR, 0);
    }

    /**
     * @param string $method
     * @param int|null $fetchMode
     * @return array|mixed|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    protected function queryInternal(string $method, int $fetchMode = null): null|string|bool|int|float|array|DataReader
    {
        $rawSql = $this->getRawSql();
        $share = $this->share ?? $this->db->share;
        if ($method === self::FETCH) {
            if (preg_match('#^SELECT#is', $rawSql) && !preg_match('#LIMIT#is', $rawSql)) {
                $rawSql .= ' LIMIT 1';
            }
        }

        $func = function () use ($method, &$rawSql, $fetchMode) {
            if ($method !== '') {
                $info = $this->db->getQueryCacheInfo($this->queryCacheDuration, $this->cache);
                if (is_array($info)) {
                    /** @var CacheInterface $cache */
                    $cache = $info[0];
                    $cacheKey = array_filter([
                        __CLASS__,
                        $method,
                        $fetchMode,
                        $this->db->dsn,
                        $rawSql,
                    ]);
                    $cacheKey = extension_loaded('igbinary') ? igbinary_serialize($cacheKey) : serialize($cacheKey);
                    $cacheKey = md5($cacheKey);
                    if (!empty($ret = $cache->get($cacheKey))) {
                        $result = unserialize($ret);
                        if (is_array($result) && isset($result[0])) {
                            $rawSql .= '; [Query result served from cache]';
                            $this->logQuery($rawSql, 'clickhouse');
                            return $this->prepareResult($result[0], $method);
                        }
                    }
                }
            }

            $this->logQuery($rawSql, 'clickhouse');

            try {
                if ($this->db->getIsExt()) {
                    $data = $this->db->select($rawSql);
                } else {
                    $data = $this->db->query($rawSql);
                }
                $result = $this->prepareResult($data, $method);
            } catch (Exception $e) {
                throw new Exception("Query error: " . $e->getMessage());
            }

            if (isset($cache, $cacheKey, $info)) {
                !$cache->has($cacheKey) && $cache->set((string)$cacheKey, serialize([$data]), $info[1]) && App::debug(
                    'Saved query result in cache',
                    'clickhouse'
                );
            }

            return $result;
        };

        if ($share > 0) {
            $cacheKey = array_filter([
                __CLASS__,
                $method,
                $fetchMode,
                $this->db->dsn,
                $rawSql,
            ]);
            $cacheKey = extension_loaded('igbinary') ? igbinary_serialize($cacheKey) : serialize($cacheKey);
            $cacheKey = md5($cacheKey);
            $type = $this->db->shareType;
            $s = $type($cacheKey, $func, $share);
            $status = $s->getStatus();
            if ($status === SWOOLE_CHANNEL_CLOSED) {
                $rawSql .= '; [Query result read from channel share]';
                $this->logQuery($rawSql);
            } elseif ($status === ProcessShare::STATUS_PROCESS) {
                $rawSql .= '; [Query result read from process share]';
                $this->logQuery($rawSql);
            } elseif ($status === ProcessShare::STATUS_CHANNEL) {
                $rawSql .= '; [Query result read from process channel share]';
                $this->logQuery($rawSql);
            }
            return $s->result;
        }
        return $func();
    }

    /**
     * @param array $result
     * @param string|null $method
     * @return array|mixed
     */
    private function prepareResult(array $result, string $method = null): null|string|bool|int|float|array
    {
        switch ($method) {
            case self::FETCH_COLUMN:
                return array_map(function ($a) {
                    return array_values($a)[0];
                }, $result);
            case self::FETCH_SCALAR:
                if (array_key_exists(0, $result)) {
                    return current($result[0]);
                }
                break;
            case self::FETCH:
                return is_array($result) ? array_shift($result) : $result;
        }

        return $result;
    }

    /**
     * @param string $table
     * @param array|Query $columns
     * @return $this
     */
    public function insert(string $table, array|Query $columns, bool $withUpdate = false): self
    {
        if ($this->db instanceof Client) {
            $this->db->insert($table, $columns);
        } else {
            $this->executed = $this->db->insert($table, array_keys($columns), array_values($columns));
        }
        return $this;
    }

    /**
     * @param string $table
     * @param array $columns
     * @param array|Generator $rows
     * @return $this
     */
    public function batchInsert(string $table, array $columns, $rows): self
    {
        if ($this->db instanceof Client) {
            $this->db->writeStart($table, $columns);
            $this->db->writeBlock($rows);
            $this->db->writeEnd();
        } else {
            $this->executed = $this->db->insert($table, $columns, $rows);
        }
        return $this;
    }

    /**
     * @return DataReader
     * @throws Exception
     */
    public function query(): DataReader
    {
        throw new Exception('Clichouse unsupport cursor');
    }
}
