<?php
declare(strict_types=1);

namespace Rabbit\DB\Click;

use Exception;
use Generator;
use Psr\SimpleCache\CacheInterface;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\DB\DataReader;
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

    /** @var int fetch type result */
    public int $fetchMode = 0;
    /** @var int */
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
                $this->_pendingParams[$name] = $value;
                $this->params[$name] = $value[0];
            } else {
                $this->params[$name] = $value;
            }
        }

        return $this;
    }

    /**
     * @return int
     * @throws Exception
     */
    public function execute(): int
    {
        if ($this->executed === null) {
            $rawSql = $this->getRawSql();

            $this->logQuery($rawSql, 'clickhouse');
            $res = $this->db->execute($rawSql);
        } else {
            $this->logQuery("Inserted with SeasClick", 'clickhouse');
            $res = $this->executed;
            $this->executed = null;
        }
        return (int)$res;
    }


    /**
     * @return array|null
     * @throws InvalidArgumentException
     */
    public function queryColumn(): ?array
    {
        return $this->queryInternal(self::FETCH_COLUMN);
    }

    /**
     * @return string|null
     * @throws InvalidArgumentException
     */
    public function queryScalar(): ?string
    {
        $result = $this->queryInternal(self::FETCH_SCALAR, 0);
        if (is_array($result)) {
            return current($result);
        } else {
            return $result === null ? null : (string)$result;
        }
    }

    /**
     * @return string
     */
    public function getRawSql(): string
    {
        if (empty($this->params)) {
            return $this->_sql;
        }
        $params = [];
        foreach ($this->params as $name => $value) {
            if (is_string($name) && strncmp(':', $name, 1)) {
                $name = ':' . $name;
            }
            if (is_string($value)) {
                $params[$name] = $this->db->quoteValue($value);
            } elseif (is_bool($value)) {
                $params[$name] = ($value ? 'TRUE' : 'FALSE');
            } elseif ($value === null) {
                $params[$name] = 'NULL';
            } elseif (!is_object($value) && !is_resource($value)) {
                $params[$name] = $value;
            }
        }
        if (!isset($params[0])) {
            return strtr($this->_sql, $params);
        }
        $sql = '';
        foreach (explode('?', $this->_sql) as $i => $part) {
            $sql .= $part . (isset($params[$i]) ? $params[$i] : '');
        }
        return $sql;
    }

    /**
     * @param string $method
     * @param int $fetchMode
     * @return array|mixed|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    protected function queryInternal(string $method, int $fetchMode = null)
    {
        $rawSql = $this->getRawSql();
        if ($method === self::FETCH) {
            if (preg_match('#^SELECT#is', $rawSql) && !preg_match('#LIMIT#is', $rawSql)) {
                $rawSql .= ' LIMIT 1';
            }
        }

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
                if (!empty($ret = $cache->get($cacheKey))) {
                    $result = unserialize($ret);
                    if (is_array($result) && isset($result[0])) {
                        $this->logQuery($rawSql . '; [Query result served from cache]', 'clickhouse');
                        return $this->prepareResult($result[0], $method);
                    }
                }
            }
        }

        $this->logQuery($rawSql);

        try {
            $data = $this->db->select($rawSql);
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
    }

    /**
     * @param array $result
     * @param string|null $method
     * @return array|mixed
     */
    private function prepareResult(array $result, string $method = null)
    {
        switch ($method) {
            case self::FETCH_COLUMN:
                return array_map(function ($a) {
                    return array_values($a)[0];
                }, $result);
                break;
            case self::FETCH_SCALAR:
                if (array_key_exists(0, $result)) {
                    return current($result[0]);
                }
                break;
            case self::FETCH:
                return is_array($result) ? array_shift($result) : $result;
                break;
        }

        return $result;
    }

    /**
     * @param string $table
     * @param array|\Rabbit\DB\Query $columns
     * @return $this
     */
    public function insert(string $table, $columns): self
    {
        $this->executed = $this->db->insert($table, array_keys($columns), array_values($columns));
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
        $this->executed = $this->db->insert($table, $columns, $rows);
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
