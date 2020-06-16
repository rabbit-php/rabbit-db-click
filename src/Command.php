<?php

namespace rabbit\db\click;

use Exception;
use Psr\SimpleCache\CacheInterface;
use Psr\SimpleCache\InvalidArgumentException;
use rabbit\App;
use rabbit\db\Command as BaseCommand;
use rabbit\db\Exception as DbException;

/**
 * Class Command
 * @package rabbit\db\click
 * @property $db \rabbit\db\click\Connection
 */
class Command extends BaseCommand
{
    const FETCH = 'fetch';
    const FETCH_ALL = 'fetchAll';
    const FETCH_COLUMN = 'fetchColumn';
    const FETCH_SCALAR = 'fetchScalar';

    /** @var int fetch type result */
    public $fetchMode = 0;
    /** @var int */
    private $executed = null;

    /**
     * @param array $values
     * @return $this|BaseCommand
     */
    public function bindValues($values)
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
    public function execute()
    {
        if ($this->executed === null) {
            $rawSql = $this->getRawSql();

            if (strlen($rawSql) < $this->db->maxLog) {
                $this->logQuery($rawSql, 'clickhouse');
            }
            $res = $this->db->execute($rawSql);
        } else {
            $this->logQuery("Inserted with SeasClick", 'clickhouse');
            $res = $this->executed;
            $this->executed = null;
        }
        return $res;
    }


    /**
     * @return array|mixed|null
     * @throws DbException
     * @throws InvalidArgumentException
     */
    public function queryColumn()
    {
        return $this->queryInternal(self::FETCH_COLUMN);
    }

    /**
     * @return array|false|int|mixed|string|null
     * @throws DbException
     * @throws InvalidArgumentException
     */
    public function queryScalar()
    {
        $result = $this->queryInternal(self::FETCH_SCALAR, 0);
        if (is_array($result)) {
            return current($result);
        } else {
            return $result;
        }
    }

    public function getRawSql()
    {
        if (empty($this->params)) {
            return $this->getSql();
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
            return strtr($this->getSql(), $params);
        }
        $sql = '';
        foreach (explode('?', $this->getSql()) as $i => $part) {
            $sql .= $part . (isset($params[$i]) ? $params[$i] : '');
        }
        return $sql;
    }

    /**
     * @param string $method
     * @param null $fetchMode
     * @return array|mixed|null
     * @throws DbException
     * @throws InvalidArgumentException
     * @throws Exception
     */
    protected function queryInternal($method, $fetchMode = null)
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
            throw new DbException("Query error: " . $e->getMessage());
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
     * @param $result
     * @param null $method
     * @return array|mixed|null
     */
    private function prepareResult($result, $method = null)
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
     * Creates an INSERT command.
     * For example,
     *
     * ```php
     * $connection->createCommand()->insert('user', [
     *     'name' => 'Sam',
     *     'age' => 30,
     * ])->execute();
     * ```
     *
     * The method will properly escape the column names, and bind the values to be inserted.
     *
     * Note that the created command is not executed until [[execute()]] is called.
     *
     * @param string $table the table that new rows will be inserted into.
     * @param array $columns the column data (name => value) to be inserted into the table.
     * @return $this the command object itself
     */
    public function insert($table, $columns)
    {
        $this->executed = $this->db->insert($table, array_keys($columns), array_values($columns));
        return $this;
    }

    /**
     * Creates a batch INSERT command.
     * For example,
     *
     * ```php
     * $connection->createCommand()->batchInsert('user', ['name', 'age'], [
     *     ['Tom', 30],
     *     ['Jane', 20],
     *     ['Linda', 25],
     * ])->execute();
     * ```
     * @param $table
     * @param $columns
     * @param $rows
     * @return Command
     */
    public function batchInsert($table, $columns, $rows)
    {
        $this->executed = $this->db->insert($table, $columns, $rows);
        return $this;
    }

    public function query()
    {
        throw new DbException('Clichouse unsupport cursor');
    }
}
