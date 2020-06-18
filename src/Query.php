<?php

namespace rabbit\db\click;

use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use rabbit\core\ObjectFactory;
use rabbit\db\clickhouse\BatchQueryResult;
use rabbit\db\Exception as DbException;
use rabbit\db\Query as BaseQuery;

/**
 * Class Query
 * @package rabbit\db\click
 */
class Query extends BaseQuery
{
    /**
     * @var null
     */
    public $sample = null;
    public $preWhere = null;
    public $limitBy = null;

    /**
     * @param null $db
     * @return \rabbit\db\Command
     * @throws Exception
     */
    public function createCommand($db = null)
    {
        if ($db === null) {
            $db = getDI('click')->get();
        }
        [$sql, $params] = $db->getQueryBuilder()->build($this);

        $command = $db->createCommand($sql, $params);
        $this->setCommandCache($command);

        return $command;
    }

    /**
     * Starts a batch query.
     *
     * A batch query supports fetching data in batches, which can keep the memory usage under a limit.
     * This method will return a [[BatchQueryResult]] object which implements the [[\Iterator]] interface
     * and can be traversed to retrieve the data in batches.
     *
     * For example,
     *
     * ```php
     * $query = (new Query)->from('user');
     * foreach ($query->batch() as $rows) {
     *     // $rows is an array of 100 or fewer rows from user table
     * }
     * ```
     *
     * @param int $batchSize the number of records to be fetched in each batch.
     * @param Connection $db the database connection. If not set, the "db" application component will be used.
     * @return BatchQueryResult the batch query result. It implements the [[\Iterator]] interface
     * and can be traversed to retrieve the data in batches.
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function batch($batchSize = 100, $db = null)
    {
        return ObjectFactory::createObject([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $db,
            'each' => false,
        ], [], false);
    }

    /**
     * Starts a batch query and retrieves data row by row.
     *
     * This method is similar to [[batch()]] except that in each iteration of the result,
     * only one row of data is returned. For example,
     *
     * ```php
     * $query = (new Query)->from('user');
     * foreach ($query->each() as $row) {
     * }
     * ```
     *
     * @param int $batchSize the number of records to be fetched in each batch.
     * @param Connection $db the database connection. If not set, the "db" application component will be used.
     * @return BatchQueryResult the batch query result. It implements the [[\Iterator]] interface
     * and can be traversed to retrieve the data in batches.
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function each($batchSize = 100, $db = null)
    {
        return ObjectFactory::createObject([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $db,
            'each' => true,
        ], [], false);
    }
}
