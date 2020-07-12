<?php
declare(strict_types=1);

namespace Rabbit\DB\Click;

use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use Rabbit\DB\BatchQueryResult;
use Rabbit\DB\Command;
use Rabbit\DB\ConnectionInterface;
use Throwable;

/**
 * Class Query
 * @package Rabbit\DB\Click
 */
class Query extends \Rabbit\DB\Query
{
    /**
     * @var null
     */
    public $sample = null;
    public $preWhere = null;
    public $limitBy = null;

    public function __construct(?\Rabbit\Pool\ConnectionInterface $db = null, array $config = [])
    {
        parent::__construct($db ?? getDI('click')->get(), $config);
    }

    /**
     * @return Command
     */
    public function createCommand(): Command
    {
        [$sql, $params] = $this->db->getQueryBuilder()->build($this);

        $command = $this->db->createCommand($sql, $params);
        $this->setCommandCache($command);

        return $command;
    }

    /**
     * @param int $batchSize
     * @return BatchQueryResult
     * @throws DependencyException
     * @throws NotFoundException
     * @throws \ReflectionException
     */
    public function batch(int $batchSize = 100): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $this->db,
            'each' => false,
        ], [], false);
    }

    /**
     * @param int $batchSize
     * @return BatchQueryResult
     * @throws DependencyException
     * @throws NotFoundException
     * @throws \ReflectionException
     */
    public function each(int $batchSize = 100): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $this->db,
            'each' => true,
        ], [], false);
    }
}
