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

    /**
     * @param ConnectionInterface|null $db
     * @return Command
     * @throws Throwable
     */
    public function createCommand(ConnectionInterface $db = null): Command
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
     * @param int $batchSize
     * @param ConnectionInterface|null $db
     * @return BatchQueryResult
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function batch(int $batchSize = 100, ConnectionInterface $db = null): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $db,
            'each' => false,
        ], [], false);
    }

    /**
     * @param int $batchSize
     * @param ConnectionInterface|null $db
     * @return BatchQueryResult
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function each(int $batchSize = 100, ConnectionInterface $db = null): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $db,
            'each' => true,
        ], [], false);
    }
}
