<?php
declare(strict_types=1);

namespace rabbit\db\click;

use rabbit\db\ConnectionInterface;
use rabbit\db\Exception;
use rabbit\db\RetryHandlerInterface;

/**
 * Class RetryHandler
 * @package rabbit\db\click
 */
class RetryHandler extends RetryHandlerInterface
{
    /** @var int */
    private $sleep = 1;

    /**
     * RetryHandler constructor.
     * @param int $totalCount
     */
    public function __construct(int $totalCount = 3)
    {
        $this->totalCount = $totalCount;
    }

    /**
     * @return int
     */
    public function getTotalCount(): int
    {
        return $this->totalCount;
    }

    /**
     * @param int $count
     */
    public function setTotalCount(int $count): void
    {
        $this->totalCount = $count;
    }

    /**
     * @param ConnectionInterface $db
     * @param \Throwable $e
     * @param int $count
     * @return bool
     */
    public function handle(\Throwable $e, int $count): bool
    {
        if ($count < $this->totalCount) {
            $count > 1 && \Co::sleep($this->sleep);
            return true;
        }
        return false;
    }
}
