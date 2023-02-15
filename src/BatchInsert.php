<?php

declare(strict_types=1);

namespace Rabbit\DB\Click;

use Rabbit\DB\ClickHouse\BatchInsert as ClickHouseBatchInsert;
use Throwable;

class BatchInsert extends ClickHouseBatchInsert
{
    protected array $rows = [];

    public function execute(): int
    {
        if ($this->rows) {
            try {
                $this->db->writeStart($this->table, $this->columns);
                $this->db->writeBlock($this->rows);
                return (int)$this->db->writeEnd();
            } catch (Throwable $e) {
                throw $e;
            } finally {
                $this->db->release();
            }
        }
        return 0;
    }

    protected function bindRows(array $rows): void
    {
        $this->rows[] = $rows;
    }

    protected function quoteValue(string $str): string
    {
        return $str;
    }
}
