<?php

declare(strict_types=1);

namespace Rabbit\DB\Click;

use Rabbit\DB\ClickHouse\BatchInsert as ClickHouseBatchInsert;

class BatchInsert extends ClickHouseBatchInsert
{
    protected array $rows = [];

    public function execute(): int
    {
        if (count($this->delItems) > 0) {
            $this->db->createCommand("ALTER TABLE {$this->table} DELETE WHERE {$this->delKey} in (" . implode(', ', $this->delItems) . ')')->execute();
        }
        if ($this->rows) {
            $this->db->writeStart($this->table, $this->columns);
            $this->db->writeBlock($this->rows);
            return (int)$this->db->writeEnd();
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
