<?php
declare(strict_types=1);

namespace Rabbit\DB\Click;


/**
 * Class BatchInsert
 * @package Rabbit\DB\Click
 */
class BatchInsert extends \Rabbit\DB\BatchInsert
{
    /** @var array */
    private array $rows = [];

    /**
     * @return int
     */
    public function getRows(): int
    {
        return count($this->rows);
    }

    /**
     * @param array $columns
     * @return bool
     */
    public function addColumns(array $columns): bool
    {
        if (empty($columns)) {
            return false;
        }
        $this->columns = $columns;
        return true;
    }

    /**
     * @param array $rows
     * @param bool $isIndex
     * @return bool
     */
    public function addRow(array $rows, bool $isIndex = false): bool
    {
        if (empty($rows)) {
            return false;
        }
        if ($isIndex) {
            $this->rows = array_merge($this->rows, $rows);
        } else {
            $this->rows[] = $rows;
        }
        return true;
    }

    /**
     * @return int
     */
    public function execute(): int
    {
        if ($this->rows) {
            return (int)$this->db->insert($this->table, $this->columns, $this->rows);
        }
        return 0;
    }
}
