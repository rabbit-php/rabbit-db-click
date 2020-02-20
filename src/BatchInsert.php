<?php
declare(strict_types=1);

namespace rabbit\db\click;

use rabbit\db\ConnectionInterface;

/**'
 * Class BatchInsert
 * @package rabbit\db
 */
class BatchInsert extends \rabbit\db\BatchInsert
{
    /** @var array */
    private $rows = [];

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
     * @param bool $checkFields
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
