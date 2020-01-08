<?php


namespace rabbit\db\click;

use rabbit\db\ConnectionInterface;

/**'
 * Class BatchInsert
 * @package rabbit\db
 */
class BatchInsert
{
    /** @var string */
    protected $table;
    /** @var array */
    protected $columns = [];
    /** @var ConnectionInterface */
    protected $db;
    /** @var array */
    private $rows = [];

    public function __construct(string $table, ConnectionInterface $db)
    {
        $this->table = $table;
        $this->db = $db;
    }

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
    public function addRow(array $rows, bool $checkFields = true): bool
    {
        if (empty($rows)) {
            return false;
        }
        $this->rows = array_merge($this->rows, $rows);
        return true;
    }

    /**
     * @return int
     */
    public function execute(): int
    {
        if ($this->rows) {
            return $this->db->pdo->insert($this->table, $this->columns, $this->rows);
        }
        return 0;
    }
}
