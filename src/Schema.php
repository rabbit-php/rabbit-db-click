<?php

namespace rabbit\db\click;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\db\Exception;
use rabbit\db\TableSchema;

/**
 * Class Schema
 * @package rabbit\db\clickhouse
 */
class Schema extends \rabbit\db\clickhouse\Schema
{
    /**
     * @param string $name
     * @return TableSchema|null
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Exception
     */
    protected function loadTableSchema($name)
    {
        $sql = 'SELECT * FROM system.columns WHERE `table`=:name and `database`=:database';
        $result = $this->db->createCommand($sql, [
            ':name' => $name,
            ':database' => $this->db->database
        ])->queryAll();

        if ($result && isset($result[0])) {
            $table = new TableSchema();
            $table->schemaName = $result[0]['database'];
            $table->name = $name;
            $table->fullName = $table->schemaName . '.' . $table->name;

            foreach ($result as $info) {
                $column = $this->loadColumnSchema($info);
                $table->columns[$column->name] = $column;
            }
            return $table;
        }

        return null;
    }
}
