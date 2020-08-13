<?php
declare(strict_types=1);

namespace Rabbit\DB\Click;


use DI\DependencyException;
use DI\NotFoundException;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\DB\ClickHouse\TableSchema;
use Rabbit\DB\ColumnSchema;
use ReflectionException;
use Throwable;

/**
 * Class Schema
 * @package Rabbit\DB\ClickHouse
 */
class Schema extends \Rabbit\DB\ClickHouse\Schema
{
    /**
     * @param string $name
     * @return \Rabbit\DB\TableSchema|null
     * @throws DependencyException
     * @throws InvalidArgumentException
     * @throws NotFoundException
     * @throws Throwable
     */
    protected function loadTableSchema(string $name): ?\Rabbit\DB\TableSchema
    {
        $sql = 'SELECT * FROM system.columns WHERE `table`=:name and `database`=:database';
        $result = $this->db->createCommand($sql, [
            ':name' => $name,
            ':database' => $this->db->database === null ? 'default' : $this->db->database
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

    /**
     * @param array $info
     * @return ColumnSchema
     * @throws DependencyException
     * @throws NotFoundException|ReflectionException
     */
    protected function loadColumnSchema(array $info): ColumnSchema
    {
        $column = $this->createColumnSchema();
        $column->name = $info['name'];
        $column->dbType = $info['type'];
        $column->type = isset(self::$typeMap[$column->dbType]) ? self::$typeMap[$column->dbType] : self::TYPE_STRING;


        if (preg_match('/^([\w ]+)(?:\(([^\)]+)\))?$/', $column->dbType, $matches)) {
            $type = $matches[1];
            $column->dbType = $matches[1] . (isset($matches[2]) ? "({$matches[2]})" : '');
            if (isset(self::$typeMap[$type])) {
                $column->type = self::$typeMap[$type];
            }
        }

        $unsignedTypes = ['UInt8', 'UInt16', 'UInt32', 'UInt64'];
        if (in_array($column->dbType, $unsignedTypes)) {
            $column->unsigned = true;
        }

        $column->phpType = $this->getColumnPhpType($column);
        if (empty($info['default_type'])) {
            $column->defaultValue = $info['default_expression'];
        }
        return $column;
    }
}
