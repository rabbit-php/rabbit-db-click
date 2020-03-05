<?php


namespace rabbit\db\click;

use rabbit\core\Context;
use rabbit\db\ConnectionInterface;

/**
 * Class ClickContext
 * @package rabbit\db\click
 */
class ClickContext extends Context
{
    /**
     * @param string $key
     * @return array|null
     */
    public static function getAll(?string $key = 'clickhouse'): ?array
    {
        return parent::getAll($key); // TODO: Change the autogenerated stub
    }

    /**
     * @param array $config
     * @param string $key
     */
    public static function setAll($config = [], ?string $key = 'clickhouse'): void
    {
        parent::setAll($config, $key); // TODO: Change the autogenerated stub
    }

    /**
     * @param string $name
     * @param string $key
     * @return |null
     */
    public static function get(string $name, ?string $key = 'clickhouse')
    {
        return parent::get($name, $key); // TODO: Change the autogenerated stub
    }

    /**
     * @param string $name
     * @param $value
     * @param string $key
     */
    public static function set(string $name, $value, ?string $key = 'clickhouse'): void
    {
        parent::set($name, $value, $key); // TODO: Change the autogenerated stub
    }

    /**
     * @param string $name
     * @param string $key
     * @return bool
     */
    public static function has(string $name, ?string $key = 'clickhouse'): bool
    {
        return parent::has($name, $key); // TODO: Change the autogenerated stub
    }

    /**
     * @param string $name
     * @param string $key
     */
    public static function delete(string $name, ?string $key = 'clickhouse'): void
    {
        parent::delete($name, $key);
    }

    /**
     *
     */
    public static function release(): void
    {
        $context = \Co::getContext();
        if (isset($context['clickhouse'])) {
            /** @var ConnectionInterface $connection */
            foreach ($context['clickhouse'] as $connection) {
                $connection->release(true);
            }
        }
    }
}
