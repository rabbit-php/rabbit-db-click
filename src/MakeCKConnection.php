<?php


namespace rabbit\db\click;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\core\ObjectFactory;

class MakeCKConnection
{
    /**
     * @param string $class
     * @param string $name
     * @param string $dsn
     * @param array|null $config
     * @throws DependencyException
     * @throws NotFoundException
     */
    public static function addConnection(string $class, string $name, string $dsn, array $config = null): void
    {
        /** @var Manager $manager */
        $manager = getDI('click');
        if (!$manager->hasConnection($name)) {
            $conn = [
                'class' => $class,
                'dsn' => $dsn,
            ];
            if (is_array($config)) {
                foreach ($config as $key => $value) {
                    $conn[$key] = $value;
                }
            }
            $manager->addConnection([$name => ObjectFactory::createObject($conn, [], false)]);
        }
    }
}
