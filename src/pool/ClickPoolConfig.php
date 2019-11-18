<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 9:49
 */

namespace rabbit\db\click\pool;

use rabbit\pool\PoolProperties;

/**
 * Class ClickPoolConfig
 * @package rabbit\db\click\pool
 */
class ClickPoolConfig extends PoolProperties
{
    /** @var array */
    private $config = [];

    /**
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    /**
     * @param array $config
     */
    public function setConfig(array $config)
    {
        $this->config = $config;
    }
}
