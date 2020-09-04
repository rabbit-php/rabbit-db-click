<?php

declare(strict_types=1);

namespace Rabbit\DB\Click\Client;

use Rabbit\DB\Exception;

class Write
{
    private $conn;

    private $buf = '';
    /**
     * @author Albert <63851587@qq.com>
     * @param [type] $conn
     */
    public function __construct($conn)
    {
        $this->conn = $conn;
    }


    /**
     * @author Albert <63851587@qq.com>
     * @param [type] ...$nr
     * @return self
     */
    public function number(...$nr): self
    {
        $r = [];
        foreach ($nr as $n) {
            $b = 0;
            while ($n >= 128) {
                $r[] = $n | 128;
                $b++;
                $n = $n >> 7;
            }
            $r[] = $n;
        }
        if ($r) {
            $this->buf .= pack('C*', ...$r);
        }
        return $this;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param integer $n
     * @return self
     */
    public function int(int $n): self
    {
        $this->buf .= pack('l', $n);
        return $this;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param [type] ...$str
     * @return self
     */
    public function string(...$str): self
    {
        foreach ($str as $s) {
            $this->number(strlen($s));
            $this->buf .= $s;
        }
        return $this;
    }


    /**
     * @author Albert <63851587@qq.com>
     * @param string $str
     * @return self
     */
    public function addBuf(string $str): self
    {
        $this->buf .= $str;
        return $this;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @return boolean
     */
    public function flush(): bool
    {
        if ($this->buf === '') {
            return true;
        }

        $len = fwrite($this->conn, $this->buf);
        if ($len !== strlen($this->buf)) {
            throw new Exception('write fail', null, 10001);
        }
        $this->buf = '';
        return true;
    }
}
