<?php

declare(strict_types=1);

namespace Rabbit\DB\Click\Client;

use Rabbit\DB\Exception;

class Read
{
    /**
     * @var resource
     */
    private $conn;

    private $buf = '';

    private int $i = 0;

    private int $len = 0;
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
     * @param integer $n
     * @return string
     */
    public function fixed(int $n): string
    {
        $s = str_repeat('1', $n);
        for ($i = 0; $i < $n; $i++) {
            $s[$i] = $this->getChar();
        }
        return $s;
    }
    /**
     * @author Albert <63851587@qq.com>
     * @return string
     */
    private function getChar(): string
    {
        if ($this->i >= $this->len) {
            $this->get();
            if ($this->i >= $this->len) {
                throw new Exception('read fail', null, 10002);
            }
        }
        $r = $this->buf[$this->i];
        $this->i++;
        return $r;
    }
    /**
     * @author Albert <63851587@qq.com>
     * @return void
     */
    private function get(): void
    {
        $buffer = fread($this->conn, 4096);
        if ($buffer === false) {
            throw new Exception('read from remote timeout', null, 10003);
        }
        $this->buf .= $buffer;
        $this->len = strlen($this->buf);
    }
    /**
     * @author Albert <63851587@qq.com>
     * @return void
     */
    public function flush(): void
    {
        $this->buf = substr($this->buf, $this->i);
        $this->len = strlen($this->buf);
        $this->i   = 0;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @return integer
     */
    public function number(): int
    {
        $r = 0;
        $b = 0;
        while (1) {
            $j = ord($this->getChar());
            $r = ($j << ($b * 7)) | $r;
            if ($j < 128) {
                return $r;
            }
            $b++;
        }
    }

    /**
     * @author Albert <63851587@qq.com>
     * @return integer
     */
    public function int(): int
    {
        return unpack('l', $this->fixed(4))[1];
    }

    /**
     * @author Albert <63851587@qq.com>
     * @return string
     */
    public function string(): string
    {
        $l = ord($this->getChar());
        $s = '';
        for ($i = 0; $i < $l; $i++) {
            $s .= $this->getChar();
        }
        return $s;
    }
}
