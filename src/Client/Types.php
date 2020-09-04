<?php

declare(strict_types=1);

namespace Rabbit\DB\Click\Client;

use Rabbit\DB\Exception;

class Types
{
    private static array $typePack = [
        'int8'    => ['c', 1],
        'uint8'   => ['C', 1],
        'int16'   => ['s', 2],
        'uint16'  => ['S', 2],
        'int32'   => ['l', 4],
        'uint32'  => ['L', 4],
        'int64'   => ['q', 8],
        'uint64'  => ['Q', 8],
        'float32' => ['f', 4],
        'float64' => ['d', 8],
    ];
    protected Write $write;
    protected Read $read;

    public function __construct(Write $write, Read $read)
    {
        $this->write = $write;
        $this->read  = $read;
    }

    public static function toIpv6(string $ip)
    {
        $ar = explode(':', $ip);
        if (count($ar) < 8 || strpos($ip, '::')) {
            $r = [];
            foreach ($ar as $v) {
                if (empty($v)) {
                    $r = array_merge($r, array_fill(0, 9 - count($ar), '0000'));
                    continue;
                }
                $r[] = strlen($v) < 4 ? str_repeat('0', 4 - strlen($v)) . $v : $v;
            }
            $ar = $r;
        }
        return hex2bin(implode($ar));
    }

    public static function toFixedString(string $str, int $n)
    {
        return $str . str_repeat(chr(0), $n - strlen($str));
    }

    protected function map(string $type): string
    {
        static $arr = [
            'decimal32' => 'float32',
            'decimal64' => 'float64',
            'date'      => 'uint16',
            'datetime'  => 'uint32',
            'ipv4'      => 'uint32',
            'ipv6'      => 'fixedstring(16)',
            'enum8'     => 'int8',
            'enum16'    => 'int16'
        ];

        if (isset($arr[$type])) {
            return $arr[$type];
        }

        if (substr($type, 0, 8) === 'decimal(') {
            $arr = explode(',', substr($type, 8, -1));
            if ($arr[0] < 10) {
                return 'int32';
            } else if ($arr[0] < 19) {
                return 'int64';
            } else {
                return 'int128';
            }
        }
        if (substr($type, 0, 11) === 'datetime64(') {
            return 'uint64';
        }
        return $type;
    }
    /**
     * @author Albert <63851587@qq.com>
     * @param [type] $data
     * @param string $type
     * @return void
     */
    protected function writeFormat($data, string $type)
    {
        if (isset(self::$typePack[$type])) {
            return $data;
        }

        if ($type === 'string') {
            return $data;
        }

        switch ($type) {
            case 'date':
            case 'datetime':
                return strtotime($data);
            case 'ipv4':
                return ip2long($data);
            case 'ipv6':
                return self::toIpv6((string)$data);
        }
        if (substr($type, 0, 8) === 'decimal(') {
            $arr = explode(',', substr($type, 8, -1));
            if (is_string($data)) {
                return str_replace('.', '', $data);
            } else {
                return $data * pow(10, $arr[1]);
            }
        }
        if (substr($type, 0, 11) === 'datetime64(') {
            $n = substr($type, 11, -1);
            return self::toDatetime64((string)$data, (int)$n);
        }
        return $data;
    }
    /**
     * @author Albert <63851587@qq.com>
     * @param string $time
     * @param integer $n
     * @return void
     */
    public static function toDatetime64(string $time, int $n = 3)
    {
        $ar = explode('.', $time);
        $l  = isset($ar[1]) ? strlen($ar[1]) : 0;
        $n  = strtotime($ar[0]) . (isset($ar[1]) ? $ar[1] : '') . str_repeat('0', min(max($n - $l, 0), 9));
        return $n * 1;
    }
    /**
     * @author Albert <63851587@qq.com>
     * @param string $data
     * @return void
     */
    protected function ipv6Unpack(string $data)
    {
        $s = bin2hex($data);
        $r = [];
        $a = '';
        for ($i = 0; $i < 32; $i++) {
            $a .= $s[$i];
            if ($i < 31 && $i % 4 === 3) {
                $r[] = ltrim($a, '0');
                $a   = '';
            }
        }
        $r[] = ltrim($a, '0');
        $r   = implode(':', $r);
        while (strpos($r, ':::') !== false) {
            $r = str_replace(':::', '::', $r);
        }
        return $r;
    }
    /**
     * @author Albert <63851587@qq.com>
     * @param string $data
     * @param string $type
     * @return void
     */
    protected function readFormat($data, string $type)
    {
        if (isset(self::$typePack[$type])) {
            return $data;
        }

        if ($type === 'string') {
            return $data;
        }

        switch ($type) {
            case 'date':
                return date('Y-m-d', $data);
            case 'datetime':
                return date('Y-m-d H:i:s', $data);
            case 'ipv4':
                return long2ip($data);
            case 'ipv6':
                return $this->ipv6Unpack($data);
        }
        if (substr($type, 0, 8) === 'decimal(') {
            $arr = explode(',', substr($type, 8, -1));
            if (is_string($data)) {
                return substr($data, 0, -$arr[1]) . '.' . substr($data, -$arr[1]);
            } else {
                return $data / pow(10, $arr[1]);
            }
        }
        if (substr($type, 0, 11) === 'datetime64(') {
            return date('Y-m-d H:i:s', substr($data, 0, 10)) . '.' . substr($data, 10);
        }
        return $data;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param string $type
     * @return void
     */
    protected function sInfo(string $type)
    {
        $type = strtolower(trim($type));
        if (isset(self::$typePack[$type])) {
            return unpack(self::$typePack[$type][0], $this->read->fixed(self::$typePack[$type][1]))[1];
        } else if ($type === 'string') {
            return $this->read->string();
        } else if (substr($type, 0, 12) === 'fixedstring(') {
            return $this->read->fixed(intval(substr($type, 12, -1)));
        } else if ($type === 'int128') {
            return $this->int128Unpack();
        } else if ($type === 'uuid') {
            return $this->uuidUnpack();
        } else {
            throw new Exception('unset type :' . $type, null, 10126);
        }
    }
    /**
     * @author Albert <63851587@qq.com>
     * @param string $type
     * @return void
     */
    public function unpack(string $type)
    {
        return $this->readFormat(
            $this->sInfo($this->map($type)),
            $type
        );
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param [type] $data
     * @param string $type
     * @return void
     */
    public function pack($data, string $type): void
    {
        $data = $this->writeFormat($data, $type);
        $type = $this->map($type);
        if (isset(self::$typePack[$type])) {
            $pack = self::$typePack[$type][0];
            $this->write->addBuf(pack("$pack*", $data));
        } else if ($type === 'string' || $type === 'uuid') {
            $this->write->string($data);
        } else if (substr($type, 0, 12) === 'fixedstring(') {
            $n = intval(substr($type, 12, -1));
            $this->write->addBuf(self::toFixedString($data, $n));
        } else if ($type === 'int128') {
            $this->write->addBuf($this->int128Pack($data));
        } else {
            throw new Exception('unset type :' . $type, null, 10125);
        }
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param string $n
     * @return string
     */
    public function int128Pack(string $n): string
    {
        if (!is_string($n)) {
            $n = "{$n}";
        }
        $is_n = false;
        if ($n[0] === '-') {
            $is_n = true;
        }
        $r = '';
        for ($i = 0; $i < 16; $i++) {
            $b = bcpow(2, 8);
            $c = bcmod($n, $b);
            $n = bcdiv($n, $b, 0);
            if ($is_n) {
                $v = ~abs(intval($c));
                if ($i === 0) {
                    $v = $v + 1;
                }
            } else {
                $v = intval($c);
            }
            $r .= chr($v);
        }
        return $r;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @return string
     */
    protected function int128Unpack(): string
    {
        $str  = $this->read->fixed(16);
        $is_n = false;
        if (ord($str[15]) > 127) {
            $is_n = true;
        }
        $r = '0';
        for ($i = 0; $i < 16; $i++) {
            $n = ord($str[$i]);
            if ($is_n) {
                if ($i === 0) {
                    $n = $n - 1;
                }
                $n = ~$n & 255;
            }
            if ($n !== 0) {
                $r = bcadd(bcmul("{$n}", bcpow(2, 8 * $i)), $r);
            }
        }
        return $is_n ? '-' . $r : $r;
    }
}
