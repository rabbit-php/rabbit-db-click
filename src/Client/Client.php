<?php

declare(strict_types=1);

namespace Rabbit\DB\Click\Client;

use Rabbit\DB\Exception;

class Client
{
    const CLIENT_HELLO  = 0;
    const CLIENT_QUERY  = 1;
    const CLIENT_DATA   = 2;
    const CLIENT_CANCEL = 3;
    const CLIENT_PING   = 4;

    const SERVER_HELLO         = 0;
    const SERVER_DATA          = 1;
    const SERVER_EXCEPTION     = 2;
    const SERVER_PROGRESS      = 3;
    const SERVER_PONG          = 4;
    const SERVER_END_OF_STREAM = 5;
    const SERVER_PROFILE_INFO  = 6;
    const SERVER_TOTALS        = 7;
    const SERVER_EXTREMES      = 8;

    const COMPRESSION_DISABLE = 0;
    const COMPRESSION_ENABLE  = 1;
    const STAGES_COMPLETE     = 2;

    const NAME          = 'PHP-RABBIT-CLIENT';
    const VERSION       = 54213;
    const VERSION_MAJOR = 1;
    const VERSION_MINOR = 1;

    const DBMS_MIN_V_TEMPORARY_TABLES         = 50264;
    const DBMS_MIN_V_TOTAL_ROWS_IN_PROGRESS   = 51554;
    const DBMS_MIN_V_BLOCK_INFO               = 51903;
    const DBMS_MIN_V_CLIENT_INFO              = 54032;
    const DBMS_MIN_V_SERVER_TIMEZONE          = 54058;
    const DBMS_MIN_V_QUOTA_KEY_IN_CLIENT_INFO = 54060;

    private $conn;
    private ?Write $write = null;
    private ?Read $read = null;
    private ?Types $types = null;
    private array $conf = [];


    private $serverInfo = [];
    private $rowData    = [];
    private $totalRow   = 0;
    private $isNull     = [];
    private $fields       = [];
    /**
     * @author Albert <63851587@qq.com>
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param string $database
     * @param array $options
     */
    public function __construct(string $host = '127.0.0.1', int $port = 9000, $username = 'default', $password = '', $database = 'default', $options = [])
    {
        $time_out   = isset($options['time_out']) ? $options['time_out'] : 3;
        $this->conn = stream_socket_client("tcp://$host:$port", $code, $msg, $time_out);
        if (!$this->conn) {
            throw new Exception($msg, null, $code);
        }
        stream_set_timeout($this->conn, $time_out);
        $this->write = new Write($this->conn);
        $this->read  = new Read($this->conn);
        $this->types = new Types($this->write, $this->read);
        $this->conf  = [$username, $password, $database];
        $this->hello(...$this->conf);
    }
    /**
     * @author Albert <63851587@qq.com>
     * @return void
     */
    private function addClientInfo(): void
    {
        $this->write->string(self::NAME)->number(self::VERSION_MAJOR, self::VERSION_MINOR, self::VERSION);
    }
    /**
     * @author Albert <63851587@qq.com>
     * @param string $username
     * @param string $password
     * @param string $database
     * @return void
     */
    private function hello(string $username, string  $password, string $database)
    {
        $this->write->number(self::CLIENT_HELLO)->string(self::NAME)->number(self::VERSION_MAJOR, self::VERSION_MINOR, self::VERSION)->string($database, $username, $password)->flush();
        return $this->receive();
    }

    /**
     * @return bool
     */
    public function ping(): bool
    {
        $this->write->number(self::CLIENT_PING)->flush();
        return $this->receive() === null;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @return array|null
     */
    private function receive()
    {
        $this->rowData  = [];
        $this->totalRow = 0;
        $this->isNull   = [];
        $this->fields     = [];
        $progressInfo   = [];
        $profileInfo    = [];

        $code = null;
        do {
            if ($code === null) {
                $code = $this->read->number();
            }
            switch ($code) {
                case self::SERVER_HELLO:
                    $this->setServerInfo();
                    return null;
                case self::SERVER_EXCEPTION:
                    $this->readErr();
                case self::SERVER_DATA:
                    $n = $this->readData();
                    if ($n > 1) {
                        $code = $n;
                    }
                    continue 2;
                case self::SERVER_PROGRESS:
                    $progressInfo = [
                        'rows'       => $this->read->number(),
                        'bytes'      => $this->read->number(),
                        'total_rows' => $this->gtV(self::DBMS_MIN_V_TOTAL_ROWS_IN_PROGRESS) ? $this->read->number() : 0,
                    ];
                    $this->read->flush();
                    break;
                case self::SERVER_END_OF_STREAM:
                    $this->read->flush();
                    return $this->totalRow;
                case self::SERVER_PROFILE_INFO:
                    $profileInfo = [
                        'rows'                         => $this->read->number(),
                        'blocks'                       => $this->read->number(),
                        'bytes'                        => $this->read->number(),
                        'applied_limit'                => $this->read->number(),
                        'rows_before_limit'            => $this->read->number(),
                        'calculated_rows_before_limit' => $this->read->number()
                    ];
                    $this->read->flush();
                    break;
                case self::SERVER_TOTALS:
                case self::SERVER_EXTREMES:
                    break;
                case self::SERVER_PONG:
                    $this->read->flush();
                    return null;
                default:
                    throw new Exception('undefined code ' . $code, \null, 10005);
            }
            $code = null;
        } while (true);
    }
    /**
     * @author Albert <63851587@qq.com>
     * @param [type] $v
     * @return boolean
     */
    private function gtV(int $v): bool
    {
        return $this->serverInfo['version'] >= $v;
    }


    /**
     * @author Albert <63851587@qq.com>
     * @return array
     */
    public function getServerInfo(): array
    {
        return $this->serverInfo;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param string $t
     * @param integer $n
     * @return string
     */
    private function isNull(string $t, int $n): string
    {
        $t = strtolower($t);
        if (strpos($t, 'nullable(') === 0) {
            for ($i = 0; $i < $n; $i++) {
                $j = $this->read->number();
                if ($j === 1) {
                    $this->isNull[$i] = 1;
                }
            }
            return substr($t, 9, -1);
        } else {
            $this->isNull = [];
            return $t;
        }
    }

    /**
     * @author Albert <63851587@qq.com>
     * @return integer
     */
    private function readData(): int
    {
        if (count($this->fields) === 0) {
            $this->readHeader();
        }
        list($code, $row_count) = $this->readHeader();
        if ($row_count === 0) {
            return $code;
        }
        foreach ($this->fields as $t) {
            $f = $this->read->string();
            $t = $this->read->string();
            $t = $this->isNull($t, $row_count);
            for ($i = 0; $i < $row_count; $i++) {
                $v = $this->types->unpack($t);

                $this->rowData[$i + $this->totalRow][$f] = isset($this->isNull[$i]) ? null : $v;
            }
        }
        $this->totalRow += $row_count;
        $this->read->flush();
        return 1;
    }
    /**
     * @author Albert <63851587@qq.com>
     * @return void
     */
    private function readHeader(): array
    {
        $n = $this->read->number();
        if ($n > 1) {
            return [$n, 0];
        }
        $info = [
            'num1'         => $this->read->number(),
            'is_overflows' => $this->read->number(),
            'num2'         => $this->read->number(),
            'bucket_num'   => $this->read->int(),
            'num3'         => $this->read->number(),
            'col_count'    => $this->read->number(),
            'row_count'    => $this->read->number(),
        ];
        if (count($this->fields) === 0) {
            for ($i = 0; $i < $info['col_count']; $i++) {
                $this->fields[$this->read->string()] = $this->read->string();
            }
        }
        $this->read->flush();
        return [0, $info['row_count']];
    }
    /**
     * @author Albert <63851587@qq.com>
     * @return void
     */
    private function setServerInfo(): void
    {
        $this->serverInfo              = [
            'name'          => $this->read->string(),
            'major_version' => $this->read->number(),
            'minor_version' => $this->read->number(),
            'version'       => $this->read->number(),
        ];
        $this->serverInfo['time_zone'] = $this->gtV(self::DBMS_MIN_V_SERVER_TIMEZONE) ? $this->read->string() : '';
        $this->read->flush();
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param string $sql
     * @return void
     */
    private function sendQuery(string $sql): void
    {
        $this->write->number(self::CLIENT_QUERY, 0);

        if ($this->gtV(self::DBMS_MIN_V_CLIENT_INFO)) {
            $this->write->number(1)->string('', '', '[::ffff:127.0.0.1]:0')->number(1)->string('', '');
            $this->addClientInfo();
            if ($this->gtV(self::DBMS_MIN_V_QUOTA_KEY_IN_CLIENT_INFO)) {
                $this->write->string('');
            }
        }

        $this->write->number(0, self::STAGES_COMPLETE, self::COMPRESSION_DISABLE)->string($sql);
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param string $sql
     * @return void
     */
    public function execute(string $sql)
    {
        $this->sendQuery($sql);
        $this->writeEnd();
        return $this->receive();
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param string $table
     * @param array $data
     * @return array|null
     */
    public function insert(string $table, array $columns, array $data)
    {
        $this->writeStart($table, $columns);
        $this->writeBlock($data);
        $this->writeEnd();
        return $this->receive();
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param string $table
     * @param array $fields
     * @return void
     */
    public function writeStart(string $table, array $fields): void
    {
        $table = trim($table);
        $this->sendQuery('INSERT INTO ' . $table . ' (' . implode(',', $fields) . ') VALUES ');
        $this->writeEnd();
        while (true) {
            $code = $this->read->number();
            if ($code == self::SERVER_DATA) {
                $this->readHeader();
                break;
            } else if ($code == self::SERVER_PROGRESS) {
                continue;
            } else if ($code == self::SERVER_EXCEPTION) {
                $this->readErr();
            } else {
                throw new Exception('insert err code:' . $code, null, 10015);
            }
        }
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param array $data
     * @return void
     */
    public function writeBlock(array $data)
    {
        if (count($this->fields) === 0) {
            throw new Exception('Please execute first writeStart', null, 10036);
        }
        $this->writeBlockHead();

        // column count , row Count
        $row_count = count($data);
        $this->write->number(count($data[0]), $row_count);

        $new_data = [];
        foreach ($data as $row) {
            foreach ($row as $k => $v) {
                $new_data[$k][] = $v;
            }
        }

        foreach ($new_data as $field => $data) {
            $type = $this->fields[$field];
            $this->write->string($field, $type);
            $type = $this->writeIsNull($type, $data);
            $this->write->number(...$this->isNull);
            foreach ($data as $i => $d) {
                $this->types->pack(
                    (isset($this->isNull[$i]) && $this->isNull[$i] === 1) ?
                        0 :
                        $d,
                    $type
                );
                $this->write->flush();
            }
        }
        $this->write->flush();
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param string $type
     * @param string $data
     * @return void
     */
    private function writeIsNull(string $type, array $data): string
    {
        $t = strtolower($type);
        if (strpos($t, 'nullable(') === 0) {
            foreach ($data as $i => $v) {
                if ($v === null) {
                    $this->isNull[$i] = 1;
                } else {
                    $this->isNull[$i] = 0;
                }
            }
            return substr($t, 9, -1);
        } else {
            $this->isNull = [];
            return $t;
        }
    }

    /**
     * @author Albert <63851587@qq.com>
     * @return void
     */
    public function writeEnd(): void
    {
        $this->writeBlockHead();
        $this->write->number(0)->number(0)->flush();
    }
    /**
     * @author Albert <63851587@qq.com>
     * @return void
     */
    private function writeBlockHead(): void
    {
        $this->write->number(self::CLIENT_DATA);
        if ($this->gtV(self::DBMS_MIN_V_TEMPORARY_TABLES)) {
            $this->write->number(0);
        }
        if ($this->gtV(self::DBMS_MIN_V_BLOCK_INFO)) {
            $this->write->number(1, 0, 2)->int(-1)->number(0);
        }
    }
    /**
     * @author Albert <63851587@qq.com>
     * @return void
     */
    private function readErr(): void
    {
        $c   = $this->read->int();
        $n   = $this->read->string();
        $msg = $this->read->string();
        $this->read->flush();
        throw new Exception(substr($msg, strlen($n) + 1), null, $c);
    }
}
