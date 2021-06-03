<?php

declare(strict_types=1);

namespace Rabbit\DB\Click;

use OneCk\Client;

class StreamWrite
{
    private string $table;
    private array $field;
    private ?Client $client = null;
    private string $db;
    private bool $isStart = false;

    public function __construct(string $table, array $field, string $db = 'click')
    {
        $this->table = $table;
        $this->field = $field;
        $this->db = $db;
    }

    public function start(): void
    {
        if ($this->client === null) {
            $client = getDI('db')->get($this->db);
            $client->getPool()->sub();
            $client->open();
            $this->client = $client->getConn();
        }
        if (!$this->isStart) {
            $this->isStart = true;
            $this->client->writeStart($this->table, $this->field);
        }
    }

    public function write(array $data): void
    {
        if (!$this->isStart) {
            $this->start();
        }
        $this->client->writeBlock($data);
    }

    public function flush(): void
    {
        if ($this->isStart) {
            $this->isStart = false;
            $this->client->writeEnd();
            $this->start();
        }
    }
}
