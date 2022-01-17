<?php

declare(strict_types=1);

namespace Rabbit\DB\Click;

use OneCk\Client;
use Rabbit\Base\Core\Channel;

class StreamWrite
{
    private ?Client $client = null;
    private Channel $channel;
    private int $num = 0;

    public function __construct(private string $table, private array $field, private string $db = 'click')
    {
        $this->channel = new Channel();
    }

    public function write(array $data): void
    {
        $this->channel->push($data);
    }

    public function send(int $sleep = 3): void
    {
        static $run = false;
        if (!$run) {
            $run = true;
            $client = getDI('db')->get($this->db);
            $client->getPool()->sub();
            $client->open();
            $this->client = $client->getConn();
            $this->client->writeStart($this->table, $this->field);
            $time = time();
            loop(function () use (&$time, $sleep) {
                $data = $this->channel->pop($sleep);
                if ($data !== false) {
                    $this->client->writeBlock($data);
                    $this->num++;
                }
                $now = time();
                if ($this->num > 0 && $now - $time > $sleep) {
                    $this->client->writeEnd();
                    $this->client->writeStart($this->table, $this->field);
                    $time = $now;
                    $this->num = 0;
                }
            });
        }
    }
}
