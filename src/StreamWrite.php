<?php

declare(strict_types=1);

namespace Rabbit\DB\Click;

use OneCk\Client;
use Rabbit\Base\Contract\InitInterface;
use Rabbit\Base\Core\Channel;
use Throwable;

class StreamWrite implements InitInterface
{
    private ?Client $client = null;
    private Channel $channel;
    private bool $status = false;

    public function __construct(private string $table, private array $field, private int $batch = 1000, private int $sleep = 3, private string $db = 'click')
    {
        $this->channel = new Channel();
    }

    public function init(): void
    {
        $this->send();
    }

    public function write(array $data): void
    {
        $this->channel->push($data);
    }

    public function send(): void
    {
        if (!$this->status) {
            $this->status = true;
            $this->start();
            $num = 0;
            $buffer = [];
            loop(function () use (&$num, &$buffer) {
                for ($i = 0; $i < $this->batch; $i++) {
                    if (false === $data = $this->channel->pop($this->sleep)) {
                        break;
                    }
                    if (count($buffer) > 0) {
                        $this->client->writeBlock($buffer);
                        $buffer = [];
                    }
                    try {
                        $this->client->writeBlock($data);
                    } catch (Throwable $e) {
                        $buffer = [...$buffer, ...$data];
                        $this->start();
                    } finally {
                        $num++;
                    }
                }
                if ($num > 0) {
                    $num = 0;
                    $this->client->writeEnd();
                    $this->client->writeStart($this->table, $this->field);
                }
            });
        }
    }

    private function start(): void
    {
        $client = getDI('db')->get($this->db);
        $client->getPool()->sub();
        $client->open();
        $this->client = $client->getConn();
        $this->client->writeStart($this->table, $this->field);
    }
}
