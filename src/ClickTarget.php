<?php

declare(strict_types=1);

namespace Rabbit\DB\Click;

use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Log\Targets\AbstractTarget;

class ClickTarget extends AbstractTarget
{
    protected array $template = [
        ['appname', 'string'],
        ['datetime', 'timespan'],
        ['level', 'string'],
        ['request_uri', 'string'],
        ['request_method', 'string'],
        ['clientip', 'string'],
        ['requestid', 'string'],
        ['filename', 'string'],
        ['memoryusage', 'int'],
        ['message', 'string']
    ];

    protected ?StreamWrite $stream = null;

    public function __construct(protected string $table)
    {
        parent::__construct();
    }

    public function export(array $msg): void
    {
        if ($this->stream === null) {
            $fields = [];
            foreach ($this->template as [$field, $_]) {
                $fields[] = $field;
            }
            $this->stream = new StreamWrite($this->table, $fields);
            $this->stream->init();
        }
        ArrayHelper::remove($msg, '%c');
        if (!empty($this->levelList) && !in_array($msg[$this->levelIndex], $this->levelList)) {
            return;
        }
        $log = [];
        foreach ($msg as $index => $value) {
            [$name, $type] = $this->template[$index];
            switch ($type) {
                case "string":
                    $log[$name] = trim($value);
                    break;
                case "int":
                    $log[$name] = (int)$value;
                    break;
                default:
                    $log[$name] = trim($value);
            }
        }
        if ($this->batch > 0) {
            $this->loop();
            $this->channel->push($log);
        } else {
            $this->flush([$log]);
        }
    }

    protected function flush(array|string &$logs): void
    {
        $this->stream->write($logs);
    }
}
