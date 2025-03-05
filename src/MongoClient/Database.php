<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf/GoTask.
 *
 * @link     https://www.github.com/hyperf/gotask
 * @document  https://www.github.com/hyperf/gotask
 * @contact  guxi99@gmail.com
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace Hyperf\GoTask\MongoClient;

use Hyperf\Contract\ConfigInterface;

use function MongoDB\BSON\fromPHP;
use function MongoDB\BSON\toPHP;

class Database
{
    use MongoTrait;

    public function __construct(
        private MongoProxy $mongo,
        private ConfigInterface $config,
        protected string $database,
        private array $typeMap,
    ) {
    }

    public function __get(string $collName): Collection
    {
        return new Collection($this->mongo, $this->config, $this->database, $collName, $this->typeMap);
    }

    public function collection(string $collName): Collection
    {
        return new Collection($this->mongo, $this->config, $this->database, $collName, $this->typeMap);
    }

    public function runCommand(array $command = [], array $opts = []): array|object|string
    {
        $payload = [
            'Database' => $this->database,
            'Command' => $this->sanitize($command),
            'Opts' => $this->sanitizeOpts($opts),
        ];

        $document= \MongoDB\BSON\Document::fromPHP($payload);
        $data = $this->mongo->runCommand($document->toCanonicalExtendedJSON());
        if ($data !== '') {
            $typeMap = $opts['typeMap'] ?? $this->typeMap;
            $document= \MongoDB\BSON\Document::fromBSON($data);
            return $document->toPHP($typeMap);
        }
        return '';
    }

    public function runCommandCursor(array $command = [], array $opts = []): array|object|string
    {
        $payload = [
            'Database' => $this->database,
            'Command' => $this->sanitize($command),
            'Opts' => $this->sanitizeOpts($opts),
        ];

        $document= \MongoDB\BSON\Document::fromPHP($payload);
        $data = $this->mongo->runCommandCursor($document->toCanonicalExtendedJSON());
        if ($data !== '') {
            $typeMap = $opts['typeMap'] ?? $this->typeMap;
            $document= \MongoDB\BSON\Document::fromBSON($data);
            return $document->toPHP($typeMap);
        }
        return '';
    }
}
