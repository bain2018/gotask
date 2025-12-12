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
use Hyperf\GoTask\MongoClient\Type\BulkWriteResult;
use Hyperf\GoTask\MongoClient\Type\DeleteResult;
use Hyperf\GoTask\MongoClient\Type\IndexInfo;
use Hyperf\GoTask\MongoClient\Type\InsertManyResult;
use Hyperf\GoTask\MongoClient\Type\InsertOneResult;
use Hyperf\GoTask\MongoClient\Type\UpdateResult;

class Collection
{
    use MongoTrait;

    public function __construct(
        private MongoProxy $mongo,
        private ConfigInterface $config,
        protected string $database,
        protected string $collection,
        private array $typeMap,
    ) {
    }

    public function insertOne($document = [], array $opts = []): InsertOneResult
    {
        $document = $this->sanitize($document);
        $data = $this->mongo->insertOne($this->makePayload([
            'Record' => $document,
        ], $opts));
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $document->toPHP(['root' => InsertOneResult::class]);
    }

    public function insertMany($documents = [], array $opts = []): InsertManyResult
    {
        $documents = $this->sanitize($documents);
        $data = $this->mongo->insertMany($this->makePayload([
            'Records' => $documents,
        ], $opts));

        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $document->toPHP(['root' => InsertManyResult::class]);
    }

    public function find($filter = [], array $opts = []): array|object
    {
        $filter = $this->sanitize($filter);
        $data = $this->mongo->find($this->makePayload([
            'Filter' => $filter,
        ], $opts));

        if ($data === '') {
            return [];
        }

        $typeMap = $opts['typeMap'] ?? $this->typeMap;
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP($typeMap);
    }

    public function findOne($filter = [], array $opts = []): array|object
    {
        $filter = $this->sanitize($filter);
        $data = $this->mongo->findOne($this->makePayload([
            'Filter' => $filter,
        ], $opts));

        if ($data === '') {
            return [];
        }

        $typeMap = $opts['typeMap'] ?? $this->typeMap;
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP($typeMap);
    }

    public function findOneAndDelete($filter = [], array $opts = []): array|object
    {
        $filter = $this->sanitize($filter);
        $data = $this->mongo->findOneAndDelete($this->makePayload([
            'Filter' => $filter,
        ], $opts));

        if ($data === '') {
            return [];
        }

        $typeMap = $opts['typeMap'] ?? $this->typeMap;
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP($typeMap);
    }

    public function findOneAndUpdate($filter = [], $update = [], array $opts = []): array|object
    {
        $filter = $this->sanitize($filter);
        $data = $this->mongo->findOneAndUpdate($this->makePayload([
            'Filter' => $filter,
            'Update' => $update,
        ], $opts));

        if ($data === '') {
            return [];
        }

        $typeMap = $opts['typeMap'] ?? $this->typeMap;
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP($typeMap);
    }

    public function findOneAndReplace($filter = [], $replace = [], array $opts = []): array|object
    {
        $filter = $this->sanitize($filter);
        $data = $this->mongo->findOneAndReplace($this->makePayload([
            'Filter' => $filter,
            'Replace' => $replace,
        ], $opts));

        if ($data === '') {
            return [];
        }

        $typeMap = $opts['typeMap'] ?? $this->typeMap;
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP($typeMap);
    }

    public function updateOne($filter = [], $update = [], array $opts = []): UpdateResult
    {
        $filter = $this->sanitize($filter);
        $update = $this->sanitize($update);
        $data = $this->mongo->updateOne($this->makePayload([
            'Filter' => $filter,
            'Update' => $update,
        ], $opts));
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return  $document->toPHP(['root' => UpdateResult::class]);
    }

    public function updateMany($filter = [], $update = [], array $opts = []): UpdateResult
    {
        $filter = $this->sanitize($filter);
        $update = $this->sanitize($update);
        $data = $this->mongo->updateMany($this->makePayload([
            'Filter' => $filter,
            'Update' => $update,
        ], $opts));

        $document= \MongoDB\BSON\Document::fromBSON($data);
        return  $document->toPHP(['root' => UpdateResult::class]);
    }

    public function replaceOne($filter = [], $replace = [], array $opts = []): UpdateResult
    {
        $filter = $this->sanitize($filter);
        $replace = $this->sanitize($replace);
        $data = $this->mongo->replaceOne($this->makePayload([
            'Filter' => $filter,
            'Replace' => $replace,
        ], $opts));
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return  $document->toPHP(['root' => UpdateResult::class]);
    }

    public function countDocuments($filter = [], array $opts = []): int
    {
        $filter = $this->sanitize($filter);
        $data = $this->mongo->countDocuments($this->makePayload([
            'Filter' => $filter,
        ], $opts));
        return unpack('P', $data)[1];
    }

    public function deleteOne($filter = [], array $opts = []): DeleteResult
    {
        $filter = $this->sanitize($filter);
        $data = $this->mongo->deleteOne($this->makePayload([
            'Filter' => $filter,
        ], $opts));

        $document= \MongoDB\BSON\Document::fromBSON($data);
        return  $document->toPHP(['root' => DeleteResult::class]);
    }

    public function deleteMany($filter = [], array $opts = []): DeleteResult
    {
        $filter = $this->sanitize($filter);
        $data = $this->mongo->deleteMany($this->makePayload([
            'Filter' => $filter,
        ], $opts));
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return  $document->toPHP(['root' => DeleteResult::class]);
    }

    public function aggregate($pipeline = [], array $opts = []): array|object
    {
        $pipeline = $this->sanitize($pipeline);
        $data = $this->mongo->aggregate($this->makePayload([
            'Pipeline' => $pipeline,
        ], $opts));
        if ($data === '') {
            return [];
        }
        $typeMap = $opts['typeMap'] ?? $this->typeMap;
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP($typeMap);
    }

    public function bulkWrite($operations = [], array $opts = []): BulkWriteResult
    {
        $operations = $this->sanitize($operations);
        $data = $this->mongo->bulkWrite($this->makePayload([
            'Operations' => $operations,
        ], $opts));

        $document= \MongoDB\BSON\Document::fromBSON($data);
        return  $document->toPHP(['root' => BulkWriteResult::class]);
    }

    public function distinct(string $fieldName, $filter = [], array $opts = []): array|object
    {
        $filter = $this->sanitize($filter);
        $data = $this->mongo->distinct($this->makePayload([
            'FieldName' => $fieldName,
            'Filter' => $filter,
        ], $opts));
        if ($data === '') {
            return [];
        }
        $typeMap = $opts['typeMap'] ?? $this->typeMap;
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP($typeMap);
    }

    public function createIndex($index = [], array $opts = []): string
    {
        $index = $this->sanitize($index);
        return $this->mongo->createIndex($this->makePayload([
            'IndexKeys' => $index,
        ], $opts));
    }

    public function createIndexes($indexes = [], array $opts = []): array|object
    {
        $indexes = $this->sanitize($indexes);
        $data = $this->mongo->createIndexes($this->makePayload([
            'Models' => $indexes,
        ], $opts));
        if ($data === '') {
            return [];
        }
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP(['root' => 'array']);
    }

    public function listIndexes($indexes = [], array $opts = []): array|object
    {
        $data = $this->mongo->listIndexes($this->makePayload([], $opts));
        if ($data === '') {
            return [];
        }
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP(['root' => 'array', 'document' => IndexInfo::class, 'fieldPaths' => ['$.key' => 'array']]);
    }

    public function dropIndex(string $name, array $opts = []): array|object
    {
        $data = $this->mongo->dropIndex($this->makePayload([
            'Name' => $name,
        ], $opts));
        if ($data === '') {
            return [];
        }
        $typeMap = $opts['typeMap'] ?? $this->typeMap;
        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP($typeMap);
    }

    public function dropIndexes(array $opts = []): array|object
    {
        $data = $this->mongo->dropIndexes($this->makePayload([
        ], $opts));
        if ($data === '') {
            return [];
        }
        $typeMap = $opts['typeMap'] ?? $this->typeMap;

        $document= \MongoDB\BSON\Document::fromBSON($data);
        return $data === '' ? [] : $document->toPHP($typeMap);
    }

    public function drop(): string
    {
        $document= \MongoDB\BSON\Document::fromPHP([
            'Database' => $this->database,
            'Collection' => $this->collection,
        ]);

        return $this->mongo->drop((string) $document);
    }

    private function makePayload(array $partial, array $opts): string
    {
        $document= \MongoDB\BSON\Document::fromPHP(array_merge($partial, [
            'Database' => $this->database,
            'Collection' => $this->collection,
            'Opts' => $this->sanitizeOpts($opts),
//             'OptsList' => $this->sanitizeOptsList($opts),
        ]));
        return (string) $document;
    }
}
