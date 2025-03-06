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

namespace Hyperf\GoTask\MongoClient\Type;

use MongoDB\BSON\Int64;
use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Unserializable;

class BulkWriteResult implements Unserializable
{
    private Int64 $matchedCount;

    private Int64 $modifiedCount;

    private Int64 $upsertedCount;

    private Int64 $deletedCount;

    private Int64 $insertedCount;

    /**
     * @var array<ObjectId>
     */
    private array $upsertedIds;

    public function bsonUnserialize(array $data): void
    {
        $this->matchedCount = $data['matchedcount'];
        $this->modifiedCount = $data['modifiedcount'];
        $this->upsertedCount = $data['upsertedcount'];
        $this->deletedCount = $data['deletedcount'];
        $this->insertedCount = $data['insertedcount'];
        $this->upsertedIds = (array) $data['upsertedids'];
    }

    public function getMatchedCount(): Int64
    {
        return $this->matchedCount;
    }

    public function getModifiedCount(): Int64
    {
        return $this->modifiedCount;
    }

    public function getUpsertedCount(): Int64
    {
        return $this->upsertedCount;
    }

    public function getDeletedCount(): Int64
    {
        return $this->deletedCount;
    }

    public function getUpsertedIds(): array
    {
        return (array) $this->upsertedIds;
    }

    public function getinsertedCount(): Int64
    {
        return $this->insertedCount;
    }
}
