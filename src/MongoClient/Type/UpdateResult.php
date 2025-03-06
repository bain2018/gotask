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
use MongoDB\BSON\Unserializable;

class UpdateResult implements Unserializable
{
    private Int64 $matchedCount;

    private Int64 $modifiedCount;

    private Int64 $upsertedCount;

    private ?string $upsertedId;

    public function bsonUnserialize(array $data): void
    {
        $this->matchedCount = $data['matchedcount'];
        $this->modifiedCount = $data['modifiedcount'];
        $this->upsertedCount = $data['upsertedcount'];
        $this->upsertedId = $data['upsertedid'];
    }

    /**
     * @return mixed
     */
    public function getUpsertedId(): ?string
    {
        return $this->upsertedId;
    }

    public function getUpsertedCount(): Int64
    {
        return $this->upsertedCount;
    }

    public function getModifiedCount(): Int64
    {
        return $this->modifiedCount;
    }

    public function getMatchedCount(): Int64
    {
        return $this->matchedCount;
    }
}
