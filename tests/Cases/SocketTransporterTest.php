<?php

declare(strict_types=1);

namespace HyperfTest\Cases;

use Hyperf\GoTask\Relay\SocketTransporter;
use PHPUnit\Framework\TestCase;
use Spiral\Goridge\Exceptions\RelayException;

class SocketTransporterTest extends TestCase
{
    public function testReceiveSyncThrowsWhenBodyReadFails(): void
    {
        $relay = new SocketTransporterHarness(new SocketTransporterFakeSocket([
            pack('CPJ', 0, 5, 5),
            false,
        ]));

        $this->expectException(RelayException::class);

        $flags = null;
        $relay->receiveSync($flags);
    }
}

class SocketTransporterHarness
{
    use SocketTransporter;

    public const BUFFER_SIZE = 10;
    public const PAYLOAD_NONE = 1;

    private ?SocketTransporterFakeSocket $socket;

    public function __construct(SocketTransporterFakeSocket $socket)
    {
        $this->socket = $socket;
    }

    public function connect(): bool
    {
        return true;
    }
}

class SocketTransporterFakeSocket
{
    public string $errMsg = 'fake socket read failed';
    public int $errCode = 1001;

    private int $reads = 0;

    public function __construct(private array $chunks)
    {
    }

    public function recv(int $length): string|false
    {
        if (! array_key_exists($this->reads, $this->chunks)) {
            throw new \RuntimeException('fake socket would spin forever');
        }

        return $this->chunks[$this->reads++];
    }

    public function close(): void
    {
    }
}
