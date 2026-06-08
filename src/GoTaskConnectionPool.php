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

namespace Hyperf\GoTask;

use Hyperf\Contract\ConnectionInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\GoTask\Config\DomainConfig;
use Hyperf\Pool\Frequency;
use Hyperf\Pool\Pool;
use Psr\Container\ContainerInterface;
use Throwable;

use function Hyperf\Support\make;

class GoTaskConnectionPool extends Pool
{
    private bool $debug = false;

    private float $debugWaitMs = 20.0;

    private float $debugCallMs = 200.0;

    private float $debugPressureRatio = 0.8;

    public function __construct(ContainerInterface $container, DomainConfig $config)
    {
        $options = $config->getPoolOptions();
        $this->debug = (bool) ($options['debug'] ?? false);
        $this->debugWaitMs = (float) ($options['debug_wait_ms'] ?? 20.0);
        $this->debugCallMs = (float) ($options['debug_call_ms'] ?? 200.0);
        $this->debugPressureRatio = (float) ($options['debug_pressure_ratio'] ?? 0.8);
        $this->frequency = make(Frequency::class);
        parent::__construct($container, $options);
    }

    public function get(): ConnectionInterface
    {
        if (! $this->debug) {
            return parent::get();
        }

        $startedAt = microtime(true);
        try {
            return parent::get();
        } finally {
            $this->reportPoolWait((microtime(true) - $startedAt) * 1000);
        }
    }

    public function createConnection(): ConnectionInterface
    {
        return make(GoTaskConnection::class, ['pool' => $this]);
    }

    public function reportCall(string $method, float $elapsedMs, bool $hasContextConnection, ?Throwable $throwable = null): void
    {
        if (! $this->debug) {
            return;
        }

        if ($throwable === null && $elapsedMs < $this->debugCallMs) {
            return;
        }

        $this->warning('gotask_rpc_slow', [
            'method' => $method,
            'elapsed_ms' => round($elapsedMs, 2),
            'context_reused' => $hasContextConnection,
            'pid' => getmypid(),
            'error_class' => $throwable ? get_class($throwable) : null,
            'error_message' => $throwable?->getMessage(),
        ]);
    }

    private function reportPoolWait(float $waitMs): void
    {
        $current = $this->getCurrentConnections();
        $idle = $this->getConnectionsInChannel();
        $active = $current - $idle;
        $max = $this->getOption()->getMaxConnections();
        $pressureRatio = $max > 0 ? $active / $max : 0.0;

        if ($waitMs < $this->debugWaitMs && $pressureRatio < $this->debugPressureRatio) {
            return;
        }

        $this->warning('gotask_pool_pressure', [
            'wait_ms' => round($waitMs, 2),
            'current' => $current,
            'idle' => $idle,
            'active' => $active,
            'max' => $max,
            'pressure_ratio' => round($pressureRatio, 4),
            'pid' => getmypid(),
        ]);
    }

    private function warning(string $message, array $context): void
    {
        try {
            if ($this->container->has(StdoutLoggerInterface::class)) {
                $this->container->get(StdoutLoggerInterface::class)->warning($message, $context);
            }
        } catch (Throwable) {
        }
    }
}
