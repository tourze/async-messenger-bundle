<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Symfony\Component\Messenger\Transport\TransportInterface;

interface ConsumptionStrategyInterface
{
    /**
     * @param array<string, TransportInterface> $transports
     * @return string|null The name of the transport to consume from, or null if none available
     */
    public function selectTransport(array $transports, CircuitBreakerInterface $circuitBreaker): ?string;
    
    /**
     * Record consumption result for future decision making
     */
    public function recordResult(string $transportName, bool $success, float $latency): void;
}