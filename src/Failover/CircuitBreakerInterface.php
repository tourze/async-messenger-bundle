<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

interface CircuitBreakerInterface
{
    public function isAvailable(string $transportName): bool;
    
    public function recordSuccess(string $transportName): void;
    
    public function recordFailure(string $transportName, \Throwable $exception): void;
    
    public function getState(string $transportName): CircuitBreakerState;
}
