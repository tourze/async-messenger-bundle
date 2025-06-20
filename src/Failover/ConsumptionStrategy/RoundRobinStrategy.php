<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy;

use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategyInterface;

class RoundRobinStrategy implements ConsumptionStrategyInterface
{
    private int $currentIndex = 0;

    public function selectTransport(array $transports, CircuitBreakerInterface $circuitBreaker): ?string
    {
        $transportNames = array_keys($transports);
        $count = count($transportNames);
        
        if ($count === 0) {
            return null;
        }
        
        // Try each transport starting from current index
        for ($i = 0; $i < $count; $i++) {
            $index = ($this->currentIndex + $i) % $count;
            $transportName = $transportNames[$index];
            
            if ($circuitBreaker->isAvailable($transportName)) {
                $this->currentIndex = ($index + 1) % $count;
                return $transportName;
            }
        }
        
        return null;
    }

    public function recordResult(string $transportName, bool $success, float $latency): void
    {
        // Round robin doesn't use results for decision making
    }
}