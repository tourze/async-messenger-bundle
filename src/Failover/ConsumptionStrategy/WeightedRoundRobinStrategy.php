<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy;

use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategyInterface;

class WeightedRoundRobinStrategy implements ConsumptionStrategyInterface
{
    private array $weights = [];
    private array $successCounts = [];
    private array $failureCounts = [];
    private int $currentIndex = 0;
    private array $weightedList = [];
    private bool $needsRebalance = true;

    public function selectTransport(array $transports, CircuitBreakerInterface $circuitBreaker): ?string
    {
        if ($this->needsRebalance) {
            $this->rebalanceWeights(array_keys($transports), $circuitBreaker);
        }
        
        if (empty($this->weightedList)) {
            return null;
        }
        
        // Try transports in weighted order
        $count = count($this->weightedList);
        for ($i = 0; $i < $count; $i++) {
            $index = ($this->currentIndex + $i) % $count;
            $transportName = $this->weightedList[$index];
            
            if ($circuitBreaker->isAvailable($transportName)) {
                $this->currentIndex = ($index + 1) % $count;
                return $transportName;
            }
        }
        
        return null;
    }

    private function rebalanceWeights(array $transportNames, CircuitBreakerInterface $circuitBreaker): void
    {
        $this->weights = [];

        foreach ($transportNames as $name) {
            if (!$circuitBreaker->isAvailable($name)) {
                continue;
            }

            $successes = $this->successCounts[$name] ?? 0;
            $failures = $this->failureCounts[$name] ?? 0;
            $total = $successes + $failures;

            if ($total === 0) {
                // New transport, give it average weight
                $this->weights[$name] = 50;
            } else {
                // Calculate weight based on success rate
                $successRate = $successes / $total;
                $this->weights[$name] = (int)($successRate * 100);
            }
        }

        // Build weighted list
        $this->weightedList = [];
        foreach ($this->weights as $name => $weight) {
            // Add transport name multiple times based on weight
            $count = max(1, (int)($weight / 10)); // Scale down to reasonable numbers
            for ($i = 0; $i < $count; $i++) {
                $this->weightedList[] = $name;
            }
        }

        // Shuffle to avoid patterns
        shuffle($this->weightedList);

        $this->needsRebalance = false;
    }

    public function recordResult(string $transportName, bool $success, float $latency): void
    {
        if ($success) {
            $this->successCounts[$transportName] = ($this->successCounts[$transportName] ?? 0) + 1;
        } else {
            $this->failureCounts[$transportName] = ($this->failureCounts[$transportName] ?? 0) + 1;
        }

        // Rebalance after every 100 operations
        $totalOps = array_sum($this->successCounts) + array_sum($this->failureCounts);
        if ($totalOps % 100 === 0) {
            $this->needsRebalance = true;
        }
    }
}