<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy;

use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategyInterface;

class AdaptivePriorityStrategy implements ConsumptionStrategyInterface
{
    private array $performanceScores = [];
    private array $recentLatencies = [];
    private array $lastUsed = [];
    private readonly int $windowSize;
    private readonly float $latencyWeight;
    private readonly float $successWeight;
    
    public function __construct(
        array $options = []
    ) {
        $this->windowSize = $options['window_size'] ?? 10;
        $this->latencyWeight = $options['latency_weight'] ?? 0.7;
        $this->successWeight = $options['success_weight'] ?? 0.3;
    }

    public function selectTransport(array $transports, CircuitBreakerInterface $circuitBreaker): ?string
    {
        $availableTransports = [];
        
        foreach (array_keys($transports) as $name) {
            if ($circuitBreaker->isAvailable($name)) {
                $availableTransports[] = $name;
            }
        }
        
        if (empty($availableTransports)) {
            return null;
        }
        
        // Sort by priority score
        usort($availableTransports, function ($a, $b) {
            return $this->getScore($b) <=> $this->getScore($a);
        });
        
        // Select based on priority but with some randomization to avoid starvation
        $topCount = min(3, count($availableTransports));
        $topTransports = array_slice($availableTransports, 0, $topCount);
        
        // Weighted random selection from top transports
        $selected = $this->weightedRandomSelect($topTransports);
        
        $this->lastUsed[$selected] = time();
        
        return $selected;
    }

    private function getScore(string $transportName): float
    {
        return $this->performanceScores[$transportName] ?? 50.0;
    }

    private function weightedRandomSelect(array $transports): string
    {
        if (count($transports) === 1) {
            return $transports[0];
        }

        $weights = [];
        $totalWeight = 0;

        foreach ($transports as $transport) {
            $weight = max(1, $this->getScore($transport));
            $weights[$transport] = $weight;
            $totalWeight += $weight;
        }

        $random = mt_rand(0, (int)$totalWeight - 1);
        $current = 0;

        foreach ($weights as $transport => $weight) {
            $current += $weight;
            if ($random < $current) {
                return $transport;
            }
        }

        return $transports[0]; // Fallback
    }

    public function recordResult(string $transportName, bool $success, float $latency): void
    {
        // Track recent latencies
        if (!isset($this->recentLatencies[$transportName])) {
            $this->recentLatencies[$transportName] = [];
        }

        $this->recentLatencies[$transportName][] = [
            'latency' => $latency,
            'success' => $success,
            'timestamp' => microtime(true)
        ];

        // Keep only recent entries
        if (count($this->recentLatencies[$transportName]) > $this->windowSize) {
            array_shift($this->recentLatencies[$transportName]);
        }

        // Update performance score
        $this->updatePerformanceScore($transportName);
    }

    private function updatePerformanceScore(string $transportName): void
    {
        if (!isset($this->recentLatencies[$transportName]) || empty($this->recentLatencies[$transportName])) {
            $this->performanceScores[$transportName] = 50.0;
            return;
        }

        $data = $this->recentLatencies[$transportName];

        // Calculate average latency and success rate
        $totalLatency = 0;
        $successCount = 0;

        foreach ($data as $entry) {
            $totalLatency += $entry['latency'];
            if ($entry['success']) {
                $successCount++;
            }
        }

        $avgLatency = $totalLatency / count($data);
        $successRate = $successCount / count($data);

        // Normalize latency score (lower is better)
        // Assume 100ms is good, 1000ms is bad
        $latencyScore = max(0, 100 - ($avgLatency / 10));

        // Success rate score (0-100)
        $successScore = $successRate * 100;

        // Combined score
        $score = ($this->latencyWeight * $latencyScore) + ($this->successWeight * $successScore);

        // Apply time decay to prevent old good scores from dominating
        $lastUsedTime = $this->lastUsed[$transportName] ?? time();
        $timeSinceLastUse = time() - $lastUsedTime;
        if ($timeSinceLastUse > 300) { // 5 minutes
            $decayFactor = max(0.5, 1 - ($timeSinceLastUse / 3600)); // Decay over 1 hour
            $score *= $decayFactor;
        }

        $this->performanceScores[$transportName] = $score;
    }
}
