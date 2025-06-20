<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy;

use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategyInterface;

class LatencyAwareStrategy implements ConsumptionStrategyInterface
{
    private array $latencyStats = [];
    private readonly int $measurementInterval;
    private readonly float $latencyThreshold;
    
    public function __construct(
        array $options = []
    ) {
        $this->measurementInterval = $options['measurement_interval'] ?? 60; // seconds
        $this->latencyThreshold = $options['latency_threshold'] ?? 500; // milliseconds
    }

    public function selectTransport(array $transports, CircuitBreakerInterface $circuitBreaker): ?string
    {
        $candidates = [];
        
        foreach (array_keys($transports) as $name) {
            if (!$circuitBreaker->isAvailable($name)) {
                continue;
            }
            
            $latency = $this->getAverageLatency($name);
            $candidates[$name] = $latency;
        }
        
        if (empty($candidates)) {
            return null;
        }
        
        // Sort by latency (lowest first)
        asort($candidates);
        
        // Always pick the lowest latency transport
        return array_key_first($candidates);
    }

    private function getAverageLatency(string $transportName): float
    {
        if (!isset($this->latencyStats[$transportName])) {
            return 0.0; // Unknown transports get priority
        }

        return $this->latencyStats[$transportName]['ewma'];
    }

    public function recordResult(string $transportName, bool $success, float $latency): void
    {
        if (!$success) {
            // Record high latency for failures
            $latency = $this->latencyThreshold * 2;
        }

        if (!isset($this->latencyStats[$transportName])) {
            $this->latencyStats[$transportName] = [
                'measurements' => [],
                'ewma' => $latency, // Exponential weighted moving average
            ];
        }

        $stats = &$this->latencyStats[$transportName];

        // Add new measurement
        $stats['measurements'][] = [
            'latency' => $latency,
            'timestamp' => microtime(true)
        ];

        // Update EWMA
        $alpha = 0.2; // Smoothing factor
        $stats['ewma'] = ($alpha * $latency) + ((1 - $alpha) * $stats['ewma']);

        // Clean old measurements
        $this->cleanOldMeasurements($transportName);
    }

    private function cleanOldMeasurements(string $transportName): void
    {
        $stats = &$this->latencyStats[$transportName];
        $cutoff = microtime(true) - $this->measurementInterval;
        
        $stats['measurements'] = array_filter(
            $stats['measurements'],
            fn($m) => $m['timestamp'] > $cutoff
        );
    }
}
