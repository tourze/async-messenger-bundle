<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy;

use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategyInterface;

class LatencyAwareStrategy implements ConsumptionStrategyInterface
{
    /** @var array<string, array<string, mixed>> */
    private array $latencyStats = [];

    private readonly int $measurementInterval;

    private readonly float $latencyThreshold;

    /**
     * @param array<string, mixed> $options
     */
    public function __construct(
        array $options = [],
    ) {
        $measurementIntervalValue = $options['measurement_interval'] ?? 60;
        $this->measurementInterval = is_numeric($measurementIntervalValue) ? (int) $measurementIntervalValue : 60;
        $latencyThresholdValue = $options['latency_threshold'] ?? 500;
        $this->latencyThreshold = is_numeric($latencyThresholdValue) ? (float) $latencyThresholdValue : 500.0;
    }

    /**
     * @param array<string, mixed> $transports
     */
    public function selectTransport(array $transports, CircuitBreakerInterface $circuitBreaker): ?string
    {
        /** @var array<string, float> $candidates */
        $candidates = [];

        foreach (array_keys($transports) as $name) {
            if (!$circuitBreaker->isAvailable($name)) {
                continue;
            }

            $latency = $this->getAverageLatency($name);
            $candidates[$name] = $latency;
        }

        if (0 === count($candidates)) {
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

        $ewmaValue = $this->latencyStats[$transportName]['ewma'] ?? 0.0;

        return is_numeric($ewmaValue) ? (float) $ewmaValue : 0.0;
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

        // Add new measurement
        if (!isset($this->latencyStats[$transportName]['measurements']) || !is_array($this->latencyStats[$transportName]['measurements'])) {
            $this->latencyStats[$transportName]['measurements'] = [];
        }
        $this->latencyStats[$transportName]['measurements'][] = [
            'latency' => $latency,
            'timestamp' => microtime(true),
        ];

        // Update EWMA
        $alpha = 0.2; // Smoothing factor
        $currentEwmaValue = $this->latencyStats[$transportName]['ewma'] ?? 0.0;
        $currentEwma = is_numeric($currentEwmaValue) ? (float) $currentEwmaValue : 0.0;
        $this->latencyStats[$transportName]['ewma'] = ($alpha * $latency) + ((1 - $alpha) * $currentEwma);

        // Clean old measurements
        $this->cleanOldMeasurements($transportName);
    }

    private function cleanOldMeasurements(string $transportName): void
    {
        $cutoff = microtime(true) - $this->measurementInterval;

        $measurementsValue = $this->latencyStats[$transportName]['measurements'] ?? [];
        $measurements = is_array($measurementsValue) ? $measurementsValue : [];
        $this->latencyStats[$transportName]['measurements'] = array_filter(
            $measurements,
            fn ($m) => is_array($m) && isset($m['timestamp']) && is_numeric($m['timestamp']) && $m['timestamp'] > $cutoff
        );
    }
}
