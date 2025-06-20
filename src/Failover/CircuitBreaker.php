<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

class CircuitBreaker implements CircuitBreakerInterface
{
    private array $states = [];
    private array $failureCounts = [];
    private array $successCounts = [];
    private array $stateChangedTimes = [];

    public function __construct(
        private readonly int $failureThreshold = 5,
        private readonly int $successThreshold = 2,
        private readonly int $timeout = 30,
        private readonly float $timeoutMultiplier = 2.0,
        private readonly int $maxTimeout = 300
    ) {
    }

    public function isAvailable(string $transportName): bool
    {
        $state = $this->getState($transportName);
        
        return match ($state) {
            CircuitBreakerState::CLOSED => true,
            CircuitBreakerState::OPEN => $this->shouldAttemptReset($transportName),
            CircuitBreakerState::HALF_OPEN => true,
        };
    }

    public function getState(string $transportName): CircuitBreakerState
    {
        if (!isset($this->states[$transportName])) {
            $this->states[$transportName] = CircuitBreakerState::CLOSED;
        }

        $state = $this->states[$transportName];

        if ($state === CircuitBreakerState::OPEN && $this->shouldAttemptReset($transportName)) {
            $this->transitionTo($transportName, CircuitBreakerState::HALF_OPEN);
            return CircuitBreakerState::HALF_OPEN;
        }

        return $state;
    }

    private function shouldAttemptReset(string $transportName): bool
    {
        if (!isset($this->stateChangedTimes[$transportName])) {
            return false;
        }

        $timeout = $this->calculateTimeout($transportName);
        $elapsed = time() - $this->stateChangedTimes[$transportName];

        return $elapsed >= $timeout;
    }

    private function calculateTimeout(string $transportName): int
    {
        $failureCount = $this->failureCounts[$transportName] ?? 0;
        $multiplier = pow($this->timeoutMultiplier, min($failureCount / $this->failureThreshold, 5));

        return min((int)($this->timeout * $multiplier), $this->maxTimeout);
    }

    private function transitionTo(string $transportName, CircuitBreakerState $newState): void
    {
        $this->states[$transportName] = $newState;
        $this->stateChangedTimes[$transportName] = time();
    }

    public function recordSuccess(string $transportName): void
    {
        $state = $this->getState($transportName);

        if ($state === CircuitBreakerState::HALF_OPEN) {
            $this->successCounts[$transportName] = ($this->successCounts[$transportName] ?? 0) + 1;

            if ($this->successCounts[$transportName] >= $this->successThreshold) {
                $this->transitionTo($transportName, CircuitBreakerState::CLOSED);
                $this->resetCounters($transportName);
            }
        } else {
            $this->failureCounts[$transportName] = 0;
        }
    }

    private function resetCounters(string $transportName): void
    {
        $this->failureCounts[$transportName] = 0;
        $this->successCounts[$transportName] = 0;
    }

    public function recordFailure(string $transportName, \Throwable $exception): void
    {
        $state = $this->getState($transportName);

        if ($state === CircuitBreakerState::HALF_OPEN) {
            $this->transitionTo($transportName, CircuitBreakerState::OPEN);
            $this->resetCounters($transportName);
        } else {
            $this->failureCounts[$transportName] = ($this->failureCounts[$transportName] ?? 0) + 1;

            if ($this->failureCounts[$transportName] >= $this->failureThreshold) {
                $this->transitionTo($transportName, CircuitBreakerState::OPEN);
                $this->resetCounters($transportName);
            }
        }
    }
}
