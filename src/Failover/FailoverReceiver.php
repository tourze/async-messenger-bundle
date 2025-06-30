<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Exception\TransportException;
use Tourze\AsyncMessengerBundle\Failover\Stamp\FailoverSourceStamp;

class FailoverReceiver implements ReceiverInterface, ListableReceiverInterface
{
    private array $lastConsumedTransport = [];

    /**
     * @param TransportInterface[] $transports
     */
    public function __construct(
        private readonly array $transports,
        private readonly CircuitBreakerInterface $circuitBreaker,
        private readonly ConsumptionStrategyInterface $consumptionStrategy,
        private readonly array $options = []
    ) {
    }

    public function get(): iterable
    {
        $maxRetries = $this->options['max_retries'] ?? 3;
        $retryDelay = $this->options['retry_delay'] ?? 100; // milliseconds
        
        while (true) {
            $transportName = $this->consumptionStrategy->selectTransport($this->transports, $this->circuitBreaker);
            
            if ($transportName === null) {
                // All transports are unavailable, wait before retrying
                usleep($retryDelay * 1000);
                continue;
            }
            
            $transport = $this->transports[$transportName];
            $startTime = microtime(true);
            
            try {
                $envelopes = $transport->get();
                
                foreach ($envelopes as $envelope) {
                    // Add stamp to track source transport
                    $envelope = $envelope->with(new FailoverSourceStamp($transportName));
                    $this->lastConsumedTransport[spl_object_id($envelope)] = $transportName;
                    
                    yield $envelope;
                }
                
                $latency = (microtime(true) - $startTime) * 1000;
                $this->circuitBreaker->recordSuccess($transportName);
                $this->consumptionStrategy->recordResult($transportName, true, $latency);
                
                // Successfully consumed from this transport, continue with next iteration
                return;
                
            } catch (\Throwable $e) {
                $latency = (microtime(true) - $startTime) * 1000;
                $this->circuitBreaker->recordFailure($transportName, $e);
                $this->consumptionStrategy->recordResult($transportName, false, $latency);
                
                // Log the failure if logger is available
                if (isset($this->options['logger'])) {
                    $this->options['logger']->error('Failed to consume from transport', [
                        'transport' => $transportName,
                        'exception' => $e,
                    ]);
                }
                
                // Try next available transport
                continue;
            }
        }
    }

    public function ack(Envelope $envelope): void
    {
        $transportName = $this->getSourceTransport($envelope);
        
        if ($transportName === null || !isset($this->transports[$transportName])) {
            throw new TransportException('Cannot determine source transport for envelope');
        }
        
        try {
            $this->transports[$transportName]->ack($envelope);
            $this->circuitBreaker->recordSuccess($transportName);
        } catch (\Throwable $e) {
            $this->circuitBreaker->recordFailure($transportName, $e);
            throw $e;
        }
    }

    private function getSourceTransport(Envelope $envelope): ?string
    {
        // First check our stamp
        $stamp = $envelope->last(FailoverSourceStamp::class);
        if ($stamp instanceof FailoverSourceStamp) {
            return $stamp->getTransportName();
        }

        // Fallback to our internal tracking
        $objectId = spl_object_id($envelope);
        return $this->lastConsumedTransport[$objectId] ?? null;
    }

    public function reject(Envelope $envelope): void
    {
        $transportName = $this->getSourceTransport($envelope);

        if ($transportName === null || !isset($this->transports[$transportName])) {
            throw new TransportException('Cannot determine source transport for envelope');
        }

        try {
            $this->transports[$transportName]->reject($envelope);
            $this->circuitBreaker->recordSuccess($transportName);
        } catch (\Throwable $e) {
            $this->circuitBreaker->recordFailure($transportName, $e);
            throw $e;
        }
    }

    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        $transportName = $this->getSourceTransport($envelope);

        if ($transportName === null || !isset($this->transports[$transportName])) {
            return; // Silently ignore if we can't determine source
        }

        $transport = $this->transports[$transportName];

        // Check if transport supports keepalive
        if (!method_exists($transport, 'keepalive')) {
            return;
        }

        try {
            $transport->keepalive($envelope, $seconds);
        } catch (\Throwable $e) {
            // Don't record keepalive failures as circuit breaker failures
            // as they're not critical
        }
    }

    public function all(?int $limit = null): iterable
    {
        $count = 0;

        foreach ($this->transports as $transportName => $transport) {
            if (!$this->circuitBreaker->isAvailable($transportName)) {
                continue;
            }

            if (!$transport instanceof ListableReceiverInterface) {
                continue;
            }

            try {
                $remaining = $limit !== null ? $limit - $count : null;

                foreach ($transport->all($remaining) as $envelope) {
                    yield $envelope->with(new FailoverSourceStamp($transportName));
                    $count++;

                    if ($limit !== null && $count >= $limit) {
                        return;
                    }
                }
            } catch (\Throwable $e) {
                $this->circuitBreaker->recordFailure($transportName, $e);
            }
        }
    }

    public function find(mixed $id): ?Envelope
    {
        foreach ($this->transports as $transportName => $transport) {
            if (!$this->circuitBreaker->isAvailable($transportName)) {
                continue;
            }

            if (!$transport instanceof ListableReceiverInterface) {
                continue;
            }

            try {
                $envelope = $transport->find($id);
                if ($envelope !== null) {
                    return $envelope->with(new FailoverSourceStamp($transportName));
                }
            } catch (\Throwable $e) {
                $this->circuitBreaker->recordFailure($transportName, $e);
            }
        }

        return null;
    }
}