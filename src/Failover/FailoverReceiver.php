<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Exception\TransportException;
use Tourze\AsyncMessengerBundle\Failover\Stamp\FailoverSourceStamp;

class FailoverReceiver implements ReceiverInterface, ListableReceiverInterface
{
    /** @var array<int, string> */
    private array $lastConsumedTransport = [];

    /**
     * @param array<string, TransportInterface> $transports
     * @param array<string, mixed> $options
     */
    public function __construct(
        private readonly array $transports,
        private readonly CircuitBreakerInterface $circuitBreaker,
        private readonly ConsumptionStrategyInterface $consumptionStrategy,
        private readonly array $options = [],
    ) {
    }

    public function get(): iterable
    {
        $maxRetriesValue = $this->options['max_retries'] ?? 3;
        $retryDelayValue = $this->options['retry_delay'] ?? 100;
        $maxRetries = is_numeric($maxRetriesValue) ? (int) $maxRetriesValue : 3;
        $retryDelay = is_numeric($retryDelayValue) ? (int) $retryDelayValue : 100; // milliseconds
        $retryCount = 0;

        while (true) {
            $transportName = $this->consumptionStrategy->selectTransport($this->transports, $this->circuitBreaker);

            if (null === $transportName) {
                if (!$this->handleNoTransportAvailable($retryCount, $maxRetries, $retryDelay)) {
                    return;
                }
                ++$retryCount;
                continue;
            }

            $retryCount = 0;
            $result = $this->processTransport($transportName);
            if (null !== $result) {
                yield from $result;

                return;
            }
        }
    }

    private function handleNoTransportAvailable(int $retryCount, int $maxRetries, int $retryDelay): bool
    {
        if ($retryCount >= $maxRetries) {
            return false;
        }

        usleep($retryDelay * 1000);

        return true;
    }

    /**
     * @return array<int, Envelope>|null
     */
    private function processTransport(string $transportName): ?array
    {
        $transport = $this->transports[$transportName];
        $startTime = microtime(true);

        try {
            $envelopes = $transport->get();
            $processedEnvelopes = [];

            foreach ($envelopes as $envelope) {
                $envelope = $envelope->with(new FailoverSourceStamp($transportName));
                $this->lastConsumedTransport[spl_object_id($envelope)] = $transportName;
                $processedEnvelopes[] = $envelope;
            }

            $this->recordTransportSuccess($transportName, $startTime);

            return $processedEnvelopes;
        } catch (\Throwable $e) {
            $this->recordTransportFailure($transportName, $startTime, $e);

            return null;
        }
    }

    private function recordTransportSuccess(string $transportName, float $startTime): void
    {
        $latency = (microtime(true) - $startTime) * 1000;
        $this->circuitBreaker->recordSuccess($transportName);
        $this->consumptionStrategy->recordResult($transportName, true, $latency);
    }

    private function recordTransportFailure(string $transportName, float $startTime, \Throwable $e): void
    {
        $latency = (microtime(true) - $startTime) * 1000;
        $this->circuitBreaker->recordFailure($transportName, $e);
        $this->consumptionStrategy->recordResult($transportName, false, $latency);

        $logger = $this->options['logger'] ?? null;
        if ($logger instanceof LoggerInterface) {
            $logger->error('Failed to consume from transport', [
                'transport' => $transportName,
                'exception' => $e,
            ]);
        }
    }

    public function ack(Envelope $envelope): void
    {
        $transportName = $this->getSourceTransport($envelope);

        if (null === $transportName || !isset($this->transports[$transportName])) {
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

        if (null === $transportName || !isset($this->transports[$transportName])) {
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

        if (null === $transportName || !isset($this->transports[$transportName])) {
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
            if (!$this->shouldProcessTransport($transportName, $transport)) {
                continue;
            }

            try {
                $envelopesProcessed = 0;
                foreach ($this->processTransportEnvelopes($transportName, $transport, $limit, $count) as $envelope) {
                    if ($envelope instanceof Envelope) {
                        yield $envelope;
                    }
                    ++$count;
                    ++$envelopesProcessed;

                    if ($this->hasReachedLimit($limit, $count)) {
                        return;
                    }
                }
            } catch (\Throwable $e) {
                $this->circuitBreaker->recordFailure($transportName, $e);
            }
        }
    }

    private function shouldProcessTransport(string $transportName, object $transport): bool
    {
        return $this->circuitBreaker->isAvailable($transportName);
    }

    private function calculateRemaining(?int $limit, int $count): ?int
    {
        return null !== $limit ? $limit - $count : null;
    }

    private function hasReachedLimit(?int $limit, int $count): bool
    {
        return null !== $limit && $count >= $limit;
    }

    private function processTransportEnvelopes(string $transportName, object $transport, ?int $limit, int $count): \Generator
    {
        $remaining = $this->calculateRemaining($limit, $count);

        if (!$transport instanceof ListableReceiverInterface) {
            yield from [];

            return;
        }

        foreach ($transport->all($remaining) as $envelope) {
            yield $envelope->with(new FailoverSourceStamp($transportName));

            if ($this->hasReachedLimit($limit, $count + 1)) {
                break;
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
                if (null !== $envelope) {
                    return $envelope->with(new FailoverSourceStamp($transportName));
                }
            } catch (\Throwable $e) {
                $this->circuitBreaker->recordFailure($transportName, $e);
            }
        }

        return null;
    }
}
