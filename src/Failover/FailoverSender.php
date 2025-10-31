<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Exception\FailoverException;

class FailoverSender implements SenderInterface
{
    /**
     * @param array<string, TransportInterface> $transports
     * @param array<string, mixed> $options
     */
    public function __construct(
        private readonly array $transports,
        private readonly CircuitBreakerInterface $circuitBreaker,
        private readonly array $options = [],
    ) {
    }

    public function send(Envelope $envelope): Envelope
    {
        $lastException = null;
        $attemptedTransports = [];

        // First, try to send to healthy transports in order
        $result = $this->tryHealthyTransports($envelope, $lastException, $attemptedTransports);
        if (null !== $result['envelope']) {
            return $result['envelope'];
        }
        $lastException = $result['lastException'];
        $attemptedTransports = $result['attemptedTransports'];

        // If all healthy transports failed, try unhealthy ones as last resort
        $result = $this->tryUnhealthyTransportsIfEnabled($envelope, $lastException, $attemptedTransports);
        if (null !== $result['envelope']) {
            return $result['envelope'];
        }
        $lastException = $result['lastException'];

        // All transports failed
        $this->handleAllTransportsFailed($lastException);
    }

    /**
     * @param array<string> $attemptedTransports
     * @return array{envelope: ?Envelope, lastException: ?\Throwable, attemptedTransports: array<string>}
     */
    private function tryHealthyTransports(Envelope $envelope, ?\Throwable $lastException, array $attemptedTransports): array
    {
        foreach ($this->transports as $transportName => $transport) {
            if (!$this->circuitBreaker->isAvailable($transportName)) {
                continue;
            }

            $attemptedTransports[] = $transportName;
            $result = $this->attemptSend($envelope, $transportName, $transport, $lastException);

            if (null !== $result['envelope']) {
                return [
                    'envelope' => $result['envelope'],
                    'lastException' => $result['lastException'],
                    'attemptedTransports' => $attemptedTransports,
                ];
            }
            $lastException = $result['lastException'];
        }

        return [
            'envelope' => null,
            'lastException' => $lastException,
            'attemptedTransports' => $attemptedTransports,
        ];
    }

    /**
     * @param array<string> $attemptedTransports
     * @return array{envelope: ?Envelope, lastException: ?\Throwable}
     */
    private function tryUnhealthyTransportsIfEnabled(Envelope $envelope, ?\Throwable $lastException, array $attemptedTransports): array
    {
        $tryUnhealthy = $this->options['try_unhealthy_on_failure'] ?? true;
        if (!is_bool($tryUnhealthy) || !$tryUnhealthy) {
            return ['envelope' => null, 'lastException' => $lastException];
        }

        foreach ($this->transports as $transportName => $transport) {
            if (in_array($transportName, $attemptedTransports, true)) {
                continue;
            }

            $result = $this->attemptSendUnhealthy($envelope, $transportName, $transport, $lastException);

            if (null !== $result['envelope']) {
                return $result;
            }
            $lastException = $result['lastException'];
        }

        return ['envelope' => null, 'lastException' => $lastException];
    }

    /**
     * @return array{envelope: ?Envelope, lastException: ?\Throwable}
     */
    private function attemptSend(Envelope $envelope, string $transportName, TransportInterface $transport, ?\Throwable $lastException): array
    {
        try {
            $startTime = microtime(true);
            $sentEnvelope = $transport->send($envelope);
            $latency = (microtime(true) - $startTime) * 1000;

            $this->circuitBreaker->recordSuccess($transportName);
            $this->logSuccessfulSend($transportName, $latency);

            return [
                'envelope' => $sentEnvelope->with(new Stamp\FailoverSourceStamp($transportName)),
                'lastException' => $lastException,
            ];
        } catch (\Throwable $e) {
            $this->circuitBreaker->recordFailure($transportName, $e);
            $this->logFailure($transportName, $e);

            return [
                'envelope' => null,
                'lastException' => $e,
            ];
        }
    }

    /**
     * @return array{envelope: ?Envelope, lastException: ?\Throwable}
     */
    private function attemptSendUnhealthy(Envelope $envelope, string $transportName, TransportInterface $transport, ?\Throwable $lastException): array
    {
        try {
            $sentEnvelope = $transport->send($envelope);
            $this->circuitBreaker->recordSuccess($transportName);
            $this->logUnhealthyRecovery($transportName);

            return [
                'envelope' => $sentEnvelope->with(new Stamp\FailoverSourceStamp($transportName)),
                'lastException' => $lastException,
            ];
        } catch (\Throwable $e) {
            $this->circuitBreaker->recordFailure($transportName, $e);

            return [
                'envelope' => null,
                'lastException' => $e,
            ];
        }
    }

    private function logSuccessfulSend(string $transportName, float $latency): void
    {
        $logger = $this->options['logger'] ?? null;
        if ($logger instanceof LoggerInterface) {
            $logger->debug('Message sent successfully', [
                'transport' => $transportName,
                'latency_ms' => $latency,
            ]);
        }
    }

    private function logFailure(string $transportName, \Throwable $e): void
    {
        $logger = $this->options['logger'] ?? null;
        if ($logger instanceof LoggerInterface) {
            $logger->warning('Failed to send message to transport', [
                'transport' => $transportName,
                'exception' => $e,
            ]);
        }
    }

    private function logUnhealthyRecovery(string $transportName): void
    {
        $logger = $this->options['logger'] ?? null;
        if ($logger instanceof LoggerInterface) {
            $logger->info('Message sent to previously unhealthy transport', [
                'transport' => $transportName,
            ]);
        }
    }

    private function handleAllTransportsFailed(?\Throwable $lastException): never
    {
        $errorMessage = sprintf(
            'All transports failed. Attempted: %s',
            implode(', ', array_keys($this->transports))
        );

        $logger = $this->options['logger'] ?? null;
        if ($logger instanceof LoggerInterface) {
            $logger->error($errorMessage, [
                'last_exception' => $lastException,
            ]);
        }

        throw new FailoverException($errorMessage, 0, $lastException);
    }
}
