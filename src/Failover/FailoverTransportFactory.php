<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use Symfony\Component\DependencyInjection\Attribute\AutowireIterator;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Exception\InvalidConfigurationException;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\RoundRobinStrategy;

/**
 * @implements TransportFactoryInterface<FailoverTransport>
 */
#[AutoconfigureTag(name: 'messenger.transport_factory')]
class FailoverTransportFactory implements TransportFactoryInterface
{
    /**
     * @param iterable<mixed, TransportFactoryInterface<TransportInterface>> $factories
     */
    public function __construct(
        #[AutowireIterator(tag: 'messenger.transport_factory')]
        private readonly iterable $factories,
    ) {
    }

    /**
     * @param array<mixed> $options
     */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        /** @var array<string, mixed> $options */
        $transportDsns = $this->parseTransportDsns($dsn);

        if (count($transportDsns) < 2) {
            throw new InvalidConfigurationException('Failover transport requires at least 2 transport DSNs');
        }

        $transports = $this->createTransports($transportDsns, $serializer);
        $circuitBreaker = $this->createCircuitBreaker($options);
        $strategyName = $options['consumption_strategy'] ?? 'round_robin';
        $consumptionStrategy = $this->createConsumptionStrategy(is_string($strategyName) ? $strategyName : 'round_robin', $options);

        return new FailoverTransport($transports, $circuitBreaker, $consumptionStrategy, $options);
    }

    /**
     * @return array<int, string>
     */
    private function parseTransportDsns(string $dsn): array
    {
        // Remove the scheme
        $transportList = substr($dsn, strlen('failover://'));

        if ('' === $transportList) {
            throw new InvalidConfigurationException('No transport DSNs provided in DSN');
        }

        return array_map('trim', explode(',', $transportList));
    }

    /**
     * @param array<int, string> $transportDsns
     * @return array<string, TransportInterface>
     */
    private function createTransports(array $transportDsns, SerializerInterface $serializer): array
    {
        $transports = [];

        foreach ($transportDsns as $index => $transportDsn) {
            $transportName = 'transport_' . $index;
            $transportDsn = $this->normalizeTransportDsn($transportDsn);
            $transport = $this->findAndCreateTransport($transportDsn, $serializer);
            $transports[$transportName] = $transport;
        }

        return $transports;
    }

    private function normalizeTransportDsn(string $transportDsn): string
    {
        if (str_contains($transportDsn, '://')) {
            return $transportDsn;
        }

        return match ($transportDsn) {
            'async_doctrine' => 'async-doctrine://',
            'async_redis' => 'async-redis://',
            default => throw new InvalidConfigurationException(sprintf('Unknown transport name: %s', $transportDsn)),
        };
    }

    private function findAndCreateTransport(string $transportDsn, SerializerInterface $serializer): TransportInterface
    {
        foreach ($this->factories as $factory) {
            if ($factory === $this) {
                continue;
            }

            if ($factory->supports($transportDsn, [])) {
                return $factory->createTransport($transportDsn, [], $serializer);
            }
        }

        throw new InvalidConfigurationException(sprintf('No factory found for DSN: %s', $transportDsn));
    }

    /**
     * @param array<string, mixed> $options
     */
    private function createCircuitBreaker(array $options): CircuitBreakerInterface
    {
        $circuitBreakerOptions = $options['circuit_breaker'] ?? [];
        if (!is_array($circuitBreakerOptions)) {
            $circuitBreakerOptions = [];
        }

        $failureThreshold = $circuitBreakerOptions['failure_threshold'] ?? 5;
        $successThreshold = $circuitBreakerOptions['success_threshold'] ?? 2;
        $timeout = $circuitBreakerOptions['timeout'] ?? 30;
        $timeoutMultiplier = $circuitBreakerOptions['timeout_multiplier'] ?? 2.0;
        $maxTimeout = $circuitBreakerOptions['max_timeout'] ?? 300;

        return new CircuitBreaker(
            is_numeric($failureThreshold) ? (int) $failureThreshold : 5,
            is_numeric($successThreshold) ? (int) $successThreshold : 2,
            is_numeric($timeout) ? (int) $timeout : 30,
            is_numeric($timeoutMultiplier) ? (float) $timeoutMultiplier : 2.0,
            is_numeric($maxTimeout) ? (int) $maxTimeout : 300
        );
    }

    /**
     * @param array<string, mixed> $options
     */
    private function createConsumptionStrategy(string $name, array $options): ConsumptionStrategyInterface
    {
        return match ($name) {
            'round_robin' => new RoundRobinStrategy(),
            'weighted_round_robin' => new ConsumptionStrategy\WeightedRoundRobinStrategy(),
            'adaptive_priority' => new ConsumptionStrategy\AdaptivePriorityStrategy($this->ensureStringMixedArray($options['adaptive_priority'] ?? [])),
            'latency_aware' => new ConsumptionStrategy\LatencyAwareStrategy($this->ensureStringMixedArray($options['latency_aware'] ?? [])),
            default => throw new InvalidConfigurationException(sprintf('Unknown consumption strategy: %s', $name)),
        };
    }

    /**
     * @param mixed $value
     * @return array<string, mixed>
     */
    private function ensureStringMixedArray($value): array
    {
        if (is_array($value)) {
            // 确保所有键都是字符串类型
            $result = [];
            foreach ($value as $key => $val) {
                $result[(string) $key] = $val;
            }

            return $result;
        }

        return [];
    }

    /**
     * @param array<mixed> $options
     */
    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'failover://');
    }
}
