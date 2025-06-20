<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\RoundRobinStrategy;

/**
 * @implements TransportFactoryInterface<FailoverTransport>
 */
#[AutoconfigureTag('messenger.transport_factory')]
class FailoverTransportFactory implements TransportFactoryInterface
{
    /**
     * @param iterable<mixed, TransportFactoryInterface> $factories
     */
    public function __construct(
        private readonly iterable $factories
    ) {
    }

    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        // Parse DSN: failover://dsn1,dsn2,dsn3  OR  failover://transport1,transport2
        $transportDsns = $this->parseTransportDsns($dsn);
        
        if (count($transportDsns) < 2) {
            throw new \InvalidArgumentException('Failover transport requires at least 2 transport DSNs');
        }
        
        // Create transports from DSNs
        $transports = [];

        foreach ($transportDsns as $index => $transportDsn) {
            $transportName = 'transport_' . $index;

            // If DSN doesn't contain "://", assume it's a transport name and construct DSN
            if (!str_contains($transportDsn, '://')) {
                // Map common transport names to their DSNs
                $transportDsn = match($transportDsn) {
                    'async_doctrine' => 'async-doctrine://',
                    'async_redis' => 'async-redis://',
                    default => throw new \InvalidArgumentException(sprintf('Unknown transport name: %s', $transportDsn))
                };
            }

            // Find the appropriate factory for this DSN
            $transport = null;
            foreach ($this->factories as $factory) {
                // Skip self to avoid infinite recursion
                if ($factory === $this) {
                    continue;
                }

                if ($factory->supports($transportDsn, [])) {
                    $transport = $factory->createTransport($transportDsn, [], $serializer);
                    break;
                }
            }

            if ($transport === null) {
                throw new \InvalidArgumentException(sprintf('No factory found for DSN: %s', $transportDsn));
            }

            $transports[$transportName] = $transport;
        }
        
        // Create circuit breaker
        $circuitBreakerOptions = $options['circuit_breaker'] ?? [];
        $circuitBreaker = new CircuitBreaker(
            $circuitBreakerOptions['failure_threshold'] ?? 5,
            $circuitBreakerOptions['success_threshold'] ?? 2,
            $circuitBreakerOptions['timeout'] ?? 30,
            $circuitBreakerOptions['timeout_multiplier'] ?? 2.0,
            $circuitBreakerOptions['max_timeout'] ?? 300
        );
        
        // Create consumption strategy
        $strategyName = $options['consumption_strategy'] ?? 'round_robin';
        $consumptionStrategy = $this->createConsumptionStrategy($strategyName, $options);
        
        return new FailoverTransport(
            $transports,
            $circuitBreaker,
            $consumptionStrategy,
            $options
        );
    }

    private function parseTransportDsns(string $dsn): array
    {
        // Remove the scheme
        $transportList = substr($dsn, strlen('failover://'));

        if (empty($transportList)) {
            throw new \InvalidArgumentException('No transport DSNs provided in DSN');
        }

        return array_map('trim', explode(',', $transportList));
    }
    
    private function createConsumptionStrategy(string $name, array $options): ConsumptionStrategyInterface
    {
        return match ($name) {
            'round_robin' => new RoundRobinStrategy(),
            'weighted_round_robin' => new ConsumptionStrategy\WeightedRoundRobinStrategy(),
            'adaptive_priority' => new ConsumptionStrategy\AdaptivePriorityStrategy($options['adaptive_priority'] ?? []),
            'latency_aware' => new ConsumptionStrategy\LatencyAwareStrategy($options['latency_aware'] ?? []),
            default => throw new \InvalidArgumentException(sprintf('Unknown consumption strategy: %s', $name))
        };
    }
    
    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'failover://');
    }
}
