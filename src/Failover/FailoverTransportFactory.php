<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Contracts\Service\ServiceProviderInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\RoundRobinStrategy;

/**
 * @implements TransportFactoryInterface<FailoverTransport>
 */
#[AutoconfigureTag('messenger.transport_factory')]
class FailoverTransportFactory implements TransportFactoryInterface
{
    public function __construct(
        private readonly ServiceProviderInterface $transportLocator
    ) {
    }

    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        // Parse DSN: failover://transport1,transport2,transport3
        $transportNames = $this->parseTransportNames($dsn);
        
        if (count($transportNames) < 2) {
            throw new \InvalidArgumentException('Failover transport requires at least 2 transport names');
        }
        
        // Load transports
        $transports = [];
        foreach ($transportNames as $transportName) {
            if (!$this->transportLocator->has($transportName)) {
                throw new \InvalidArgumentException(sprintf('Transport "%s" not found', $transportName));
            }
            $transports[$transportName] = $this->transportLocator->get($transportName);
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

    private function parseTransportNames(string $dsn): array
    {
        // Remove the scheme
        $transportList = substr($dsn, strlen('failover://'));

        if (empty($transportList)) {
            throw new \InvalidArgumentException('No transport names provided in DSN');
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