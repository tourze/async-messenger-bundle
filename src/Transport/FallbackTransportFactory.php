<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Transport;

use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Factory for creating FallbackTransport instances
 * 
 * @implements TransportFactoryInterface<FallbackTransport>
 */
#[AutoconfigureTag('messenger.transport_factory')]
final class FallbackTransportFactory implements TransportFactoryInterface
{
    public function __construct(
        private readonly ContainerInterface $container,
        private readonly ?LoggerInterface $logger = null
    ) {
    }

    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        // Validate DSN format
        if (!str_starts_with($dsn, 'fallback://')) {
            throw new \InvalidArgumentException('Invalid DSN for fallback transport');
        }
        
        // Get transport names from options or use defaults
        $transportNames = $options['transports'] ?? ['async_redis', 'async_doctrine', 'sync'];
        
        $transports = [];
        foreach ($transportNames as $transportName) {
            $serviceId = 'messenger.transport.' . $transportName;
            
            if (!$this->container->has($serviceId)) {
                throw new \InvalidArgumentException(sprintf('Transport "%s" not found in container', $transportName));
            }
            
            $transports[$transportName] = $this->container->get($serviceId);
        }
        
        return new FallbackTransport($transports, $this->logger);
    }

    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'fallback://');
    }
}