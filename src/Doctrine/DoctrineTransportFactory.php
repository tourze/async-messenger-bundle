<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Tourze\AsyncMessengerBundle\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * @author Vincent Touzet <vincent.touzet@gmail.com>
 *
 * @implements TransportFactoryInterface<DoctrineTransport>
 */
#[AutoconfigureTag(name: 'messenger.transport_factory')]
readonly class DoctrineTransportFactory implements TransportFactoryInterface
{
    public function __construct(
        #[Autowire(service: 'doctrine.dbal.async_messenger_connection')] private DBALConnection $connection,
    ) {
    }

    /**
     * @param array<mixed, mixed> $options
     */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        unset($options['transport_name'], $options['use_notify']);

        /** @var array<string, mixed> $stringKeyedOptions */
        $stringKeyedOptions = $options;
        $configuration = Connection::buildConfiguration($dsn, $stringKeyedOptions);
        $connection = new Connection($configuration, $this->connection);

        return new DoctrineTransport($connection, $serializer);
    }

    /**
     * @param array<mixed> $options
     */
    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'async-doctrine://');
    }
}
