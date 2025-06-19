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

use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\DoctrineDedicatedConnectionBundle\Attribute\WithDedicatedConnection;

/**
 * @author Vincent Touzet <vincent.touzet@gmail.com>
 *
 * @implements TransportFactoryInterface<DoctrineTransport>
 */
#[AutoconfigureTag('messenger.transport_factory')]
#[WithDedicatedConnection('async_messenger')]
class DoctrineTransportFactory implements TransportFactoryInterface
{
    public function __construct(
        private readonly \Doctrine\DBAL\Connection $connection,
    ) {
    }

    /**
     * @param array $options You can set 'use_notify' to false to not use LISTEN/NOTIFY with postgresql
     */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        unset($options['transport_name'], $options['use_notify']);

        $configuration = Connection::buildConfiguration($dsn, $options);
        $connection = new Connection($configuration, $this->connection);

        return new DoctrineTransport($connection, $serializer);
    }

    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'async-doctrine://');
    }
}
