<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Tourze\AsyncMessengerBundle\Redis;

use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * @author Alexander Schranz <alexander@suluio>
 * @author Antoine Bluchet <soyuka@gmail.com>
 *
 * @implements TransportFactoryInterface<RedisTransport>
 */
#[AutoconfigureTag('messenger.transport_factory')]
class RedisTransportFactory implements TransportFactoryInterface
{
    public function __construct(
        #[Autowire(service: 'snc_redis.default')] private readonly \Redis $redis,
    )
    {
    }

    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        unset($options['transport_name']);

        return new RedisTransport(new Connection($this->redis, $options), $serializer);
    }

    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'async-redis://');
    }
}
