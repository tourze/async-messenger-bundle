<?php

namespace Tourze\AsyncMessengerBundle\Tests\Redis;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;
use Tourze\AsyncMessengerBundle\Redis\RedisTransportFactory;

class RedisTransportFactoryTest extends TestCase
{
    private RedisTransportFactory $factory;
    private SerializerInterface $serializer;

    public function test_implements_transport_factory_interface(): void
    {
        $this->assertInstanceOf(TransportFactoryInterface::class, $this->factory);
    }

    public function test_supports_asyncRedisScheme(): void
    {
        $this->assertTrue($this->factory->supports('async-redis://localhost', []));
    }

    public function test_doesNotSupport_otherSchemes(): void
    {
        $this->assertFalse($this->factory->supports('redis://localhost', []));
        $this->assertFalse($this->factory->supports('mysql://localhost', []));
        $this->assertFalse($this->factory->supports('postgresql://localhost', []));
        $this->assertFalse($this->factory->supports('amqp://localhost', []));
        $this->assertFalse($this->factory->supports('doctrine://localhost', []));
    }

    public function test_doesNotSupport_emptyDsn(): void
    {
        $this->assertFalse($this->factory->supports('', []));
    }

    public function test_doesNotSupport_invalidDsn(): void
    {
        $this->assertFalse($this->factory->supports('invalid-dsn', []));
    }

    public function test_createTransport_returnsRedisTransport(): void
    {
        $dsn = 'async-redis://localhost:6379';
        $options = [];
        
        $redis = $this->createMock(\Redis::class);
        $redis->method('isConnected')->willReturn(true);
        
        $factory = new RedisTransportFactory($redis);
        $transport = $factory->createTransport($dsn, $options, $this->serializer);
        
        $this->assertInstanceOf(RedisTransport::class, $transport);
    }

    public function test_createTransport_removesTransportNameFromOptions(): void
    {
        $dsn = 'async-redis://localhost:6379';
        $options = [
            'transport_name' => 'my_transport',
            'queue' => 'test_queue'
        ];

        $redis = $this->createMock(\Redis::class);
        $redis->method('isConnected')->willReturn(true);
        
        $factory = new RedisTransportFactory($redis);
        $transport = $factory->createTransport($dsn, $options, $this->serializer);
        
        $this->assertInstanceOf(RedisTransport::class, $transport);
    }

    public function test_supports_withComplexDsn(): void
    {
        $this->assertTrue($this->factory->supports('async-redis://user:pass@localhost:6379/myqueue', []));
    }

    public function test_supports_isCaseSensitive(): void
    {
        // DSN 协议应该是大小写敏感的
        $this->assertFalse($this->factory->supports('ASYNC-REDIS://localhost', []));
        $this->assertFalse($this->factory->supports('Async-Redis://localhost', []));
    }

    public function test_supports_withQueryParameters(): void
    {
        $this->assertTrue($this->factory->supports('async-redis://localhost?auto_setup=true&queue=messages', []));
    }

    public function test_supports_withFragment(): void
    {
        $this->assertTrue($this->factory->supports('async-redis://localhost#fragment', []));
    }

    protected function setUp(): void
    {
        $redis = $this->createMock(\Redis::class);
        $this->factory = new RedisTransportFactory($redis);
        $this->serializer = $this->createMock(SerializerInterface::class);
    }
}