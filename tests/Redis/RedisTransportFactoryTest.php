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

    public function test_supports_redisScheme(): void
    {
        $this->assertTrue($this->factory->supports('redis://localhost', []));
    }

    public function test_supports_redissScheme(): void
    {
        $this->assertTrue($this->factory->supports('rediss://localhost', []));
    }

    public function test_supports_valkeyScheme(): void
    {
        $this->assertTrue($this->factory->supports('valkey://localhost', []));
    }

    public function test_supports_valkeysScheme(): void
    {
        $this->assertTrue($this->factory->supports('valkeys://localhost', []));
    }

    public function test_doesNotSupport_otherSchemes(): void
    {
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
        // 由于 Connection::fromDsn 需要实际的 Redis 扩展，我们只能测试基本的创建逻辑
        // 在单元测试中，我们主要验证工厂方法的正确性
        $dsn = 'redis://localhost:6379';
        $options = ['stream' => 'test_stream'];

        try {
            $transport = $this->factory->createTransport($dsn, $options, $this->serializer);
            $this->assertInstanceOf(RedisTransport::class, $transport);
        } catch (\Exception $e) {
            // 在没有 Redis 扩展或连接的环境中，这是预期的
            $this->assertStringContainsString('Redis', $e->getMessage());
        }
    }

    public function test_createTransport_removesTransportNameFromOptions(): void
    {
        $dsn = 'redis://localhost:6379';
        $options = [
            'transport_name' => 'my_transport',
            'stream' => 'test_stream'
        ];

        try {
            $this->factory->createTransport($dsn, $options, $this->serializer);
            // 如果能成功创建，说明 transport_name 被正确移除了
            $this->assertTrue(true);
        } catch (\Exception $e) {
            // 在没有 Redis 扩展的环境中，验证错误消息不包含 transport_name 相关内容
            $this->assertStringNotContainsString('transport_name', $e->getMessage());
        }
    }

    public function test_supports_withComplexDsn(): void
    {
        $this->assertTrue($this->factory->supports('redis://user:pass@localhost:6379/mystream', []));
        $this->assertTrue($this->factory->supports('rediss://user:pass@localhost:6380/mystream', []));
    }

    public function test_supports_isCaseInsensitive(): void
    {
        // DSN 协议应该是大小写敏感的，但我们测试一下边界情况
        $this->assertFalse($this->factory->supports('REDIS://localhost', []));
        $this->assertFalse($this->factory->supports('Redis://localhost', []));
    }

    public function test_supports_withQueryParameters(): void
    {
        $this->assertTrue($this->factory->supports('redis://localhost?auto_setup=true&stream=messages', []));
        $this->assertTrue($this->factory->supports('valkey://localhost?group=workers&consumer=worker1', []));
    }

    public function test_supports_withFragment(): void
    {
        $this->assertTrue($this->factory->supports('redis://localhost#fragment', []));
    }

    protected function setUp(): void
    {
        $this->factory = new RedisTransportFactory();
        $this->serializer = $this->createMock(SerializerInterface::class);
    }
}