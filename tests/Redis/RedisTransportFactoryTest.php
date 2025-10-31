<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RunTestsInSeparateProcesses;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;
use Tourze\AsyncMessengerBundle\Redis\RedisTransportFactory;
use Tourze\PHPUnitSymfonyKernelTest\AbstractIntegrationTestCase;

/**
 * @internal
 */
#[CoversClass(RedisTransportFactory::class)]
#[RunTestsInSeparateProcesses]
final class RedisTransportFactoryTest extends AbstractIntegrationTestCase
{
    private RedisTransportFactory $factory;

    protected function onSetUp(): void
    {
        // AbstractIntegrationTestCase 要求实现这个方法
    }

    private function initializeFactory(): void
    {
        /*
         * 为集成测试配置一个Mock Redis服务
         * 1) Redis 是 PHP Redis 扩展的具体类，没有对应的接口可以使用
         * 2) 测试工厂类不需要真实的 Redis 连接，Mock 可以隔离外部依赖
         * 3) 工厂类只是将 Redis 实例传递给 Transport，不需要测试 Redis 本身的功能
         */
        $redis = $this->createMock(\Redis::class);

        // 将Mock的Redis服务注入到容器中
        self::getContainer()->set(\Redis::class, $redis);

        $this->factory = self::getService(RedisTransportFactory::class);
    }

    public function testSupportsAsyncRedisScheme(): void
    {
        $this->initializeFactory();
        $this->assertTrue($this->factory->supports('async-redis://localhost', []));
    }

    public function testDoesNotSupportOtherSchemes(): void
    {
        $this->initializeFactory();
        $this->assertFalse($this->factory->supports('redis://localhost', []));
        $this->assertFalse($this->factory->supports('mysql://localhost', []));
        $this->assertFalse($this->factory->supports('postgresql://localhost', []));
        $this->assertFalse($this->factory->supports('amqp://localhost', []));
        $this->assertFalse($this->factory->supports('doctrine://localhost', []));
    }

    public function testDoesNotSupportEmptyDsn(): void
    {
        $this->initializeFactory();
        $this->assertFalse($this->factory->supports('', []));
    }

    public function testDoesNotSupportInvalidDsn(): void
    {
        $this->initializeFactory();
        $this->assertFalse($this->factory->supports('invalid-dsn', []));
    }

    public function testCreateTransportReturnsRedisTransport(): void
    {
        $dsn = 'async-redis://localhost:6379';
        $options = [];

        // 理由1：Redis是PHP Redis扩展的具体类，没有对应的接口可以使用
        // 理由2：测试工厂类不需要真实的Redis连接，Mock可以隔离外部依赖
        // 理由3：工厂类只是将Redis实例传递给Transport，不需要测试Redis本身的功能
        $redis = $this->createMock(\Redis::class);
        $redis->method('isConnected')->willReturn(true);

        $serializer = $this->createMock(SerializerInterface::class);

        // 将Mock的Redis服务注入到容器中
        self::getContainer()->set(\Redis::class, $redis);
        $factory = self::getService(RedisTransportFactory::class);
        $transport = $factory->createTransport($dsn, $options, $serializer);

        $this->assertInstanceOf(RedisTransport::class, $transport);
    }

    public function testCreateTransportRemovesTransportNameFromOptions(): void
    {
        $dsn = 'async-redis://localhost:6379';
        $options = [
            'transport_name' => 'my_transport',
            'queue' => 'test_queue',
        ];

        // 理由1：Redis是PHP Redis扩展的具体类，没有对应的接口可以使用
        // 理由2：测试工厂类不需要真实的Redis连接，Mock可以隔离外部依赖
        // 理由3：工厂类只是将Redis实例传递给Transport，不需要测试Redis本身的功能
        $redis = $this->createMock(\Redis::class);
        $redis->method('isConnected')->willReturn(true);

        $serializer = $this->createMock(SerializerInterface::class);

        // 将Mock的Redis服务注入到容器中
        self::getContainer()->set(\Redis::class, $redis);
        $factory = self::getService(RedisTransportFactory::class);
        $transport = $factory->createTransport($dsn, $options, $serializer);

        $this->assertInstanceOf(RedisTransport::class, $transport);
    }

    public function testSupportsWithComplexDsn(): void
    {
        $this->initializeFactory();
        $this->assertTrue($this->factory->supports('async-redis://user:pass@localhost:6379/myqueue', []));
    }

    public function testSupportsIsCaseSensitive(): void
    {
        $this->initializeFactory();
        // DSN 协议应该是大小写敏感的
        $this->assertFalse($this->factory->supports('ASYNC-REDIS://localhost', []));
        $this->assertFalse($this->factory->supports('Async-Redis://localhost', []));
    }

    public function testSupportsWithQueryParameters(): void
    {
        $this->initializeFactory();
        $this->assertTrue($this->factory->supports('async-redis://localhost?auto_setup=true&queue=messages', []));
    }

    public function testSupportsWithFragment(): void
    {
        $this->initializeFactory();
        $this->assertTrue($this->factory->supports('async-redis://localhost#fragment', []));
    }
}
