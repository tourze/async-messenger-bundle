<?php

namespace Tourze\AsyncMessengerBundle\Tests\Redis;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisReceiver;
use Tourze\AsyncMessengerBundle\Redis\RedisSender;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;

/**
 * @internal
 */
#[CoversClass(RedisTransport::class)]
final class RedisTransportTest extends TestCase
{
    private Connection&MockObject $connection;

    private SerializerInterface&MockObject $serializer;

    private RedisTransport $transport;

    public function testUsesDefaultSerializerWhenNoneProvided(): void
    {
        $transport = new RedisTransport($this->connection);

        // 使用反射检查默认序列化器类型
        $reflection = new \ReflectionClass($transport);
        $serializerProperty = $reflection->getProperty('serializer');
        $serializerProperty->setAccessible(true);
        $serializer = $serializerProperty->getValue($transport);

        $this->assertInstanceOf(PhpSerializer::class, $serializer);
    }

    public function testGetDelegatesToReceiver(): void
    {
        $expectedMessages = [new Envelope(new \stdClass(), [])];

        // 理由1：RedisReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托get()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('get')
            ->willReturn($expectedMessages)
        ;

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $result = $this->transport->get();
        $this->assertEquals($expectedMessages, $result);
    }

    public function testSendDelegatesToSender(): void
    {
        $envelope = new Envelope(new \stdClass(), []);
        $expectedEnvelope = new Envelope(new \stdClass(), []);

        // 理由1：RedisSender是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Sender的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托send()调用给sender，不需要实现sender逻辑
        $sender = $this->createMock(RedisSender::class);
        $sender->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willReturn($expectedEnvelope)
        ;

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $senderProperty = $reflection->getProperty('sender');
        $senderProperty->setAccessible(true);
        $senderProperty->setValue($this->transport, $sender);

        $result = $this->transport->send($envelope);
        $this->assertSame($expectedEnvelope, $result);
    }

    public function testAckDelegatesToReceiver(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        // 理由1：RedisReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('ack')
            ->with($envelope)
        ;

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $this->transport->ack($envelope);
    }

    public function testRejectDelegatesToReceiver(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        // 理由1：RedisReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('reject')
            ->with($envelope)
        ;

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $this->transport->reject($envelope);
    }

    public function testKeepaliveDelegatesToReceiver(): void
    {
        $envelope = new Envelope(new \stdClass(), []);
        $seconds = 30;

        // 理由1：RedisReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托keepalive()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('keepalive')
            ->with($envelope, $seconds)
        ;

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $this->transport->keepalive($envelope, $seconds);
    }

    public function testGetMessageCountDelegatesToReceiver(): void
    {
        $expectedCount = 15;

        // 理由1：RedisReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托getMessageCount()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('getMessageCount')
            ->willReturn($expectedCount)
        ;

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $result = $this->transport->getMessageCount();
        $this->assertEquals($expectedCount, $result);
    }

    public function testSetupDelegatesToConnection(): void
    {
        $this->connection->expects($this->once())
            ->method('setup')
        ;

        $this->transport->setup();
    }

    protected function setUp(): void
    {
        parent::setUp();
        // 理由1：Connection是Redis transport实现的具体类，没有对应的接口可以使用
        // 理由2：测试Transport逻辑不需要真实的Redis服务器交互，Mock可以隔离外部依赖
        // 理由3：Transport通过sender/receiver使用connection，不直接使用它
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->transport = new RedisTransport($this->connection, $this->serializer);
    }

    public function testCloseDelegatesToConnection(): void
    {
        $this->connection->expects($this->once())
            ->method('close')
        ;

        $this->transport->close();
    }

    public function testReceiverIsLazilyInitialized(): void
    {
        // 第一次调用应该创建接收器
        $this->transport->get();

        // 第二次调用应该重用同一个接收器实例
        $this->transport->get();

        // 我们无法直接验证同一个实例被重用，但可以验证行为一致
        $this->expectNotToPerformAssertions();
    }

    public function testSenderIsLazilyInitialized(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        // Mock 序列化器和连接返回值
        $this->serializer->method('encode')->willReturn(['body' => 'test', 'headers' => []]);
        $this->connection->method('add')->willReturn('test-id');

        // 第一次调用应该创建发送器
        $result1 = $this->transport->send($envelope);

        // 第二次调用应该重用同一个发送器实例
        $result2 = $this->transport->send($envelope);

        // 验证两次调用都能正常工作
        $this->assertNotNull($result1);
        $this->assertNotNull($result2);
    }

    public function testCleanup(): void
    {
        $this->connection->expects($this->once())
            ->method('cleanup')
        ;

        $this->transport->cleanup();
    }
}
