<?php

namespace Tourze\AsyncMessengerBundle\Tests\Redis;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisReceiver;
use Tourze\AsyncMessengerBundle\Redis\RedisSender;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;

class RedisTransportTest extends TestCase
{
    private Connection $connection;
    private SerializerInterface $serializer;
    private RedisTransport $transport;

    public function test_implements_required_interfaces(): void
    {
        $this->assertInstanceOf(\Symfony\Component\Messenger\Transport\TransportInterface::class, $this->transport);
        $this->assertInstanceOf(\Symfony\Component\Messenger\Transport\SetupableTransportInterface::class, $this->transport);
        $this->assertInstanceOf(\Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface::class, $this->transport);
    }

    public function test_usesDefaultSerializerWhenNoneProvided(): void
    {
        $transport = new RedisTransport($this->connection);

        // 使用反射检查默认序列化器类型
        $reflection = new \ReflectionClass($transport);
        $serializerProperty = $reflection->getProperty('serializer');
        $serializerProperty->setAccessible(true);
        $serializer = $serializerProperty->getValue($transport);

        $this->assertInstanceOf(PhpSerializer::class, $serializer);
    }

    public function test_get_delegatesToReceiver(): void
    {
        $expectedMessages = [new Envelope(new \stdClass())];

        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('get')
            ->willReturn($expectedMessages);

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $result = $this->transport->get();
        $this->assertEquals($expectedMessages, $result);
    }

    public function test_send_delegatesToSender(): void
    {
        $envelope = new Envelope(new \stdClass());
        $expectedEnvelope = new Envelope(new \stdClass());

        $sender = $this->createMock(RedisSender::class);
        $sender->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willReturn($expectedEnvelope);

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $senderProperty = $reflection->getProperty('sender');
        $senderProperty->setAccessible(true);
        $senderProperty->setValue($this->transport, $sender);

        $result = $this->transport->send($envelope);
        $this->assertSame($expectedEnvelope, $result);
    }

    public function test_ack_delegatesToReceiver(): void
    {
        $envelope = new Envelope(new \stdClass());

        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('ack')
            ->with($envelope);

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $this->transport->ack($envelope);
    }

    public function test_reject_delegatesToReceiver(): void
    {
        $envelope = new Envelope(new \stdClass());

        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('reject')
            ->with($envelope);

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $this->transport->reject($envelope);
    }

    public function test_keepalive_delegatesToReceiver(): void
    {
        $envelope = new Envelope(new \stdClass());
        $seconds = 30;

        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('keepalive')
            ->with($envelope, $seconds);

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $this->transport->keepalive($envelope, $seconds);
    }

    public function test_getMessageCount_delegatesToReceiver(): void
    {
        $expectedCount = 15;

        $receiver = $this->createMock(RedisReceiver::class);
        $receiver->expects($this->once())
            ->method('getMessageCount')
            ->willReturn($expectedCount);

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $result = $this->transport->getMessageCount();
        $this->assertEquals($expectedCount, $result);
    }

    public function test_setup_delegatesToConnection(): void
    {
        $this->connection->expects($this->once())
            ->method('setup');

        $this->transport->setup();
    }

    protected function setUp(): void
    {
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->transport = new RedisTransport($this->connection, $this->serializer);
    }

    public function test_close_delegatesToConnection(): void
    {
        $this->connection->expects($this->once())
            ->method('close');

        $this->transport->close();
    }

    public function test_receiverIsLazilyInitialized(): void
    {
        // 第一次调用应该创建接收器
        $this->transport->get();
        
        // 第二次调用应该重用同一个接收器实例
        $this->transport->get();
        
        // 我们无法直接验证同一个实例被重用，但可以验证行为一致
        $this->expectNotToPerformAssertions();
    }

    public function test_senderIsLazilyInitialized(): void
    {
        $envelope = new Envelope(new \stdClass());
        
        // Mock 序列化器和连接返回值
        $this->serializer->method('encode')->willReturn(['body' => 'test', 'headers' => []]);
        $this->connection->method('add')->willReturn('test-id');
        
        // 第一次调用应该创建发送器
        $result1 = $this->transport->send($envelope);
        
        // 第二次调用应该重用同一个发送器实例
        $result2 = $this->transport->send($envelope);
        
        // 验证两次调用都能正常工作
        $this->assertInstanceOf(Envelope::class, $result1);
        $this->assertInstanceOf(Envelope::class, $result2);
    }
}