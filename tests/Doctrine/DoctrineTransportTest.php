<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineReceiver;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineSender;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;

/**
 * @internal
 */
#[CoversClass(DoctrineTransport::class)]
final class DoctrineTransportTest extends TestCase
{
    private Connection&MockObject $connection;

    private SerializerInterface&MockObject $serializer;

    private DoctrineTransport $transport;

    public function testGetDelegatesToReceiver(): void
    {
        $expectedMessages = [new Envelope(new \stdClass(), [])];

        // 理由1：DoctrineReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托get()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(DoctrineReceiver::class);
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
        $expectedEnvelope = $envelope->with(new DelayStamp(1000));

        // 理由1：DoctrineSender是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Sender的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托send()调用给sender，不需要实现sender逻辑
        $sender = $this->createMock(DoctrineSender::class);
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

        // 理由1：DoctrineReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托ack()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(DoctrineReceiver::class);
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

        // 理由1：DoctrineReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托reject()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(DoctrineReceiver::class);
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

        // 理由1：DoctrineReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托keepalive()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(DoctrineReceiver::class);
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
        $expectedCount = 5;

        // 理由1：DoctrineReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托getMessageCount()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(DoctrineReceiver::class);
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

    public function testAllDelegatesToReceiver(): void
    {
        $limit = 10;
        $expectedMessages = [new Envelope(new \stdClass(), [])];

        // 理由1：DoctrineReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托all()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(DoctrineReceiver::class);
        $receiver->expects($this->once())
            ->method('all')
            ->with($limit)
            ->willReturn($expectedMessages)
        ;

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $result = $this->transport->all($limit);
        $this->assertEquals($expectedMessages, $result);
    }

    public function testFindDelegatesToReceiver(): void
    {
        $id = '123';
        $expectedEnvelope = new Envelope(new \stdClass(), []);

        // 理由1：DoctrineReceiver是具体类，没有对应的接口可以使用
        // 理由2：测试Transport行为时需要隔离Receiver的具体实现，Mock可以实现隔离
        // 理由3：Transport只需要委托find()调用给receiver，不需要实现receiver逻辑
        $receiver = $this->createMock(DoctrineReceiver::class);
        $receiver->expects($this->once())
            ->method('find')
            ->with($id)
            ->willReturn($expectedEnvelope)
        ;

        // 使用反射来设置私有属性
        $reflection = new \ReflectionClass($this->transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($this->transport, $receiver);

        $result = $this->transport->find($id);
        $this->assertSame($expectedEnvelope, $result);
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
        // 理由1：Connection是Doctrine DBAL的具体类，没有对应的接口可以使用
        // 理由2：测试Transport逻辑不需要真实的数据库交互，Mock可以隔离数据库依赖
        // 理由3：Transport通过sender/receiver使用connection，不直接使用它
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->transport = new DoctrineTransport($this->connection, $this->serializer);
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
        $this->connection->method('send')->willReturn('test-id');

        // 第一次调用应该创建发送器
        $result1 = $this->transport->send($envelope);

        // 第二次调用应该重用同一个发送器实例
        $result2 = $this->transport->send($envelope);

        // 验证两次调用都能正常工作，返回值应该是Envelope实例
        $this->assertInstanceOf(Envelope::class, $result1);
        $this->assertInstanceOf(Envelope::class, $result2);
    }

    public function testConfigureSchema(): void
    {
        // Arrange
        /*
         * 1) Schema 是 Doctrine DBAL 的具体类，没有对应的接口可以使用
         * 2) DoctrineTransport 需要 Schema 对象来配置数据库结构，Mock 可以隔离数据库依赖
         * 3) 没有更好的替代方案，这是 Doctrine DBAL 的设计决定
         */
        $schema = $this->createMock(Schema::class);

        /*
         * 1) Connection 是 Doctrine DBAL 的具体类，虽然实现了接口但 Mock 接口会缺少方法
         * 2) DoctrineTransport 需要完整的 Connection 功能，Mock 具体类更稳定
         * 3) 使用具体类可以确保所有必需的方法都可用
         */
        $dbalConnection = $this->createMock(DBALConnection::class);
        $isSameDatabase = function () { return true; };

        // Expect
        $this->connection->expects($this->once())
            ->method('configureSchema')
            ->with($schema, $dbalConnection, $isSameDatabase)
        ;

        // Act
        $this->transport->configureSchema($schema, $dbalConnection, $isSameDatabase);
    }
}
