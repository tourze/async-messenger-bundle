<?php

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\ServerException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\StampInterface;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineSender;

/**
 * @internal
 */
#[CoversClass(DoctrineSender::class)]
final class DoctrineSenderTest extends TestCase
{
    private Connection&MockObject $connection;

    private SerializerInterface&MockObject $serializer;

    private DoctrineSender $sender;

    public function testUsesDefaultSerializerWhenNoneProvided(): void
    {
        $sender = new DoctrineSender($this->connection);

        // 使用反射检查默认序列化器类型
        $reflection = new \ReflectionClass($sender);
        $serializerProperty = $reflection->getProperty('serializer');
        $serializerProperty->setAccessible(true);
        $serializer = $serializerProperty->getValue($sender);

        $this->assertInstanceOf(PhpSerializer::class, $serializer);
    }

    public function testSendWithoutDelaySendsMessageImmediately(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, []);
        $encodedMessage = ['body' => 'encoded-body', 'headers' => ['header1' => 'value1']];
        $messageId = '123';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('send')
            ->with('encoded-body', ['header1' => 'value1'], 0)
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);
        $transportIdStamp = $result->last(TransportMessageIdStamp::class);
        $this->assertInstanceOf(TransportMessageIdStamp::class, $transportIdStamp);
        $this->assertEquals($messageId, $transportIdStamp->getId());
    }

    public function testSendWithDelayStampSendsMessageWithDelay(): void
    {
        $message = new \stdClass();
        $delay = 5000; // 5 seconds in milliseconds
        $envelope = new Envelope($message, [new DelayStamp($delay)]);
        $encodedMessage = ['body' => 'encoded-body', 'headers' => ['header1' => 'value1']];
        $messageId = '456';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('send')
            ->with('encoded-body', ['header1' => 'value1'], $delay)
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);
        $transportIdStamp = $result->last(TransportMessageIdStamp::class);
        $this->assertInstanceOf(TransportMessageIdStamp::class, $transportIdStamp);
        $this->assertEquals($messageId, $transportIdStamp->getId());
    }

    public function testSendWithMultipleDelayStampsUsesLastDelayStamp(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, [
            new DelayStamp(1000),
            new DelayStamp(2000),
            new DelayStamp(3000), // This should be used
        ]);
        $encodedMessage = ['body' => 'encoded-body'];
        $messageId = '789';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('send')
            ->with('encoded-body', [], 3000)
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);
    }

    public function testSendWithEncodedMessageWithoutHeadersUsesEmptyHeaders(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, []);
        $encodedMessage = ['body' => 'encoded-body']; // No headers key
        $messageId = '101';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('send')
            ->with('encoded-body', [], 0)
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);
    }

    public function testSendWhenConnectionThrowsDBALExceptionThrowsTransportException(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, []);
        $encodedMessage = ['body' => 'encoded-body'];
        $driverException = $this->createMock(DriverException::class);
        $dbalException = new ServerException($driverException, null);

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('send')
            ->willThrowException($dbalException)
        ;

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('An exception occurred in the driver:');

        $this->sender->send($envelope);
    }

    public function testSendPreservesOtherStampsOnEnvelope(): void
    {
        $message = new \stdClass();
        $customStamp = new class implements StampInterface {};
        $envelope = new Envelope($message, [$customStamp]);
        $encodedMessage = ['body' => 'encoded-body'];
        $messageId = '202';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('send')
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);

        // 验证原始邮票仍然存在
        $this->assertCount(1, $result->all(get_class($customStamp)));
        // 验证新增了 TransportMessageIdStamp
        $this->assertCount(1, $result->all(TransportMessageIdStamp::class));
    }

    protected function setUp(): void
    {
        parent::setUp();
        // 理由1：Connection是Doctrine DBAL的具体类，没有对应的接口可以使用
        // 理由2：测试Sender逻辑不需要真实的数据库交互，Mock可以隔离数据库依赖
        // 理由3：Sender只需要Connection的特定方法，不需要完整的数据库功能
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->sender = new DoctrineSender($this->connection, $this->serializer);
    }
}
