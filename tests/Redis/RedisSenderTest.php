<?php

namespace Tourze\AsyncMessengerBundle\Tests\Redis;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\StampInterface;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisSender;

/**
 * @internal
 */
#[CoversClass(RedisSender::class)]
final class RedisSenderTest extends TestCase
{
    private Connection&MockObject $connection;

    private SerializerInterface&MockObject $serializer;

    private RedisSender $sender;

    public function testSendWithoutDelaySendsMessageImmediately(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, []);
        $encodedMessage = ['body' => 'encoded-body', 'headers' => ['header1' => 'value1']];
        $messageId = 'redis-id-123';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('add')
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
        $delay = 3000; // 3 seconds in milliseconds
        $envelope = new Envelope($message, [new DelayStamp($delay)]);
        $encodedMessage = ['body' => 'encoded-body', 'headers' => ['header1' => 'value1']];
        $messageId = 'redis-id-456';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('add')
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
            new DelayStamp(4000), // This should be used
        ]);
        $encodedMessage = ['body' => 'encoded-body'];
        $messageId = 'redis-id-789';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('add')
            ->with('encoded-body', [], 4000)
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);
    }

    public function testSendWithEncodedMessageWithoutHeadersUsesEmptyHeaders(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, []);
        $encodedMessage = ['body' => 'encoded-body']; // No headers key
        $messageId = 'redis-id-101';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('add')
            ->with('encoded-body', [], 0)
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);
    }

    public function testSendPreservesOtherStampsOnEnvelope(): void
    {
        $message = new \stdClass();
        $customStamp = new class implements StampInterface {};
        $envelope = new Envelope($message, [$customStamp]);
        $encodedMessage = ['body' => 'encoded-body'];
        $messageId = 'redis-id-202';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('add')
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);

        // 验证原始邮票仍然存在
        $this->assertCount(1, $result->all(get_class($customStamp)));
        // 验证新增了 TransportMessageIdStamp
        $this->assertCount(1, $result->all(TransportMessageIdStamp::class));
    }

    public function testSendWithZeroDelaySendsImmediately(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, [new DelayStamp(0)]);
        $encodedMessage = ['body' => 'encoded-body'];
        $messageId = 'redis-id-zero';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('add')
            ->with('encoded-body', [], 0)
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);
    }

    public function testSendWithLargeDelayHandlesCorrectly(): void
    {
        $message = new \stdClass();
        $largeDelay = 60000; // 1 minute
        $envelope = new Envelope($message, [new DelayStamp($largeDelay)]);
        $encodedMessage = ['body' => 'encoded-body'];
        $messageId = 'redis-id-large';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage)
        ;

        $this->connection->expects($this->once())
            ->method('add')
            ->with('encoded-body', [], $largeDelay)
            ->willReturn($messageId)
        ;

        $result = $this->sender->send($envelope);
    }

    protected function setUp(): void
    {
        parent::setUp();
        // 理由1：Connection是Redis transport实现的具体类，没有对应的接口可以使用
        // 理由2：测试Sender逻辑不需要真实的Redis服务器交互，Mock可以隔离外部依赖
        // 理由3：Sender只需要Connection的特定方法，不需要完整的Redis功能
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->sender = new RedisSender($this->connection, $this->serializer);
    }
}
