<?php

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use Doctrine\DBAL\Exception as DBALException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineSender;

class DoctrineSenderTest extends TestCase
{
    private Connection $connection;
    private SerializerInterface $serializer;
    private DoctrineSender $sender;

    public function test_implements_sender_interface(): void
    {
        $this->assertInstanceOf(\Symfony\Component\Messenger\Transport\Sender\SenderInterface::class, $this->sender);
    }

    public function test_usesDefaultSerializerWhenNoneProvided(): void
    {
        $sender = new DoctrineSender($this->connection);

        // 使用反射检查默认序列化器类型
        $reflection = new \ReflectionClass($sender);
        $serializerProperty = $reflection->getProperty('serializer');
        $serializerProperty->setAccessible(true);
        $serializer = $serializerProperty->getValue($sender);

        $this->assertInstanceOf(PhpSerializer::class, $serializer);
    }

    public function test_send_withoutDelay_sendsMessageImmediately(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message);
        $encodedMessage = ['body' => 'encoded-body', 'headers' => ['header1' => 'value1']];
        $messageId = '123';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage);

        $this->connection->expects($this->once())
            ->method('send')
            ->with('encoded-body', ['header1' => 'value1'], 0)
            ->willReturn($messageId);

        $result = $this->sender->send($envelope);

        $this->assertInstanceOf(Envelope::class, $result);
        $transportIdStamp = $result->last(TransportMessageIdStamp::class);
        $this->assertInstanceOf(TransportMessageIdStamp::class, $transportIdStamp);
        $this->assertEquals($messageId, $transportIdStamp->getId());
    }

    public function test_send_withDelayStamp_sendsMessageWithDelay(): void
    {
        $message = new \stdClass();
        $delay = 5000; // 5 seconds in milliseconds
        $envelope = new Envelope($message, [new DelayStamp($delay)]);
        $encodedMessage = ['body' => 'encoded-body', 'headers' => ['header1' => 'value1']];
        $messageId = '456';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage);

        $this->connection->expects($this->once())
            ->method('send')
            ->with('encoded-body', ['header1' => 'value1'], $delay)
            ->willReturn($messageId);

        $result = $this->sender->send($envelope);

        $this->assertInstanceOf(Envelope::class, $result);
        $transportIdStamp = $result->last(TransportMessageIdStamp::class);
        $this->assertInstanceOf(TransportMessageIdStamp::class, $transportIdStamp);
        $this->assertEquals($messageId, $transportIdStamp->getId());
    }

    public function test_send_withMultipleDelayStamps_usesLastDelayStamp(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, [
            new DelayStamp(1000),
            new DelayStamp(2000),
            new DelayStamp(3000) // This should be used
        ]);
        $encodedMessage = ['body' => 'encoded-body'];
        $messageId = '789';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage);

        $this->connection->expects($this->once())
            ->method('send')
            ->with('encoded-body', [], 3000)
            ->willReturn($messageId);

        $result = $this->sender->send($envelope);

        $this->assertInstanceOf(Envelope::class, $result);
    }

    public function test_send_withEncodedMessageWithoutHeaders_usesEmptyHeaders(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message);
        $encodedMessage = ['body' => 'encoded-body']; // No headers key
        $messageId = '101';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage);

        $this->connection->expects($this->once())
            ->method('send')
            ->with('encoded-body', [], 0)
            ->willReturn($messageId);

        $result = $this->sender->send($envelope);

        $this->assertInstanceOf(Envelope::class, $result);
    }

    public function test_send_whenConnectionThrowsDBALException_throwsTransportException(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message);
        $encodedMessage = ['body' => 'encoded-body'];
        $dbalException = new class('Database error') extends \Exception implements DBALException {};

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage);

        $this->connection->expects($this->once())
            ->method('send')
            ->willThrowException($dbalException);

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Database error');

        $this->sender->send($envelope);
    }

    public function test_send_preservesOtherStampsOnEnvelope(): void
    {
        $message = new \stdClass();
        $customStamp = new class implements \Symfony\Component\Messenger\Stamp\StampInterface {};
        $envelope = new Envelope($message, [$customStamp]);
        $encodedMessage = ['body' => 'encoded-body'];
        $messageId = '202';

        $this->serializer->expects($this->once())
            ->method('encode')
            ->with($envelope)
            ->willReturn($encodedMessage);

        $this->connection->expects($this->once())
            ->method('send')
            ->willReturn($messageId);

        $result = $this->sender->send($envelope);

        // 验证原始邮票仍然存在
        $this->assertCount(1, $result->all(get_class($customStamp)));
        // 验证新增了 TransportMessageIdStamp
        $this->assertCount(1, $result->all(TransportMessageIdStamp::class));
    }

    protected function setUp(): void
    {
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->sender = new DoctrineSender($this->connection, $this->serializer);
    }
}