<?php

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\RetryableException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineReceiver;
use Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp;

class DoctrineReceiverTest extends TestCase
{
    private Connection $connection;
    private SerializerInterface $serializer;
    private DoctrineReceiver $receiver;

    public function test_implements_required_interfaces(): void
    {
        $this->assertInstanceOf(\Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface::class, $this->receiver);
        $this->assertInstanceOf(\Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface::class, $this->receiver);
    }

    public function test_usesDefaultSerializerWhenNoneProvided(): void
    {
        $receiver = new DoctrineReceiver($this->connection);

        // 使用反射检查默认序列化器类型
        $reflection = new \ReflectionClass($receiver);
        $serializerProperty = $reflection->getProperty('serializer');
        $serializerProperty->setAccessible(true);
        $serializer = $serializerProperty->getValue($receiver);

        $this->assertInstanceOf(PhpSerializer::class, $serializer);
    }

    public function test_get_withNoMessages_returnsEmptyArray(): void
    {
        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn(null);

        $result = $this->receiver->get();

        $this->assertEquals([], iterator_to_array($result));
    }

    public function test_get_withMessage_returnsEnvelopeWithStamps(): void
    {
        $messageData = [
            'id' => '123',
            'body' => 'message-body',
            'headers' => ['header1' => 'value1']
        ];
        $message = new \stdClass();
        $originalEnvelope = new Envelope($message);

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData);

        $this->serializer->expects($this->once())
            ->method('decode')
            ->with([
                'body' => 'message-body',
                'headers' => ['header1' => 'value1']
            ])
            ->willReturn($originalEnvelope);

        $result = iterator_to_array($this->receiver->get());

        $this->assertCount(1, $result);
        $envelope = $result[0];
        $this->assertInstanceOf(Envelope::class, $envelope);

        // 验证包含正确的 Stamps
        $doctrineStamp = $envelope->last(DoctrineReceivedStamp::class);
        $this->assertInstanceOf(DoctrineReceivedStamp::class, $doctrineStamp);
        $this->assertEquals('123', $doctrineStamp->getId());

        $transportIdStamp = $envelope->last(TransportMessageIdStamp::class);
        $this->assertInstanceOf(TransportMessageIdStamp::class, $transportIdStamp);
        $this->assertEquals('123', $transportIdStamp->getId());
    }

    public function test_get_whenDecodingFails_rejectsMessageAndThrowsException(): void
    {
        $messageData = [
            'id' => '123',
            'body' => 'invalid-body',
            'headers' => []
        ];
        $decodingException = new MessageDecodingFailedException('Decoding failed');

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData);

        $this->serializer->expects($this->once())
            ->method('decode')
            ->willThrowException($decodingException);

        $this->connection->expects($this->once())
            ->method('reject')
            ->with('123');

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessage('Decoding failed');

        iterator_to_array($this->receiver->get());
    }

    public function test_get_withRetryableException_retriesUpToMaxRetries(): void
    {
        $retryableException = $this->createMock(RetryableException::class);

        $this->connection->expects($this->exactly(3))
            ->method('get')
            ->willThrowException($retryableException);

        // 前两次应该返回空数组，第三次应该抛出异常
        $result1 = iterator_to_array($this->receiver->get());
        $this->assertEquals([], $result1);

        $result2 = iterator_to_array($this->receiver->get());
        $this->assertEquals([], $result2);

        $this->expectException(TransportException::class);
        iterator_to_array($this->receiver->get());
    }

    public function test_get_withDBALException_throwsTransportException(): void
    {
        $dbalException = new class('Database error') extends \Exception implements DBALException {};

        $this->connection->expects($this->once())
            ->method('get')
            ->willThrowException($dbalException);

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Database error');

        iterator_to_array($this->receiver->get());
    }

    public function test_ack_withValidEnvelope_acknowledgesMessage(): void
    {
        $envelope = new Envelope(new \stdClass(), [new DoctrineReceivedStamp('123')]);

        $this->connection->expects($this->once())
            ->method('ack')
            ->with('123');

        $this->receiver->ack($envelope);
    }

    public function test_ack_withoutDoctrineReceivedStamp_throwsLogicException(): void
    {
        $envelope = new Envelope(new \stdClass());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('No DoctrineReceivedStamp found on the Envelope.');

        $this->receiver->ack($envelope);
    }

    public function test_reject_withValidEnvelope_rejectsMessage(): void
    {
        $envelope = new Envelope(new \stdClass(), [new DoctrineReceivedStamp('456')]);

        $this->connection->expects($this->once())
            ->method('reject')
            ->with('456');

        $this->receiver->reject($envelope);
    }

    public function test_reject_withoutDoctrineReceivedStamp_throwsLogicException(): void
    {
        $envelope = new Envelope(new \stdClass());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('No DoctrineReceivedStamp found on the Envelope.');

        $this->receiver->reject($envelope);
    }

    public function test_keepalive_updatesMessageDeliveryTime(): void
    {
        $envelope = new Envelope(new \stdClass(), [new DoctrineReceivedStamp('789')]);
        $seconds = 30;

        $this->connection->expects($this->once())
            ->method('keepalive')
            ->with('789', $seconds);

        $this->receiver->keepalive($envelope, $seconds);
    }

    public function test_getMessageCount_returnsConnectionMessageCount(): void
    {
        $expectedCount = 42;

        $this->connection->expects($this->once())
            ->method('getMessageCount')
            ->willReturn($expectedCount);

        $result = $this->receiver->getMessageCount();
        $this->assertEquals($expectedCount, $result);
    }

    public function test_getMessageCount_withDBALException_throwsTransportException(): void
    {
        $dbalException = new class('Count failed') extends \Exception implements DBALException {};

        $this->connection->expects($this->once())
            ->method('getMessageCount')
            ->willThrowException($dbalException);

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Count failed');

        $this->receiver->getMessageCount();
    }

    public function test_all_returnsAllMessages(): void
    {
        $limit = 10;
        $messagesData = [
            ['id' => '1', 'body' => 'body1', 'headers' => []],
            ['id' => '2', 'body' => 'body2', 'headers' => []]
        ];

        $message1 = new Envelope(new \stdClass());
        $message2 = new Envelope(new \stdClass());

        $this->connection->expects($this->once())
            ->method('findAll')
            ->with($limit)
            ->willReturn($messagesData);

        $this->serializer->expects($this->exactly(2))
            ->method('decode')
            ->willReturnOnConsecutiveCalls($message1, $message2);

        $result = iterator_to_array($this->receiver->all($limit));

        $this->assertCount(2, $result);
        $this->assertContainsOnlyInstancesOf(Envelope::class, $result);
    }

    public function test_find_withExistingId_returnsEnvelope(): void
    {
        $id = '123';
        $messageData = ['id' => '123', 'body' => 'body', 'headers' => []];
        $originalEnvelope = new Envelope(new \stdClass());

        $this->connection->expects($this->once())
            ->method('find')
            ->with($id)
            ->willReturn($messageData);

        $this->serializer->expects($this->once())
            ->method('decode')
            ->willReturn($originalEnvelope);

        $result = $this->receiver->find($id);

        $this->assertInstanceOf(Envelope::class, $result);
    }

    public function test_find_withNonExistentId_returnsNull(): void
    {
        $id = '999';

        $this->connection->expects($this->once())
            ->method('find')
            ->with($id)
            ->willReturn(null);

        $result = $this->receiver->find($id);

        $this->assertNull($result);
    }

    protected function setUp(): void
    {
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->receiver = new DoctrineReceiver($this->connection, $this->serializer);
    }
}