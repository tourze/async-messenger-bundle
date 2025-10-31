<?php

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\RetryableException as DBALRetryableException;
use Doctrine\DBAL\Exception\ServerException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
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

/**
 * @internal
 */
#[CoversClass(DoctrineReceiver::class)]
final class DoctrineReceiverTest extends TestCase
{
    private Connection&MockObject $connection;

    private SerializerInterface&MockObject $serializer;

    private DoctrineReceiver $receiver;

    public function testUsesDefaultSerializerWhenNoneProvided(): void
    {
        $receiver = new DoctrineReceiver($this->connection);

        // 使用反射检查默认序列化器类型
        $reflection = new \ReflectionClass($receiver);
        $serializerProperty = $reflection->getProperty('serializer');
        $serializerProperty->setAccessible(true);
        $serializer = $serializerProperty->getValue($receiver);

        $this->assertInstanceOf(PhpSerializer::class, $serializer);
    }

    public function testGetWithNoMessagesReturnsEmptyArray(): void
    {
        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn(null)
        ;

        $result = $this->receiver->get();

        $this->assertEquals([], iterator_to_array($result));
    }

    public function testGetWithMessageReturnsEnvelopeWithStamps(): void
    {
        $messageData = [
            'id' => '123',
            'body' => 'message-body',
            'headers' => ['header1' => 'value1'],
        ];
        $message = new \stdClass();
        $originalEnvelope = new Envelope($message, []);

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData)
        ;

        $this->serializer->expects($this->once())
            ->method('decode')
            ->with([
                'body' => 'message-body',
                'headers' => ['header1' => 'value1'],
            ])
            ->willReturn($originalEnvelope)
        ;

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

    public function testGetWhenDecodingFailsRejectsMessageAndThrowsException(): void
    {
        $messageData = [
            'id' => '123',
            'body' => 'invalid-body',
            'headers' => [],
        ];
        $decodingException = new MessageDecodingFailedException('Decoding failed');

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData)
        ;

        $this->serializer->expects($this->once())
            ->method('decode')
            ->willThrowException($decodingException)
        ;

        $this->connection->expects($this->once())
            ->method('reject')
            ->with('123')
        ;

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessage('Decoding failed');

        iterator_to_array($this->receiver->get());
    }

    public function testGetWithRetryableExceptionRetriesUpToMaxRetries(): void
    {
        // 理由1：RetryableException是具体异常类，没有对应的接口可以使用
        // 理由2：测试重试行为不需要创建真实的异常对象，Mock可以模拟异常行为
        // 理由3：Receiver检查这个异常类型来决定是否重试，Mock可以控制测试流程
        $retryableException = $this->createMock(DBALRetryableException::class);

        $this->connection->expects($this->exactly(3))
            ->method('get')
            ->willThrowException($retryableException)
        ;

        // 前两次应该返回空数组，第三次应该抛出异常
        $result1 = iterator_to_array($this->receiver->get());
        $this->assertEquals([], $result1);

        $result2 = iterator_to_array($this->receiver->get());
        $this->assertEquals([], $result2);

        $this->expectException(TransportException::class);
        iterator_to_array($this->receiver->get());
    }

    public function testGetWithDBALExceptionThrowsTransportException(): void
    {
        $driverException = $this->createMock(DriverException::class);
        $dbalException = new ServerException($driverException, null);

        $this->connection->expects($this->once())
            ->method('get')
            ->willThrowException($dbalException)
        ;

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('An exception occurred in the driver:');

        iterator_to_array($this->receiver->get());
    }

    public function testAckWithValidEnvelopeAcknowledgesMessage(): void
    {
        $envelope = new Envelope(new \stdClass(), [new DoctrineReceivedStamp('123')]);

        $this->connection->expects($this->once())
            ->method('ack')
            ->with('123')
        ;

        $this->receiver->ack($envelope);
    }

    public function testAckWithoutDoctrineReceivedStampThrowsLogicException(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('No DoctrineReceivedStamp found on the Envelope.');

        $this->receiver->ack($envelope);
    }

    public function testRejectWithValidEnvelopeRejectsMessage(): void
    {
        $envelope = new Envelope(new \stdClass(), [new DoctrineReceivedStamp('456')]);

        $this->connection->expects($this->once())
            ->method('reject')
            ->with('456')
        ;

        $this->receiver->reject($envelope);
    }

    public function testRejectWithoutDoctrineReceivedStampThrowsLogicException(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('No DoctrineReceivedStamp found on the Envelope.');

        $this->receiver->reject($envelope);
    }

    public function testKeepaliveUpdatesMessageDeliveryTime(): void
    {
        $envelope = new Envelope(new \stdClass(), [new DoctrineReceivedStamp('789')]);
        $seconds = 30;

        $this->connection->expects($this->once())
            ->method('keepalive')
            ->with('789', $seconds)
        ;

        $this->receiver->keepalive($envelope, $seconds);
    }

    public function testGetMessageCountReturnsConnectionMessageCount(): void
    {
        $expectedCount = 42;

        $this->connection->expects($this->once())
            ->method('getMessageCount')
            ->willReturn($expectedCount)
        ;

        $result = $this->receiver->getMessageCount();
        $this->assertEquals($expectedCount, $result);
    }

    public function testGetMessageCountWithDBALExceptionThrowsTransportException(): void
    {
        $driverException = $this->createMock(DriverException::class);
        $dbalException = new ServerException($driverException, null);

        $this->connection->expects($this->once())
            ->method('getMessageCount')
            ->willThrowException($dbalException)
        ;

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('An exception occurred in the driver:');

        $this->receiver->getMessageCount();
    }

    public function testAllReturnsAllMessages(): void
    {
        $limit = 10;
        $messagesData = [
            ['id' => '1', 'body' => 'body1', 'headers' => []],
            ['id' => '2', 'body' => 'body2', 'headers' => []],
        ];

        $message1 = new Envelope(new \stdClass(), []);
        $message2 = new Envelope(new \stdClass(), []);

        $this->connection->expects($this->once())
            ->method('findAll')
            ->with($limit)
            ->willReturn((function () use ($messagesData) { yield from $messagesData; })())
        ;

        $this->serializer->expects($this->exactly(2))
            ->method('decode')
            ->willReturnOnConsecutiveCalls($message1, $message2)
        ;

        $result = iterator_to_array($this->receiver->all($limit));

        $this->assertCount(2, $result);
        // 验证所有元素都是正确的类型，因为类型已从PHPDoc推导出，只需验证具体行为
        foreach ($result as $envelope) {
            $this->assertInstanceOf(Envelope::class, $envelope);
        }
    }

    public function testFindWithExistingIdReturnsEnvelope(): void
    {
        $id = '123';
        $messageData = ['id' => '123', 'body' => 'body', 'headers' => []];
        $originalEnvelope = new Envelope(new \stdClass(), []);

        $this->connection->expects($this->once())
            ->method('find')
            ->with($id)
            ->willReturn($messageData)
        ;

        $this->serializer->expects($this->once())
            ->method('decode')
            ->willReturn($originalEnvelope)
        ;

        $result = $this->receiver->find($id);

        $this->assertInstanceOf(Envelope::class, $result);
    }

    public function testFindWithNonExistentIdReturnsNull(): void
    {
        $id = '999';

        $this->connection->expects($this->once())
            ->method('find')
            ->with($id)
            ->willReturn(null)
        ;

        $result = $this->receiver->find($id);

        $this->assertNull($result);
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 理由1：Connection是Doctrine DBAL的具体类，没有对应的接口可以使用
        // 理由2：测试Receiver逻辑不需要真实的数据库交互，Mock可以隔离数据库依赖
        // 理由3：Receiver需要Connection的查询和更新方法，但不需要真实数据库
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->receiver = new DoctrineReceiver($this->connection, $this->serializer);
    }
}
