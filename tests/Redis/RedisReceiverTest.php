<?php

namespace Tourze\AsyncMessengerBundle\Tests\Redis;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisReceiver;
use Tourze\AsyncMessengerBundle\Stamp\RedisReceivedStamp;

/**
 * @internal
 */
#[CoversClass(RedisReceiver::class)]
final class RedisReceiverTest extends TestCase
{
    private Connection&MockObject $connection;

    private SerializerInterface&MockObject $serializer;

    private RedisReceiver $receiver;

    public function testUsesDefaultSerializerWhenNoneProvided(): void
    {
        $receiver = new RedisReceiver($this->connection);

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
            'id' => '1609459200000-0',
            'data' => ['message' => '{"body":"test-body","headers":{"header1":"value1"}}'],
        ];
        $originalMessage = new \stdClass();
        $originalEnvelope = new Envelope($originalMessage, []);

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData)
        ;

        $this->serializer->expects($this->once())
            ->method('decode')
            ->with([
                'body' => 'test-body',
                'headers' => ['header1' => 'value1'],
            ])
            ->willReturn($originalEnvelope)
        ;

        $result = iterator_to_array($this->receiver->get());

        $this->assertCount(1, $result);
        $envelope = $result[0];
        $this->assertInstanceOf(Envelope::class, $envelope);

        // 验证包含正确的 Stamps
        $redisStamp = $envelope->last(RedisReceivedStamp::class);
        $this->assertInstanceOf(RedisReceivedStamp::class, $redisStamp);
        $this->assertEquals('1609459200000-0', $redisStamp->getId());

        $transportIdStamp = $envelope->last(TransportMessageIdStamp::class);
        $this->assertInstanceOf(TransportMessageIdStamp::class, $transportIdStamp);
        $this->assertEquals('1609459200000-0', $transportIdStamp->getId());
    }

    public function testGetWithLegacyMessageFormatDecodesCorrectly(): void
    {
        $messageData = [
            'id' => '1609459200000-1',
            'data' => ['message' => '{"some":"legacy_format"}'],
        ];
        $originalMessage = new \stdClass();
        $originalEnvelope = new Envelope($originalMessage, []);

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData)
        ;

        $this->serializer->expects($this->once())
            ->method('decode')
            ->with(['some' => 'legacy_format'])
            ->willReturn($originalEnvelope)
        ;

        $result = iterator_to_array($this->receiver->get());

        $this->assertCount(1, $result);
        $this->assertInstanceOf(Envelope::class, $result[0]);
    }

    public function testGetWithInvalidJsonMessageReturnsEmptyArray(): void
    {
        $messageData = [
            'id' => '1609459200000-2',
            'data' => ['message' => 'invalid-json{'],
        ];

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData)
        ;

        $result = iterator_to_array($this->receiver->get());

        $this->assertEquals([], $result);
    }

    public function testGetWithNullDataRejectsAndRetries(): void
    {
        $messageData = [
            'id' => '1609459200000-3',
            'data' => null,
        ];

        $this->connection->expects($this->exactly(2))
            ->method('get')
            ->willReturnOnConsecutiveCalls($messageData, null)
        ;

        $this->connection->expects($this->once())
            ->method('reject')
            ->with('1609459200000-3')
        ;

        $result = iterator_to_array($this->receiver->get());

        $this->assertEquals([], $result);
    }

    public function testGetWhenDecodingFailsRejectsMessageAndThrowsException(): void
    {
        $messageData = [
            'id' => '1609459200000-4',
            'data' => ['message' => '{"body":"test","headers":{}}'],
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
            ->with('1609459200000-4')
        ;

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessage('Decoding failed');

        iterator_to_array($this->receiver->get());
    }

    public function testAckWithValidEnvelopeAcknowledgesMessage(): void
    {
        $envelope = new Envelope(new \stdClass(), [new RedisReceivedStamp('1609459200000-5')]);

        $this->connection->expects($this->once())
            ->method('ack')
            ->with('1609459200000-5')
        ;

        $this->receiver->ack($envelope);
    }

    public function testAckWithoutRedisReceivedStampThrowsLogicException(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('No RedisReceivedStamp found on the Envelope.');

        $this->receiver->ack($envelope);
    }

    public function testRejectWithValidEnvelopeRejectsMessage(): void
    {
        $envelope = new Envelope(new \stdClass(), [new RedisReceivedStamp('1609459200000-6')]);

        $this->connection->expects($this->once())
            ->method('reject')
            ->with('1609459200000-6')
        ;

        $this->receiver->reject($envelope);
    }

    public function testRejectWithoutRedisReceivedStampThrowsLogicException(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('No RedisReceivedStamp found on the Envelope.');

        $this->receiver->reject($envelope);
    }

    public function testKeepaliveUpdatesMessageState(): void
    {
        $envelope = new Envelope(new \stdClass(), [new RedisReceivedStamp('1609459200000-7')]);
        $seconds = 60;

        $this->connection->expects($this->once())
            ->method('keepalive')
            ->with('1609459200000-7', $seconds)
        ;

        $this->receiver->keepalive($envelope, $seconds);
    }

    public function testGetMessageCountReturnsConnectionMessageCount(): void
    {
        $expectedCount = 25;

        $this->connection->expects($this->once())
            ->method('getMessageCount')
            ->willReturn($expectedCount)
        ;

        $result = $this->receiver->getMessageCount();
        $this->assertEquals($expectedCount, $result);
    }

    public function testGetWithMissingMessageKeyReturnsEmptyArray(): void
    {
        $messageData = [
            'id' => '1609459200000-8',
            'data' => ['other_key' => 'some_data'], // Missing 'message' key
        ];

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData)
        ;

        $result = iterator_to_array($this->receiver->get());

        $this->assertEquals([], $result);
    }

    protected function setUp(): void
    {
        parent::setUp();
        // 理由1：Connection是Redis transport实现的具体类，没有对应的接口可以使用
        // 理由2：测试Receiver逻辑不需要真实的Redis服务器交互，Mock可以隔离外部依赖
        // 理由3：Receiver需要Connection的获取和确认消息方法，但不需要真实Redis
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->receiver = new RedisReceiver($this->connection, $this->serializer);
    }
}
