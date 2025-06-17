<?php

namespace Tourze\AsyncMessengerBundle\Tests\Redis;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisReceivedStamp;
use Tourze\AsyncMessengerBundle\Redis\RedisReceiver;

class RedisReceiverTest extends TestCase
{
    private Connection $connection;
    private SerializerInterface $serializer;
    private RedisReceiver $receiver;

    public function test_implements_required_interfaces(): void
    {
        $this->assertInstanceOf(\Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface::class, $this->receiver);
    }

    public function test_usesDefaultSerializerWhenNoneProvided(): void
    {
        $receiver = new RedisReceiver($this->connection);

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

        $this->assertIsIterable($result);
        $this->assertEquals([], iterator_to_array($result));
    }

    public function test_get_withMessage_returnsEnvelopeWithStamps(): void
    {
        $messageData = [
            'id' => '1609459200000-0',
            'data' => ['message' => '{"body":"test-body","headers":{"header1":"value1"}}']
        ];
        $originalMessage = new \stdClass();
        $originalEnvelope = new Envelope($originalMessage);

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData);

        $this->serializer->expects($this->once())
            ->method('decode')
            ->with([
                'body' => 'test-body',
                'headers' => ['header1' => 'value1']
            ])
            ->willReturn($originalEnvelope);

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

    public function test_get_withLegacyMessageFormat_decodesCorrectly(): void
    {
        $messageData = [
            'id' => '1609459200000-1',
            'data' => ['message' => '{"some":"legacy_format"}']
        ];
        $originalMessage = new \stdClass();
        $originalEnvelope = new Envelope($originalMessage);

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData);

        $this->serializer->expects($this->once())
            ->method('decode')
            ->with(['some' => 'legacy_format'])
            ->willReturn($originalEnvelope);

        $result = iterator_to_array($this->receiver->get());

        $this->assertCount(1, $result);
        $this->assertInstanceOf(Envelope::class, $result[0]);
    }

    public function test_get_withInvalidJsonMessage_returnsEmptyArray(): void
    {
        $messageData = [
            'id' => '1609459200000-2',
            'data' => ['message' => 'invalid-json{']
        ];

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData);

        $result = iterator_to_array($this->receiver->get());

        $this->assertEquals([], $result);
    }

    public function test_get_withNullData_rejectsAndRetries(): void
    {
        $messageData = [
            'id' => '1609459200000-3',
            'data' => null
        ];

        $this->connection->expects($this->exactly(2))
            ->method('get')
            ->willReturnOnConsecutiveCalls($messageData, null);

        $this->connection->expects($this->once())
            ->method('reject')
            ->with('1609459200000-3');

        $result = iterator_to_array($this->receiver->get());

        $this->assertEquals([], $result);
    }

    public function test_get_whenDecodingFails_rejectsMessageAndThrowsException(): void
    {
        $messageData = [
            'id' => '1609459200000-4',
            'data' => ['message' => '{"body":"test","headers":{}}']
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
            ->with('1609459200000-4');

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessage('Decoding failed');

        iterator_to_array($this->receiver->get());
    }

    public function test_ack_withValidEnvelope_acknowledgesMessage(): void
    {
        $envelope = new Envelope(new \stdClass(), [new RedisReceivedStamp('1609459200000-5')]);

        $this->connection->expects($this->once())
            ->method('ack')
            ->with('1609459200000-5');

        $this->receiver->ack($envelope);
    }

    public function test_ack_withoutRedisReceivedStamp_throwsLogicException(): void
    {
        $envelope = new Envelope(new \stdClass());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('No RedisReceivedStamp found on the Envelope.');

        $this->receiver->ack($envelope);
    }

    public function test_reject_withValidEnvelope_rejectsMessage(): void
    {
        $envelope = new Envelope(new \stdClass(), [new RedisReceivedStamp('1609459200000-6')]);

        $this->connection->expects($this->once())
            ->method('reject')
            ->with('1609459200000-6');

        $this->receiver->reject($envelope);
    }

    public function test_reject_withoutRedisReceivedStamp_throwsLogicException(): void
    {
        $envelope = new Envelope(new \stdClass());

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('No RedisReceivedStamp found on the Envelope.');

        $this->receiver->reject($envelope);
    }

    public function test_keepalive_updatesMessageState(): void
    {
        $envelope = new Envelope(new \stdClass(), [new RedisReceivedStamp('1609459200000-7')]);
        $seconds = 60;

        $this->connection->expects($this->once())
            ->method('keepalive')
            ->with('1609459200000-7', $seconds);

        $this->receiver->keepalive($envelope, $seconds);
    }

    public function test_getMessageCount_returnsConnectionMessageCount(): void
    {
        $expectedCount = 25;

        $this->connection->expects($this->once())
            ->method('getMessageCount')
            ->willReturn($expectedCount);

        $result = $this->receiver->getMessageCount();
        $this->assertEquals($expectedCount, $result);
    }

    public function test_get_withMissingMessageKey_returnsEmptyArray(): void
    {
        $messageData = [
            'id' => '1609459200000-8',
            'data' => ['other_key' => 'some_data'] // Missing 'message' key
        ];

        $this->connection->expects($this->once())
            ->method('get')
            ->willReturn($messageData);

        $result = iterator_to_array($this->receiver->get());

        $this->assertEquals([], $result);
    }

    protected function setUp(): void
    {
        $this->connection = $this->createMock(Connection::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->receiver = new RedisReceiver($this->connection, $this->serializer);
    }
}