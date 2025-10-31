<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;
use Tourze\AsyncMessengerBundle\Stamp\RedisReceivedStamp;
use Tourze\AsyncMessengerBundle\Tests\Redis\Integration\Fixtures\ComplexTestMessage;
use Tourze\AsyncMessengerBundle\Tests\Redis\Integration\Fixtures\LargeTestMessage;
use Tourze\AsyncMessengerBundle\Tests\Redis\Integration\Fixtures\TestMessage;

/**
 * 集成测试：RedisTransport 功能验证
 *
 * 注意：此集成测试专注于核心功能场景，通过集成方式验证RedisTransport的基础能力。
 *
 * @internal
 */
#[CoversClass(RedisTransport::class)]
final class RedisTransportIntegrationTest extends TestCase
{
    private RedisTransport $transport;

    private Connection $connection;

    private PhpSerializer $serializer;

    protected \Redis $redis;

    protected string $queueName = 'test_queue';

    protected string $delayedQueueName = 'test_queue_delayed';

    public function testSendAndReceiveCompleteMessageLifecycle(): void
    {
        // Arrange
        $message = new TestMessage();
        $message->content = 'test content';
        $message->id = uniqid();
        $envelope = new Envelope($message, []);

        // Act - 发送消息
        $sentEnvelope = $this->transport->send($envelope);

        // Assert - 验证发送结果
        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $this->assertNotNull($transportIdStamp);
        $this->assertNotEmpty($transportIdStamp->getId());

        // Act - 接收消息
        $receivedEnvelopes = $this->transport->get();

        // Assert - 验证接收结果
        $receivedEnvelopesArray = iterator_to_array($receivedEnvelopes);
        $this->assertCount(1, $receivedEnvelopesArray);
        $receivedEnvelope = $receivedEnvelopesArray[0];

        $this->assertInstanceOf(Envelope::class, $receivedEnvelope);
        $receivedMessage = $receivedEnvelope->getMessage();
        $this->assertTrue(property_exists($receivedMessage, 'content'));
        $this->assertTrue(property_exists($receivedMessage, 'id'));
        /** @var TestMessage $receivedMessage */
        $this->assertEquals($message->content, $receivedMessage->content);
        $this->assertEquals($message->id, $receivedMessage->id);

        // 验证 stamps
        $receivedStamp = $receivedEnvelope->last(RedisReceivedStamp::class);
        $this->assertNotNull($receivedStamp);
        $this->assertEquals($transportIdStamp->getId(), $receivedStamp->getId());

        // Act - 确认消息
        $this->transport->ack($receivedEnvelope);

        // Assert - 验证消息已被处理
        $this->assertEquals(0, $this->transport->getMessageCount());
    }

    public function testSendWithDelayDelaysMessageDelivery(): void
    {
        // Arrange
        $message = new TestMessage();
        $message->content = 'delayed message';
        $message->id = uniqid();
        $delayInSeconds = 2;
        $envelope = new Envelope($message, [new DelayStamp($delayInSeconds * 1000)]);

        // Act - 发送延迟消息
        $this->transport->send($envelope);

        // Assert - 立即获取应该返回空
        $immediateResult = $this->transport->get();
        $this->assertEmpty($immediateResult);

        // 等待延迟时间
        sleep($delayInSeconds + 1);

        // Assert - 延迟后应该能获取到消息
        $delayedResult = $this->transport->get();
        $delayedResultArray = iterator_to_array($delayedResult);
        $this->assertCount(1, $delayedResultArray);
        $delayedMessage = $delayedResultArray[0]->getMessage();
        /** @var TestMessage $delayedMessage */
        $this->assertTrue(property_exists($delayedMessage, 'content'));
        $this->assertEquals('delayed message', $delayedMessage->content);
    }

    public function testRejectRemovesMessageWithoutProcessing(): void
    {
        // Arrange
        $message = new TestMessage();
        $message->content = 'to be rejected';
        $message->id = uniqid();
        $envelope = new Envelope($message, []);

        // Act
        $this->transport->send($envelope);
        $receivedEnvelopes = $this->transport->get();
        $receivedEnvelopesArray = iterator_to_array($receivedEnvelopes);
        $this->assertCount(1, $receivedEnvelopesArray);

        $this->transport->reject($receivedEnvelopesArray[0]);

        // Assert
        $this->assertEquals(0, $this->transport->getMessageCount());
        $afterReject = $this->transport->get();
        $this->assertEmpty($afterReject);
    }

    public function testKeepalivePreventsMessageRedelivery(): void
    {
        // Arrange - 设置短的重投递超时
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1, // 1秒
            'claim_interval' => 100, // 0.1秒检查间隔
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new TestMessage();
        $message->content = 'long processing';
        $message->id = uniqid();
        $envelope = new Envelope($message, []);

        // Act
        $transport->send($envelope);
        $receivedEnvelopes = $transport->get();
        $receivedEnvelopesArray = iterator_to_array($receivedEnvelopes);
        $this->assertCount(1, $receivedEnvelopesArray);
        $receivedEnvelope = $receivedEnvelopesArray[0];

        // 等待足够长的时间，但通过keepalive保持消息活跃
        usleep(600000); // 0.6秒
        $transport->keepalive($receivedEnvelope);
        usleep(600000); // 再0.6秒，总共1.2秒，超过了redeliver_timeout

        // Assert - keepalive 更新了 timestamp，所以消息不会被重投递
        $secondGet = $transport->get();
        $this->assertSame([], iterator_to_array($secondGet));

        // 清理
        $transport->ack($receivedEnvelope);
    }

    public function testGetMessageCountReturnsCorrectCount(): void
    {
        // Arrange
        $messages = [];
        for ($i = 0; $i < 5; ++$i) {
            $message = new TestMessage();
            $message->content = "message {$i}";
            $message->id = "msg-{$i}";
            $messages[] = new Envelope($message, []);
        }

        // Act - 发送消息
        foreach ($messages as $envelope) {
            $this->transport->send($envelope);
        }

        // Assert
        $this->assertEquals(5, $this->transport->getMessageCount());

        // Act - 接收并确认部分消息
        $received = $this->transport->get();
        $receivedArray = iterator_to_array($received);
        $this->transport->ack($receivedArray[0]);

        // Assert
        $this->assertEquals(4, $this->transport->getMessageCount());
    }

    public function testMultipleGetReturnsAllPendingMessages(): void
    {
        // Arrange
        $messageCount = 3;
        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new TestMessage();
            $message->content = "message {$i}";
            $message->id = "multi-msg-{$i}";
            $this->transport->send(new Envelope($message, []));
        }

        // Act - 获取所有消息
        $allMessages = [];
        while (true) {
            $messages = $this->transport->get();
            if ([] === $messages) {
                break;
            }
            foreach ($messages as $msg) {
                $allMessages[] = $msg;
            }
            if (count($allMessages) >= $messageCount) {
                break;
            }
        }

        // Assert
        $this->assertCount($messageCount, $allMessages);

        // 验证消息内容（注意：Redis 使用 LIFO 顺序）
        $contents = array_map(function ($envelope) {
            $message = $envelope->getMessage();
            /** @var TestMessage $message */
            $this->assertTrue(property_exists($message, 'content'));

            return $message->content;
        }, $allMessages);

        $this->assertContains('message 0', $contents);
        $this->assertContains('message 1', $contents);
        $this->assertContains('message 2', $contents);

        // 清理
        foreach ($allMessages as $msg) {
            $this->transport->ack($msg);
        }
    }

    public function testMessageWithIdCanBeProcessed(): void
    {
        // Arrange
        $message = new TestMessage();
        $message->content = 'findable message';
        $message->id = uniqid();
        $sentEnvelope = $this->transport->send(new Envelope($message, []));

        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        if (null !== $transportIdStamp) {
            $messageId = $transportIdStamp->getId();
        } else {
            self::fail('TransportMessageIdStamp not found');
        }

        // Act - 获取消息并验证 ID
        $messages = $this->transport->get();
        $messagesArray = iterator_to_array($messages);
        $this->assertCount(1, $messagesArray);
        $receivedEnvelope = $messagesArray[0];

        // Assert
        $receivedStamp = $receivedEnvelope->last(RedisReceivedStamp::class);
        $this->assertNotNull($receivedStamp);
        $this->assertEquals($messageId, $receivedStamp->getId());
        $receivedMessage = $receivedEnvelope->getMessage();
        /** @var TestMessage $receivedMessage */
        $this->assertTrue(property_exists($receivedMessage, 'content'));
        $this->assertEquals('findable message', $receivedMessage->content);

        // 清理
        $this->transport->ack($receivedEnvelope);
    }

    public function testSetupExecutesSuccessfully(): void
    {
        // Arrange
        $connection = $this->transport->getConnection();

        // Act - setup 方法应该正常执行而不抛出异常
        $this->transport->setup();

        // Assert - 验证 setup 执行后传输仍然可以正常工作
        $message = new TestMessage();
        $message->content = 'setup test message';
        $envelope = new Envelope($message, []);

        $sentEnvelope = $this->transport->send($envelope);
        $this->assertInstanceOf(Envelope::class, $sentEnvelope);

        $receivedEnvelopes = $this->transport->get();
        $receivedEnvelopesArray = iterator_to_array($receivedEnvelopes);
        $this->assertCount(1, $receivedEnvelopesArray);
        $message = $receivedEnvelopesArray[0]->getMessage();
        $this->assertTrue(property_exists($message, 'content'));
        $this->assertEquals('setup test message', $message->content);

        $this->transport->ack($receivedEnvelopesArray[0]);
        $this->assertEquals(0, $this->transport->getMessageCount());
    }

    protected function setUp(): void
    {
        parent::setUp();
        // 创建 Redis 连接
        $this->redis = new \Redis();

        try {
            // 尝试连接到本地 Redis
            if (!$this->redis->connect('127.0.0.1', 6379)) {
                self::markTestSkipped('Redis server is not available.');
            }

            // 使用独立的测试数据库
            $this->redis->select(15);

            // 清理测试数据
            $this->redis->flushDB();
        } catch (\RedisException $e) {
            self::markTestSkipped('Redis server is not available: ' . $e->getMessage());
        }

        $this->connection = new Connection($this->redis, $this->getConnectionOptions());
        $this->serializer = new PhpSerializer();

        $this->transport = new RedisTransport($this->connection, $this->serializer);
    }

    protected function tearDown(): void
    {
        // 清理测试数据
        try {
            $this->redis->flushDB();
            $this->redis->close();
        } catch (\RedisException $e) {
            // Ignore errors during cleanup
        }

        parent::tearDown();
    }

    /**
     * @return array<string, mixed>
     */
    private function getConnectionOptions(): array
    {
        return [
            'queue' => $this->queueName,
            'delayed_queue' => $this->delayedQueueName,
            'redeliver_timeout' => 3600,
            'claim_interval' => 60000,
            'auto_setup' => false,
            'queue_max_entries' => 0,
        ];
    }

    public function testMultipleSendersCanSendConcurrently(): void
    {
        // Arrange
        $messageCount = 10;
        $envelopes = [];

        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new TestMessage();
            $message->content = "concurrent message {$i}";
            $envelopes[] = new Envelope($message, []);
        }

        // Act - 发送所有消息
        $sentIds = [];
        foreach ($envelopes as $envelope) {
            $sentEnvelope = $this->transport->send($envelope);
            $stamp = $sentEnvelope->last(TransportMessageIdStamp::class);
            if (null !== $stamp) {
                $sentIds[] = $stamp->getId();
            }
        }

        // Assert
        $this->assertCount($messageCount, array_unique($sentIds)); // 所有 ID 都是唯一的
        $this->assertEquals($messageCount, $this->transport->getMessageCount());

        // 验证所有消息都可以被接收
        $receivedCount = 0;
        while (true) {
            $messages = $this->transport->get();
            $messagesArray = iterator_to_array($messages);
            if ([] === $messagesArray) {
                break;
            }
            $receivedCount += count($messagesArray);
            foreach ($messages as $message) {
                $this->transport->ack($message);
            }
        }

        $this->assertEquals($messageCount, $receivedCount);
    }

    public function testDelayedAndNormalMessagesProcessedInCorrectOrder(): void
    {
        // Arrange
        // 发送立即消息
        $immediateMessage = new TestMessage();
        $immediateMessage->content = 'immediate';
        $immediateMessage->id = uniqid();
        $this->transport->send(new Envelope($immediateMessage, []));

        // 发送延迟消息（1秒后）
        $delayedMessage1 = new TestMessage();
        $delayedMessage1->content = 'delayed 1 second';
        $delayedMessage1->id = uniqid();
        $this->transport->send(new Envelope($delayedMessage1, [new DelayStamp(1000)]));

        // 发送另一个立即消息
        $immediateMessage2 = new TestMessage();
        $immediateMessage2->content = 'immediate 2';
        $immediateMessage2->id = uniqid();
        $this->transport->send(new Envelope($immediateMessage2, []));

        // 发送延迟消息（0.5秒后）
        $delayedMessage2 = new TestMessage();
        $delayedMessage2->content = 'delayed 0.5 second';
        $delayedMessage2->id = uniqid();
        $this->transport->send(new Envelope($delayedMessage2, [new DelayStamp(500)]));

        // Act & Assert
        // 立即获取 - 应该得到两个立即消息
        $immediateResults = [];
        while (true) {
            $envelopes = $this->transport->get();
            if ([] === $envelopes) {
                break;
            }
            foreach ($envelopes as $envelope) {
                /** @var TestMessage $msg */
                $msg = $envelope->getMessage();
                $immediateResults[] = $msg->content;
                $this->transport->ack($envelope);
            }
        }

        $this->assertCount(2, $immediateResults);
        $this->assertContains('immediate', $immediateResults);
        $this->assertContains('immediate 2', $immediateResults);

        // 等待0.6秒
        usleep(600000);

        // 应该得到0.5秒的延迟消息
        $delayedResults1 = $this->transport->get();
        $delayedResults1Array = iterator_to_array($delayedResults1);
        $this->assertCount(1, $delayedResults1Array);
        /** @var TestMessage $msg1 */
        $msg1 = $delayedResults1Array[0]->getMessage();
        $this->assertEquals('delayed 0.5 second', $msg1->content);
        $this->transport->ack($delayedResults1Array[0]);

        // 等待另外0.5秒
        usleep(500000);

        // 应该得到1秒的延迟消息
        $delayedResults2 = $this->transport->get();
        $delayedResults2Array = iterator_to_array($delayedResults2);
        $this->assertCount(1, $delayedResults2Array);
        /** @var TestMessage $msg2 */
        $msg2 = $delayedResults2Array[0]->getMessage();
        $this->assertEquals('delayed 1 second', $msg2->content);
        $this->transport->ack($delayedResults2Array[0]);

        // 确保没有更多消息
        $this->assertEquals(0, $this->transport->getMessageCount());
    }

    private function assertMessageInQueue(string $queueName, int $expectedCount): void
    {
        $actualCount = $this->redis->lLen($queueName);
        $this->assertEquals($expectedCount, $actualCount, "Expected {$expectedCount} messages in queue {$queueName}, but found {$actualCount}");
    }

    private function assertMessageInDelayedQueue(string $queueName, int $expectedCount): void
    {
        $actualCount = $this->redis->zCard($queueName);
        $this->assertEquals($expectedCount, $actualCount, "Expected {$expectedCount} messages in delayed queue {$queueName}, but found {$actualCount}");
    }

    public function testTransportCleanupRemovesAllData(): void
    {
        // Arrange
        // 发送立即消息
        for ($i = 0; $i < 3; ++$i) {
            $message = new TestMessage();
            $message->content = "cleanup test {$i}";
            $message->id = "cleanup-{$i}";
            $this->transport->send(new Envelope($message, []));
        }

        // 发送延迟消息
        for ($i = 0; $i < 2; ++$i) {
            $message = new TestMessage();
            $message->content = "delayed cleanup test {$i}";
            $message->id = "delayed-cleanup-{$i}";
            $this->transport->send(new Envelope($message, [new DelayStamp(10000)]));
        }

        // 验证消息已发送
        $this->assertEquals(5, $this->transport->getMessageCount());
        $this->assertMessageInQueue($this->queueName, 3);
        $this->assertMessageInDelayedQueue($this->delayedQueueName, 2);

        // Act
        $this->transport->cleanup();

        // Assert
        $this->assertEquals(0, $this->transport->getMessageCount());
        $this->assertMessageInQueue($this->queueName, 0);
        $this->assertMessageInDelayedQueue($this->delayedQueueName, 0);

        // 验证清理后传输仍然可以正常工作
        $newMessage = new TestMessage();
        $newMessage->content = 'post cleanup message';
        $newMessage->id = uniqid();
        $this->transport->send(new Envelope($newMessage, []));

        $receivedMessages = $this->transport->get();
        $receivedMessagesArray = iterator_to_array($receivedMessages);
        $this->assertCount(1, $receivedMessagesArray);
        /** @var TestMessage $postCleanupMsg */
        $postCleanupMsg = $receivedMessagesArray[0]->getMessage();
        $this->assertEquals('post cleanup message', $postCleanupMsg->content);
        $this->transport->ack($receivedMessagesArray[0]);
    }

    public function testTransportWithCustomSerializationFormat(): void
    {
        // Arrange - 创建包含复杂数据结构的消息
        $complexMessage = new ComplexTestMessage();
        $complexMessage->id = 'custom-serialization-001';
        $complexMessage->timestamp = (float) time();
        $complexMessage->metadata = [
            'user' => ['id' => 123, 'name' => 'Test User'],
            'tags' => ['urgent', 'customer-service'],
            'nested' => [
                'level1' => [
                    'level2' => ['data' => 'deep nested value'],
                    'array' => [1, 2, 3, 4, 5],
                ],
            ],
        ];
        $complexMessage->binary_data = base64_encode(random_bytes(50));
        $complexMessage->unicode_content = '测试中文内容 🚀 emoji support';

        $envelope = new Envelope($complexMessage, []);

        // Act
        $sentEnvelope = $this->transport->send($envelope);
        $receivedEnvelopes = $this->transport->get();

        // Assert
        $receivedEnvelopesArray = iterator_to_array($receivedEnvelopes);
        $this->assertCount(1, $receivedEnvelopesArray);
        $receivedMessage = $receivedEnvelopesArray[0]->getMessage();
        /** @var ComplexTestMessage $receivedMessage */

        // 验证序列化和反序列化的完整性
        $this->assertEquals($complexMessage->id, $receivedMessage->id);
        $this->assertEquals($complexMessage->timestamp, $receivedMessage->timestamp);
        $this->assertEquals($complexMessage->metadata, $receivedMessage->metadata);
        $this->assertEquals($complexMessage->binary_data, $receivedMessage->binary_data);
        $this->assertEquals($complexMessage->unicode_content, $receivedMessage->unicode_content);

        // 验证嵌套数据的完整性
        /** @var array<string, mixed> $metadata */
        $metadata = $receivedMessage->metadata;
        $this->assertIsArray($metadata);
        $this->assertArrayHasKey('nested', $metadata);
        $this->assertIsArray($metadata['nested']);
        $this->assertArrayHasKey('level1', $metadata['nested']);
        $this->assertIsArray($metadata['nested']['level1']);
        $this->assertArrayHasKey('level2', $metadata['nested']['level1']);
        $this->assertIsArray($metadata['nested']['level1']['level2']);
        $this->assertArrayHasKey('data', $metadata['nested']['level1']['level2']);
        $this->assertEquals('deep nested value', $metadata['nested']['level1']['level2']['data']);
        $this->assertArrayHasKey('array', $metadata['nested']['level1']);
        $this->assertEquals([1, 2, 3, 4, 5], $metadata['nested']['level1']['array']);

        // 清理
        $this->transport->ack($receivedEnvelopesArray[0]);
    }

    public function testTransportPerformanceWithLargeMessages(): void
    {
        // Arrange - 创建大消息
        $largeMessage = new LargeTestMessage();
        $largeMessage->id = 'large-message-001';
        $largeMessage->large_content = str_repeat('This is a large message content. ', 1000); // ~30KB
        $largeMessage->large_array = array_fill(0, 500, 'array item'); // 大数组
        $largeMessage->metadata = [
            'size' => 'large',
            'test_type' => 'performance',
            'created_at' => microtime(true),
        ];

        $envelope = new Envelope($largeMessage, []);
        $startTime = microtime(true);

        // Act - 发送大消息
        $sentEnvelope = $this->transport->send($envelope);
        $sendTime = microtime(true) - $startTime;

        $getStartTime = microtime(true);
        $receivedEnvelopes = $this->transport->get();
        $getTime = microtime(true) - $getStartTime;

        // Assert
        $receivedEnvelopesArray = iterator_to_array($receivedEnvelopes);
        $this->assertCount(1, $receivedEnvelopesArray);
        $receivedMessage = $receivedEnvelopesArray[0]->getMessage();
        /** @var LargeTestMessage $receivedMessage */

        // 验证消息完整性
        $this->assertEquals('large-message-001', $receivedMessage->id);
        $this->assertEquals($largeMessage->large_content, $receivedMessage->large_content);
        $this->assertCount(500, (array) $receivedMessage->large_array);
        /** @var array<string, mixed> $metadata */
        $metadata = $receivedMessage->metadata;
        $this->assertEquals('large', $metadata['size']);

        // 验证性能（大消息处理应该在合理时间内完成）
        $this->assertLessThan(1.0, $sendTime, 'Large message send should complete within 1 second');
        $this->assertLessThan(1.0, $getTime, 'Large message retrieval should complete within 1 second');

        // 验证消息大小符合预期（至少30KB）
        $serializedSize = strlen(serialize($largeMessage));
        $this->assertGreaterThan(30000, $serializedSize, 'Message should be at least 30KB');

        // 清理
        $this->transport->ack($receivedEnvelopesArray[0]);
    }

    public function testAck(): void
    {
        // Arrange
        $message = new TestMessage();
        $message->content = 'test ack method';
        $message->id = uniqid();
        $envelope = new Envelope($message, []);

        // Act
        $this->transport->send($envelope);
        $receivedEnvelopes = $this->transport->get();
        $receivedEnvelopesArray = iterator_to_array($receivedEnvelopes);
        $this->assertCount(1, $receivedEnvelopesArray);

        // 测试 ack 方法
        $this->transport->ack($receivedEnvelopesArray[0]);

        // Assert
        $this->assertEquals(0, $this->transport->getMessageCount());
    }

    public function testClose(): void
    {
        // Arrange
        $message = new TestMessage();
        $message->content = 'test close method';
        $message->id = uniqid();
        $this->transport->send(new Envelope($message, []));

        // Act - 测试 close 方法
        $this->transport->close();

        // Assert - 关闭后消息仍然存在（close 只关闭连接）
        // 创建新的传输实例来验证
        $newConnection = new Connection($this->redis, $this->getConnectionOptions());
        $newTransport = new RedisTransport($newConnection, $this->serializer);
        $this->assertGreaterThan(0, $newTransport->getMessageCount());

        // 清理
        $messages = iterator_to_array($newTransport->get());
        if (count($messages) > 0) {
            $newTransport->ack($messages[0]);
        }
    }

    public function testCleanup(): void
    {
        // Arrange
        $messageCount = 5;
        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new TestMessage();
            $message->content = "cleanup test {$i}";
            $message->id = "cleanup-{$i}";
            $this->transport->send(new Envelope($message, []));
        }

        $this->assertEquals($messageCount, $this->transport->getMessageCount());

        // Act - 测试 cleanup 方法
        $this->transport->cleanup();

        // Assert - cleanup 后消息应该被清理
        $this->assertEquals(0, $this->transport->getMessageCount());
    }
}
