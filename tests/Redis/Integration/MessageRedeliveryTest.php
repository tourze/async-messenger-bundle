<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;

/**
 * 集成测试：测试 Redis 消息重投递机制
 *
 * 本测试类主要验证消息重投递机制的正确性，包括：
 * - 未确认消息的自动重投递
 * - 多次重投递时消息的完整性
 * - Keepalive 防止消息被重投递
 * - 不同重投递超时配置的行为
 * - 并发场景下的重投递一致性
 * - 延迟消息和重投递消息的混合处理
 *
 * 注意：此集成测试通过 RedisTransport 间接测试 Connection 的重投递机制。
 * 专注于消息重投递场景的端到端验证。
 *
 * @internal
 */
#[CoversClass(RedisTransport::class)]
final class MessageRedeliveryTest extends TestCase
{
    private PhpSerializer $serializer;

    protected \Redis $redis;

    protected string $queueName = 'test_queue';

    protected string $delayedQueueName = 'test_queue_delayed';

    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new PhpSerializer();

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

    public function testAbandonedMessageIsRedelivered(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1, // 1秒后重投递
            'claim_interval' => 200, // 200ms 检查间隔
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'will be abandoned';
        $message->id = 'msg-001';
        $envelope = new Envelope($message, []);

        // Act
        $transport->send($envelope);

        // 第一个消费者获取消息但不处理
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages1 = iterator_to_array($consumer1->get());
        $this->assertCount(1, $messages1);
        $message = $messages1[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $message);
        $this->assertTrue(property_exists($message, 'content'));
        $this->assertEquals('will be abandoned', $message->content);

        // 不调用 ack，模拟消费者崩溃或处理失败
        // 等待重投递
        sleep(2);

        // 第二个消费者应该能获取到消息
        $consumer2 = new RedisTransport($connection, $this->serializer);
        $messages2 = iterator_to_array($consumer2->get());

        // Assert
        $this->assertCount(1, $messages2);
        $redeliveredMessage = $messages2[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $redeliveredMessage);
        $this->assertTrue(property_exists($redeliveredMessage, 'content'));
        $this->assertTrue(property_exists($redeliveredMessage, 'id'));
        $this->assertEquals('will be abandoned', $redeliveredMessage->content);
        $this->assertEquals('msg-001', $redeliveredMessage->id);

        // 清理
        $consumer2->ack($messages2[0]);
    }

    public function testMultipleRedeliveriesMaintainMessageIntegrity(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $originalMessage = new \stdClass();
        $originalMessage->content = 'test content';
        $originalMessage->data = ['key' => 'value', 'number' => 42];
        $originalMessage->id = 'unique-id-123';
        $envelope = new Envelope($originalMessage, []);

        // Act
        $transport->send($envelope);

        // 多次获取但不确认，模拟多次失败
        for ($i = 0; $i < 3; ++$i) {
            $consumer = new RedisTransport($connection, $this->serializer);
            $messages = iterator_to_array($consumer->get());

            $this->assertCount(1, $messages);
            $receivedMessage = $messages[0]->getMessage();

            // 验证消息完整性
            $this->assertInstanceOf(\stdClass::class, $receivedMessage);
            $this->assertTrue(property_exists($receivedMessage, 'content'));
            $this->assertTrue(property_exists($receivedMessage, 'data'));
            $this->assertTrue(property_exists($receivedMessage, 'id'));
            $this->assertEquals('test content', $receivedMessage->content);
            $this->assertEquals(['key' => 'value', 'number' => 42], $receivedMessage->data);
            $this->assertEquals('unique-id-123', $receivedMessage->id);

            // 不确认，等待重投递
            sleep(2);
        }

        // 最终成功处理
        $finalConsumer = new RedisTransport($connection, $this->serializer);
        $finalMessages = iterator_to_array($finalConsumer->get());
        $this->assertCount(1, $finalMessages);
        $finalConsumer->ack($finalMessages[0]);

        // Assert
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testKeepalivePreventsRedelivery(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 2, // 2秒超时
            'claim_interval' => 500,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'long processing';
        $envelope = new Envelope($message, []);

        // Act
        $transport->send($envelope);

        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages = iterator_to_array($consumer1->get());
        $this->assertCount(1, $messages);
        $processingMessage = $messages[0];

        // 模拟长时间处理（4秒），期间定期调用 keepalive
        for ($i = 0; $i < 4; ++$i) {
            sleep(1);
            $consumer1->keepalive($processingMessage);

            // 其他消费者不应该获取到消息
            $consumer2 = new RedisTransport($connection, $this->serializer);
            $otherMessages = iterator_to_array($consumer2->get());
            $this->assertEmpty($otherMessages);
        }

        // 完成处理
        $consumer1->ack($processingMessage);

        // Assert
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testRedeliveryTimeoutConfiguration(): void
    {
        // 测试不同的重投递超时配置
        $timeouts = [1, 3, 5]; // 秒

        foreach ($timeouts as $timeout) {
            // Arrange
            $options = array_merge($this->getConnectionOptions(), [
                'redeliver_timeout' => $timeout,
                'claim_interval' => 100,
                'queue' => "test_queue_timeout_{$timeout}",
                'delayed_queue' => "test_queue_timeout_{$timeout}_delayed",
            ]);
            $connection = new Connection($this->redis, $options);
            $transport = new RedisTransport($connection, $this->serializer);

            $message = new \stdClass();
            $message->content = "timeout test {$timeout}s";
            $envelope = new Envelope($message, []);

            // Act
            $transport->send($envelope);

            // 获取但不确认
            $consumer1 = new RedisTransport($connection, $this->serializer);
            $messages1 = iterator_to_array($consumer1->get());
            $this->assertCount(1, $messages1);

            // 在超时前尝试获取 - 应该为空
            sleep($timeout - 1);
            $consumer2 = new RedisTransport($connection, $this->serializer);
            $messages2 = iterator_to_array($consumer2->get());
            $this->assertEmpty($messages2, "Message should not be available before timeout ({$timeout}s)");

            // 等待超时后
            sleep(2);
            $messages3 = iterator_to_array($consumer2->get());
            $this->assertCount(1, $messages3, "Message should be redelivered after timeout ({$timeout}s)");
            $timeoutMessage = $messages3[0]->getMessage();
            $this->assertInstanceOf(\stdClass::class, $timeoutMessage);
            $this->assertTrue(property_exists($timeoutMessage, 'content'));
            $this->assertEquals("timeout test {$timeout}s", $timeoutMessage->content);

            // 清理
            $consumer2->ack($messages3[0]);
        }
    }

    public function testConcurrentRedeliveryMaintainsConsistency(): void
    {
        // Arrange
        $transport = $this->createTransportWithShortTimeout();
        $sentIds = $this->sendMultipleTestMessages($transport, 5);

        // Act
        $this->simulateConcurrentConsumersWithoutAck($transport);
        sleep(2); // 等待重投递
        $redeliveredIds = $this->collectRedeliveredMessages($transport);

        // Assert
        $this->assertRedeliveryConsistency($sentIds, $redeliveredIds, $transport);
        $this->assertNotEmpty($sentIds, 'Should have sent test messages');
        $this->assertCount(5, $sentIds, 'Should have sent exactly 5 messages');
    }

    private function createTransportWithShortTimeout(): RedisTransport
    {
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
        ]);
        $connection = new Connection($this->redis, $options);

        return new RedisTransport($connection, $this->serializer);
    }

    /**
     * @return list<string>
     */
    private function sendMultipleTestMessages(RedisTransport $transport, int $count): array
    {
        $sentIds = [];
        for ($i = 0; $i < $count; ++$i) {
            $message = new \stdClass();
            $message->content = "redelivery test {$i}";
            $message->id = "msg-{$i}";
            $transport->send(new Envelope($message, []));
            $sentIds[] = "msg-{$i}";
        }

        return $sentIds;
    }

    private function simulateConcurrentConsumersWithoutAck(RedisTransport $transport): void
    {
        $connection = $transport->getConnection();

        for ($i = 0; $i < 3; ++$i) {
            $consumer = new RedisTransport($connection, $this->serializer);
            $this->consumeMessagesWithoutAck($consumer, 2);
        }
    }

    private function consumeMessagesWithoutAck(RedisTransport $consumer, int $maxMessages): void
    {
        $messages = [];
        while (count($messages) < $maxMessages) {
            $batch = iterator_to_array($consumer->get());
            if ([] === $batch) {
                break;
            }
            foreach ($batch as $msg) {
                $messages[] = $msg;
                if (count($messages) >= $maxMessages) {
                    break;
                }
            }
        }
    }

    /**
     * @return list<string>
     */
    private function collectRedeliveredMessages(RedisTransport $transport): array
    {
        $connection = $transport->getConnection();
        $finalConsumer = new RedisTransport($connection, $this->serializer);
        /** @var list<string> $redeliveredIds */
        $redeliveredIds = [];

        while (true) {
            $messages = iterator_to_array($finalConsumer->get());
            if ([] === $messages) {
                break;
            }
            foreach ($messages as $msg) {
                $msgData = $msg->getMessage();
                $this->assertInstanceOf(\stdClass::class, $msgData);
                $this->assertTrue(property_exists($msgData, 'id'));
                /** @var string $id */
                $id = $msgData->id;
                $redeliveredIds[] = $id;
                $finalConsumer->ack($msg);
            }
        }

        return $redeliveredIds;
    }

    /**
     * @param list<string> $sentIds
     * @param list<string> $redeliveredIds
     */
    private function assertRedeliveryConsistency(array $sentIds, array $redeliveredIds, RedisTransport $transport): void
    {
        $this->assertCount(count($sentIds), $redeliveredIds);
        sort($sentIds);
        sort($redeliveredIds);
        $this->assertEquals($sentIds, $redeliveredIds);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testMixedDelayedAndRedeliveredMessages(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        // 发送立即消息
        $immediateMessage = new \stdClass();
        $immediateMessage->content = 'immediate';
        $transport->send(new Envelope($immediateMessage, []));

        // 发送延迟消息（2秒后）
        $delayedMessage = new \stdClass();
        $delayedMessage->content = 'delayed';
        $transport->send(new Envelope($delayedMessage, [new DelayStamp(2000)]));

        // Act
        // 获取立即消息但不确认（将被重投递）
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages1 = iterator_to_array($consumer1->get());
        $this->assertCount(1, $messages1);
        $immediateMsg = $messages1[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $immediateMsg);
        $this->assertTrue(property_exists($immediateMsg, 'content'));
        $this->assertEquals('immediate', $immediateMsg->content);

        // 等待1.5秒（重投递发生，但延迟消息还未到）
        usleep(1500000);

        // 新消费者应该获取重投递的消息
        $consumer2 = new RedisTransport($connection, $this->serializer);
        $messages2 = iterator_to_array($consumer2->get());
        $this->assertCount(1, $messages2);
        $redeliveredMsg = $messages2[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $redeliveredMsg);
        $this->assertTrue(property_exists($redeliveredMsg, 'content'));
        $this->assertEquals('immediate', $redeliveredMsg->content);
        $consumer2->ack($messages2[0]);

        // 再等待0.6秒（延迟消息应该可用了）
        usleep(600000);

        $messages3 = iterator_to_array($consumer2->get());
        $this->assertCount(1, $messages3);
        $delayedMsg = $messages3[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $delayedMsg);
        $this->assertTrue(property_exists($delayedMsg, 'content'));
        $this->assertEquals('delayed', $delayedMsg->content);
        $consumer2->ack($messages3[0]);

        // Assert
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testRedeliveryAfterConnectionLoss(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'connection loss test';
        $message->id = 'conn-loss-001';
        $envelope = new Envelope($message, []);

        // Act
        $transport->send($envelope);

        // 第一个消费者获取消息
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages1 = iterator_to_array($consumer1->get());
        $this->assertCount(1, $messages1);

        // 模拟连接丢失（不调用 ack 或 keepalive）
        // 等待超过重投递超时时间
        sleep(2);

        // 创建新的消费者（模拟重连后的消费者）
        $consumer2 = new RedisTransport($connection, $this->serializer);
        $messages2 = iterator_to_array($consumer2->get());

        // Assert
        $this->assertCount(1, $messages2);
        $connectionLossMsg = $messages2[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $connectionLossMsg);
        $this->assertTrue(property_exists($connectionLossMsg, 'content'));
        $this->assertTrue(property_exists($connectionLossMsg, 'id'));
        $this->assertEquals('connection loss test', $connectionLossMsg->content);
        $this->assertEquals('conn-loss-001', $connectionLossMsg->id);

        // 这次正确处理
        $consumer2->ack($messages2[0]);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testPartialRedeliveryBatch(): void
    {
        // Arrange
        $transport = $this->createTransportWithShortTimeout();
        $this->sendBatchMessages($transport, 5);

        // Act
        $result = $this->processPartialBatch($transport, 3);
        $this->assertPartialProcessingResults($result);

        sleep(2); // 等待重投递

        // Assert
        $redeliveredIds = $this->collectRedeliveredMessages($transport);
        $this->assertPartialRedeliveryResults($result['unacked'], $redeliveredIds, $transport);
        $this->assertGreaterThan(0, count($result['processed']), 'Should have processed some messages');
    }

    /**
     * @return array{processed: list<string>, unacked: list<string>}
     */
    private function processPartialBatch(RedisTransport $transport, int $processCount): array
    {
        $consumer = new RedisTransport($transport->getConnection(), $this->serializer);
        /** @var list<string> $processedIds */
        $processedIds = [];
        /** @var list<string> $unackedIds */
        $unackedIds = [];

        for ($i = 0; $i < 5; ++$i) {
            $messages = iterator_to_array($consumer->get());
            if ([] === $messages) {
                continue;
            }

            $msg = $messages[0];
            if ($i < $processCount) {
                $consumer->ack($msg);
                $processedMsgData = $msg->getMessage();
                $this->assertInstanceOf(\stdClass::class, $processedMsgData);
                $this->assertTrue(property_exists($processedMsgData, 'id'));
                /** @var string $processedId */
                $processedId = $processedMsgData->id;
                $processedIds[] = $processedId;
            } else {
                $unackedMsgData = $msg->getMessage();
                $this->assertInstanceOf(\stdClass::class, $unackedMsgData);
                $this->assertTrue(property_exists($unackedMsgData, 'id'));
                /** @var string $unackedId */
                $unackedId = $unackedMsgData->id;
                $unackedIds[] = $unackedId;
            }
        }

        return ['processed' => $processedIds, 'unacked' => $unackedIds];
    }

    /**
     * @param array{processed: list<string>, unacked: list<string>} $result
     */
    private function assertPartialProcessingResults(array $result): void
    {
        $this->assertCount(3, $result['processed']);
        $this->assertCount(2, $result['unacked']);
    }

    /**
     * @param list<string> $expectedUnacked
     * @param list<string> $actualRedelivered
     */
    private function assertPartialRedeliveryResults(array $expectedUnacked, array $actualRedelivered, RedisTransport $transport): void
    {
        sort($expectedUnacked);
        sort($actualRedelivered);
        $this->assertEquals($expectedUnacked, $actualRedelivered);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    /**
     * @return list<string>
     */
    private function sendBatchMessages(RedisTransport $transport, int $count): array
    {
        $messageIds = [];
        for ($i = 0; $i < $count; ++$i) {
            $message = new \stdClass();
            $message->content = "batch message {$i}";
            $message->id = "batch-{$i}";
            $transport->send(new Envelope($message, []));
            $messageIds[] = "batch-{$i}";
        }

        return $messageIds;
    }

    public function testRedeliveryWithDifferentConsumerGroups(): void
    {
        // Arrange - 创建两个不同的传输实例（模拟不同的消费者组）
        $options1 = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
            'queue' => 'group1_queue',
            'delayed_queue' => 'group1_queue_delayed',
        ]);

        $options2 = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 2, // 不同的重投递超时
            'claim_interval' => 200,
            'queue' => 'group2_queue',
            'delayed_queue' => 'group2_queue_delayed',
        ]);

        $connection1 = new Connection($this->redis, $options1);
        $transport1 = new RedisTransport($connection1, $this->serializer);

        $connection2 = new Connection($this->redis, $options2);
        $transport2 = new RedisTransport($connection2, $this->serializer);

        // 为每个组发送消息
        $message1 = new \stdClass();
        $message1->content = 'group1 message';
        $transport1->send(new Envelope($message1, []));

        $message2 = new \stdClass();
        $message2->content = 'group2 message';
        $transport2->send(new Envelope($message2, []));

        // Act - 每个组的消费者获取消息但不确认
        $consumer1 = new RedisTransport($connection1, $this->serializer);
        $consumer2 = new RedisTransport($connection2, $this->serializer);

        $messages1 = iterator_to_array($consumer1->get());
        $messages2 = iterator_to_array($consumer2->get());

        $this->assertCount(1, $messages1);
        $this->assertCount(1, $messages2);

        // 等待1.5秒（超过 group1 的超时，但未超过 group2 的超时）
        usleep(1500000);

        // Assert - group1 的消息应该被重投递，group2 的还没有
        $newConsumer1 = new RedisTransport($connection1, $this->serializer);
        $newConsumer2 = new RedisTransport($connection2, $this->serializer);

        $redelivered1 = iterator_to_array($newConsumer1->get());
        $redelivered2 = iterator_to_array($newConsumer2->get());

        $this->assertCount(1, $redelivered1);
        $group1Msg = $redelivered1[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $group1Msg);
        $this->assertTrue(property_exists($group1Msg, 'content'));
        $this->assertEquals('group1 message', $group1Msg->content);
        $this->assertEmpty($redelivered2);

        // 再等待1秒（总共2.5秒，超过 group2 的超时）
        sleep(1);

        $redelivered2Again = iterator_to_array($newConsumer2->get());
        $this->assertCount(1, $redelivered2Again);
        $group2Msg = $redelivered2Again[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $group2Msg);
        $this->assertTrue(property_exists($group2Msg, 'content'));
        $this->assertEquals('group2 message', $group2Msg->content);

        // 清理
        $newConsumer1->ack($redelivered1[0]);
        $newConsumer2->ack($redelivered2Again[0]);
    }

    public function testRedeliveryMessageIntegrityWithSerialization(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        // 创建包含复杂数据的消息
        $complexMessage = new \stdClass();
        $complexMessage->id = 'complex-001';
        $complexMessage->metadata = [
            'timestamp' => time(),
            'user_id' => 12345,
            'nested' => [
                'level1' => ['level2' => 'deep value'],
                'array' => [1, 2, 3, 4, 5],
            ],
        ];
        $complexMessage->binary_data = base64_encode(random_bytes(100));
        $complexMessage->unicode_text = '测试 Unicode 文本 🎉';

        $envelope = new Envelope($complexMessage, []);

        // Act
        $transport->send($envelope);

        // 多次重投递测试
        for ($attempt = 0; $attempt < 3; ++$attempt) {
            $consumer = new RedisTransport($connection, $this->serializer);
            $messages = iterator_to_array($consumer->get());

            $this->assertCount(1, $messages);
            $receivedMessage = $messages[0]->getMessage();
            $this->assertInstanceOf(\stdClass::class, $receivedMessage);

            // Assert - 验证消息完整性
            $this->assertTrue(property_exists($receivedMessage, 'id'));
            $this->assertTrue(property_exists($receivedMessage, 'metadata'));
            $this->assertTrue(property_exists($receivedMessage, 'binary_data'));
            $this->assertTrue(property_exists($receivedMessage, 'unicode_text'));

            $this->assertEquals('complex-001', $receivedMessage->id);
            $this->assertEquals($complexMessage->metadata, $receivedMessage->metadata);
            $this->assertEquals($complexMessage->binary_data, $receivedMessage->binary_data);
            $this->assertEquals($complexMessage->unicode_text, $receivedMessage->unicode_text);

            // 验证嵌套数据结构
            $this->assertIsArray($receivedMessage->metadata);
            $metadata = $receivedMessage->metadata;
            $this->assertArrayHasKey('nested', $metadata);
            $this->assertIsArray($metadata['nested']);
            $this->assertArrayHasKey('level1', $metadata['nested']);
            $this->assertIsArray($metadata['nested']['level1']);
            $this->assertEquals('deep value', $metadata['nested']['level1']['level2']);
            $this->assertEquals([1, 2, 3, 4, 5], $metadata['nested']['array']);

            if ($attempt < 2) {
                // 前两次不确认，等待重投递
                sleep(2);
            } else {
                // 最后一次确认处理
                $consumer->ack($messages[0]);
            }
        }

        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testKeepaliveFrequencyEffectOnRedelivery(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 3, // 3秒超时
            'claim_interval' => 100,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'keepalive frequency test';
        $envelope = new Envelope($message, []);

        // Act
        $transport->send($envelope);

        $consumer = new RedisTransport($connection, $this->serializer);
        $messages = iterator_to_array($consumer->get());
        $this->assertCount(1, $messages);
        $processingMessage = $messages[0];

        // 测试不同的 keepalive 频率
        $startTime = time();

        // 每500ms调用一次 keepalive，持续5秒
        while (time() - $startTime < 5) {
            usleep(500000); // 0.5秒
            $consumer->keepalive($processingMessage);

            // 验证消息没有被重投递
            $consumer2 = new RedisTransport($connection, $this->serializer);
            $otherMessages = iterator_to_array($consumer2->get());
            $this->assertEmpty($otherMessages, 'Message should not be redelivered during keepalive');
        }

        // 停止 keepalive，等待超时
        sleep(4);

        // Assert - 现在消息应该被重投递
        $consumer3 = new RedisTransport($connection, $this->serializer);
        $redeliveredMessages = iterator_to_array($consumer3->get());
        $this->assertCount(1, $redeliveredMessages);
        $keepaliveMsg = $redeliveredMessages[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $keepaliveMsg);
        $this->assertTrue(property_exists($keepaliveMsg, 'content'));
        $this->assertEquals('keepalive frequency test', $keepaliveMsg->content);

        // 清理
        $consumer3->ack($redeliveredMessages[0]);
    }

    public function testRedeliveryUnderHighLoad(): void
    {
        // Arrange
        $transport = $this->createTransportWithShortTimeout();
        $messageCount = 20;
        $sentIds = $this->sendHighLoadMessages($transport, $messageCount);

        // Act
        $unackedIds = $this->simulateHighLoadConsumption($transport, $messageCount);
        $this->assertCorrectMessageConsumption($messageCount, $unackedIds);

        sleep(2); // 等待重投递

        // Assert
        $redeliveredIds = $this->collectRedeliveredMessages($transport);
        $this->assertHighLoadRedelivery($sentIds, $redeliveredIds, $transport);
        $this->assertCount($messageCount, $sentIds, 'All messages should be sent');
    }

    /**
     * @return list<string>
     */
    private function sendHighLoadMessages(RedisTransport $transport, int $messageCount): array
    {
        $sentIds = [];
        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new \stdClass();
            $message->content = "high load message {$i}";
            $message->id = "load-{$i}";
            $transport->send(new Envelope($message, []));
            $sentIds[] = "load-{$i}";
        }

        return $sentIds;
    }

    /**
     * @return list<string>
     */
    private function simulateHighLoadConsumption(RedisTransport $transport, int $messageCount): array
    {
        $consumers = $this->createMultipleConsumers($transport, 5);
        /** @var list<string> $unackedMessages */
        $unackedMessages = [];

        foreach ($consumers as $consumer) {
            $unackedMessages = $this->consumeMessagesPerConsumer($consumer, $unackedMessages, $messageCount);
        }

        return $unackedMessages;
    }

    /**
     * @return array<int, RedisTransport>
     */
    private function createMultipleConsumers(RedisTransport $transport, int $consumerCount): array
    {
        $consumers = [];
        for ($i = 0; $i < $consumerCount; ++$i) {
            $consumers[$i] = new RedisTransport($transport->getConnection(), $this->serializer);
        }

        return $consumers;
    }

    /**
     * @param list<string> $unackedMessages
     * @return list<string>
     */
    private function consumeMessagesPerConsumer(RedisTransport $consumer, array $unackedMessages, int $messageCount): array
    {
        for ($j = 0; $j < 4 && count($unackedMessages) < $messageCount; ++$j) {
            $messages = iterator_to_array($consumer->get());
            if ([] === $messages) {
                continue;
            }
            $msgData = $messages[0]->getMessage();
            $this->assertInstanceOf(\stdClass::class, $msgData);
            $this->assertTrue(property_exists($msgData, 'id'));
            /** @var string $messageId */
            $messageId = $msgData->id;
            $unackedMessages[] = $messageId;
        }

        return $unackedMessages;
    }

    /**
     * @param list<string> $unackedMessages
     */
    private function assertCorrectMessageConsumption(int $expectedCount, array $unackedMessages): void
    {
        $this->assertCount($expectedCount, $unackedMessages);
    }

    /**
     * @param list<string> $sentIds
     * @param list<string> $redeliveredIds
     */
    private function assertHighLoadRedelivery(array $sentIds, array $redeliveredIds, RedisTransport $transport): void
    {
        $this->assertCount(count($sentIds), $redeliveredIds);
        sort($sentIds);
        sort($redeliveredIds);
        $this->assertEquals($sentIds, $redeliveredIds);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testRedeliveryInteractionWithDelayedMessages(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        // 发送立即消息
        $immediateMessage = new \stdClass();
        $immediateMessage->content = 'immediate for redelivery';
        $immediateMessage->type = 'immediate';
        $transport->send(new Envelope($immediateMessage, []));

        // 发送延迟消息（2秒后）
        $delayedMessage = new \stdClass();
        $delayedMessage->content = 'delayed message';
        $delayedMessage->type = 'delayed';
        $transport->send(new Envelope($delayedMessage, [new DelayStamp(2000)]));

        // Act - 获取立即消息但不确认
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages1 = iterator_to_array($consumer1->get());
        $this->assertCount(1, $messages1);
        $immediateTypeMsg = $messages1[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $immediateTypeMsg);
        $this->assertTrue(property_exists($immediateTypeMsg, 'type'));
        $this->assertEquals('immediate', $immediateTypeMsg->type);

        // 等待1.5秒（超过重投递超时，但延迟消息还未到期）
        usleep(1500000);

        // 应该得到重投递的立即消息
        $consumer2 = new RedisTransport($connection, $this->serializer);
        $messages2 = iterator_to_array($consumer2->get());
        $this->assertCount(1, $messages2);
        $redeliveredTypeMsg = $messages2[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $redeliveredTypeMsg);
        $this->assertTrue(property_exists($redeliveredTypeMsg, 'type'));
        $this->assertEquals('immediate', $redeliveredTypeMsg->type);
        $consumer2->ack($messages2[0]);

        // 再等待1秒（延迟消息应该到期了）
        sleep(1);

        // Assert - 应该得到延迟消息
        $messages3 = iterator_to_array($consumer2->get());
        $this->assertCount(1, $messages3);
        $delayedTypeMsg = $messages3[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $delayedTypeMsg);
        $this->assertTrue(property_exists($delayedTypeMsg, 'type'));
        $this->assertEquals('delayed', $delayedTypeMsg->type);
        $consumer2->ack($messages3[0]);

        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testAck(): void
    {
        $options = $this->getConnectionOptions();
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'test ack';
        $transport->send(new Envelope($message, []));

        // 获取消息
        $messages = iterator_to_array($transport->get());
        $this->assertCount(1, $messages);

        // 确认消息
        $transport->ack($messages[0]);

        // 验证消息已被删除
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testGet(): void
    {
        $options = $this->getConnectionOptions();
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'test get method';
        $transport->send(new Envelope($message, []));

        // 测试 get() 方法
        $messages = iterator_to_array($transport->get());
        $this->assertCount(1, $messages);
        $this->assertInstanceOf(Envelope::class, $messages[0]);

        $receivedMessage = $messages[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $receivedMessage);
        $this->assertTrue(property_exists($receivedMessage, 'content'));
        $this->assertEquals('test get method', $receivedMessage->content);

        // 清理
        $transport->ack($messages[0]);
    }

    public function testSend(): void
    {
        $options = $this->getConnectionOptions();
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'test send method';
        $envelope = new Envelope($message, []);

        // 测试 send() 方法
        $sentEnvelope = $transport->send($envelope);

        $this->assertInstanceOf(Envelope::class, $sentEnvelope);
        $this->assertEquals(1, $transport->getMessageCount());

        // 验证发送的消息可以被获取
        $messages = iterator_to_array($transport->get());
        $this->assertCount(1, $messages);
        $transport->ack($messages[0]);
    }

    public function testReject(): void
    {
        $options = $this->getConnectionOptions();
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'test reject';
        $transport->send(new Envelope($message, []));

        // 获取消息
        $messages = iterator_to_array($transport->get());
        $this->assertCount(1, $messages);

        // 拒绝消息
        $transport->reject($messages[0]);

        // 验证消息已被删除
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testSetup(): void
    {
        $options = $this->getConnectionOptions();
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        // 测试 setup() 方法
        $transport->setup();

        // 验证 setup 后可以正常发送和接收消息
        $message = new \stdClass();
        $message->content = 'test setup';
        $transport->send(new Envelope($message, []));

        $this->assertEquals(1, $transport->getMessageCount());

        // 清理
        $messages = iterator_to_array($transport->get());
        if (count($messages) > 0) {
            $transport->ack($messages[0]);
        }
    }

    public function testClose(): void
    {
        $options = $this->getConnectionOptions();
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'test close';
        $transport->send(new Envelope($message, []));

        // 测试 close() 方法
        $transport->close();

        // 关闭后连接应该被清理，但消息应该仍然存在
        // 创建新的传输实例验证
        $newConnection = new Connection($this->redis, $options);
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
        $options = $this->getConnectionOptions();
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        // 发送多条消息
        for ($i = 0; $i < 5; ++$i) {
            $message = new \stdClass();
            $message->content = "test cleanup {$i}";
            $transport->send(new Envelope($message, []));
        }

        $this->assertEquals(5, $transport->getMessageCount());

        // 测试 cleanup() 方法
        $transport->cleanup();

        // 验证所有消息已被清理
        $this->assertEquals(0, $transport->getMessageCount());
    }
}
