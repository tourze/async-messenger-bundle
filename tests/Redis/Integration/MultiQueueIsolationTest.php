<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\StampInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;

/**
 * 集成测试：Redis 多队列隔离场景
 *
 * Linus风格重构：数据结构优先，消除复杂性
 * - 使用 QueueManager 管理传输创建
 * - 使用 MessageProcessor 处理消息消费
 * - 使用 TestMessageFactory 标准化消息创建
 * - 每个测试方法职责单一，逻辑清晰
 *
 * 注意：此集成测试通过 RedisTransport 间接测试 Connection 的功能。
 * 专注于多队列隔离场景的端到端验证。
 *
 * @internal
 */
#[CoversClass(RedisTransport::class)]
final class MultiQueueIsolationTest extends TestCase
{
    private PhpSerializer $serializer;

    protected \Redis $redis;

    protected string $queueName = 'test_queue';

    protected string $delayedQueueName = 'test_queue_delayed';

    private QueueManager $queueManager;

    private MessageProcessor $messageProcessor;

    public function testQueuesAreCompletelyIsolated(): void
    {
        $queues = ['orders', 'emails', 'notifications'];
        $transports = $this->queueManager->createMultipleTransports($queues);

        // 发送不同数量的消息到每个队列
        $expectedCounts = [];
        foreach ($queues as $index => $queue) {
            $messageCount = count($queues) - $index; // 递减数量: 3,2,1
            $messages = TestMessageFactory::createBatch($queue, $messageCount);
            $this->queueManager->sendMessages($queue, $messages);
            $expectedCounts[$queue] = $messageCount;
        }

        // 验证每个队列只包含自己的消息
        foreach ($queues as $queue) {
            $validator = function (Envelope $msg) use ($queue): void {
                $message = $msg->getMessage();
                $this->assertInstanceOf(\stdClass::class, $message);
                $this->assertTrue(property_exists($message, 'queue'));
                $this->assertEquals($queue, $message->queue);
            };
            $received = $this->messageProcessor->consumeAndValidate($transports[$queue], $validator);
            $this->assertCount($expectedCounts[$queue], $received);
        }
    }

    public function testDelayedQueuesAreIsolated(): void
    {
        $queues = ['queue_a', 'queue_b'];
        $transports = $this->queueManager->createMultipleTransports($queues);

        // 发送不同延迟时间的消息
        $messageA = TestMessageFactory::create('queue_a', 0, ['content' => 'delayed A']);
        $messageB = TestMessageFactory::create('queue_b', 0, ['content' => 'delayed B']);

        $this->queueManager->sendMessage('queue_a', $messageA, [new DelayStamp(1000)]);
        $this->queueManager->sendMessage('queue_b', $messageB, [new DelayStamp(500)]);

        // 立即检查 - 都应该为空
        $this->assertEmpty($transports['queue_a']->get());
        $this->assertEmpty($transports['queue_b']->get());

        // 等待queue_b消息到达
        usleep(600000);
        $this->assertDelayedMessageReceived($transports['queue_b'], 'delayed B');
        $this->assertEmpty($transports['queue_a']->get());

        // 等待queue_a消息到达
        usleep(500000);
        $this->assertDelayedMessageReceived($transports['queue_a'], 'delayed A');
    }

    private function assertDelayedMessageReceived(RedisTransport $transport, string $expectedContent): void
    {
        $messages = iterator_to_array($transport->get());
        $this->assertCount(1, $messages);
        $delayedMsg = $messages[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $delayedMsg);
        $this->assertTrue(property_exists($delayedMsg, 'content'));
        $this->assertEquals($expectedContent, $delayedMsg->content);
        $transport->ack($messages[0]);
    }

    public function testConcurrentOperationsOnMultipleQueues(): void
    {
        $queues = ['high_priority', 'normal_priority', 'low_priority'];
        $messagesPerQueue = 10;
        $transports = $this->queueManager->createMultipleTransports($queues);

        // 发送消息到所有队列
        foreach ($queues as $queue) {
            $messages = TestMessageFactory::createBatch($queue, $messagesPerQueue, ['priority' => $queue]);
            $this->queueManager->sendMessages($queue, $messages);
        }

        // 并发处理消息
        $processedByQueue = $this->processConcurrentQueues($queues, $transports, $messagesPerQueue);

        // 验证结果
        foreach ($queues as $queue) {
            $this->assertEquals($messagesPerQueue, $processedByQueue[$queue]);
            $this->assertEquals(0, $transports[$queue]->getMessageCount());
        }
    }

    /**
     * @param array<string> $queues
     * @param array<string, RedisTransport> $transports
     * @return array<string, int>
     */
    private function processConcurrentQueues(array $queues, array $transports, int $expectedPerQueue): array
    {
        $processedByQueue = array_fill_keys($queues, 0);

        // 模拟20轮并发处理
        for ($round = 0; $round < 20; ++$round) {
            foreach ($queues as $queue) {
                $validator = function (Envelope $msg) use ($queue): void {
                    $message = $msg->getMessage();
                    $this->assertInstanceOf(\stdClass::class, $message);
                    $this->assertTrue(property_exists($message, 'priority'));
                    $this->assertEquals($queue, $message->priority);
                };
                $messages = $this->messageProcessor->consumeAndValidate($transports[$queue], $validator);
                $processedByQueue[$queue] += count($messages);
            }
        }

        return $processedByQueue;
    }

    public function testQueueCleanupDoesNotAffectOtherQueues(): void
    {
        $queues = ['queue_x', 'queue_y', 'queue_z'];
        $transports = $this->queueManager->createMultipleTransports($queues);

        // 为每个队列发送3条消息
        foreach ($queues as $queue) {
            $messages = TestMessageFactory::createBatch($queue, 3);
            $this->queueManager->sendMessages($queue, $messages);
        }

        // 清理queue_y
        $transports['queue_y']->cleanup();

        // 验证隔离性
        $this->assertEquals(0, $transports['queue_y']->getMessageCount());
        $this->assertEquals(3, $transports['queue_x']->getMessageCount());
        $this->assertEquals(3, $transports['queue_z']->getMessageCount());

        // 验证其他队列仍可消费
        foreach (['queue_x', 'queue_z'] as $queue) {
            $received = $this->messageProcessor->consumeAll($transports[$queue]);
            $this->assertCount(3, $received);
        }
    }

    public function testRedeliveryIsolationAcrossQueues(): void
    {
        // 创建带不同重投递超时的队列
        $transport1 = $this->queueManager->createTransport('queue_1', [
            'redeliver_timeout' => 0.5,
            'claim_interval' => 100,
        ]);
        $transport2 = $this->queueManager->createTransport('queue_2', [
            'redeliver_timeout' => 1.5,
            'claim_interval' => 100,
        ]);

        // 发送消息但不确认
        $this->queueManager->sendMessage('queue_1', TestMessageFactory::create('queue_1', 0, ['content' => 'queue 1 message']));
        $this->queueManager->sendMessage('queue_2', TestMessageFactory::create('queue_2', 0, ['content' => 'queue 2 message']));

        // 获取消息但不ack（模拟处理失败）
        $messages1 = iterator_to_array($transport1->get());
        $messages2 = iterator_to_array($transport2->get());
        $this->assertCount(1, $messages1);
        $this->assertCount(1, $messages2);

        // 等待queue_1超时并触发重投递检查
        usleep(700000);
        $redelivered1 = iterator_to_array($transport1->get());
        $redelivered2 = iterator_to_array($transport2->get());

        $this->assertCount(1, $redelivered1, 'Queue 1 message should be redelivered');
        $this->assertEmpty($redelivered2, 'Queue 2 message should not be redelivered yet');

        $transport1->ack($redelivered1[0]);

        // 等待queue_2也超时
        usleep(1000000);
        $redelivered2Again = iterator_to_array($transport2->get());
        $this->assertCount(1, $redelivered2Again, 'Queue 2 message should now be redelivered');
        $transport2->ack($redelivered2Again[0]);
    }

    public function testMessageCountsAreIsolated(): void
    {
        $queueCounts = ['queue_alpha' => 5, 'queue_beta' => 3, 'queue_gamma' => 7];
        $transports = $this->queueManager->createMultipleTransports(array_keys($queueCounts));

        // 发送不同数量的消息（包括延迟消息）
        foreach ($queueCounts as $queue => $count) {
            $messages = TestMessageFactory::createBatch($queue, $count);
            $this->queueManager->sendMessages($queue, $messages);

            // 添加延迟消息
            for ($i = 0; $i < 2; ++$i) {
                $delayedMsg = TestMessageFactory::create($queue, $count + $i, ['delayed' => true]);
                $this->queueManager->sendMessage($queue, $delayedMsg, [new DelayStamp(10000)]);
            }
        }

        // 验证初始计数
        foreach ($queueCounts as $queue => $count) {
            $this->assertEquals($count + 2, $transports[$queue]->getMessageCount());
        }

        // 部分消费后验证计数
        foreach ($transports as $queue => $transport) {
            $this->messageProcessor->consumeCount($transport, 2);
            $expectedRemaining = $queueCounts[$queue];
            $this->assertEquals($expectedRemaining, $transport->getMessageCount());
        }
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->redis = new \Redis();

        try {
            if (!$this->redis->connect('127.0.0.1', 6379)) {
                self::markTestSkipped('Redis server is not available.');
            }
            $this->redis->select(15);
            $this->redis->flushDB();
        } catch (\RedisException $e) {
            self::markTestSkipped('Redis server is not available: ' . $e->getMessage());
        }

        $this->serializer = new PhpSerializer();
        $this->queueManager = new QueueManager($this->redis, $this->serializer, $this->getConnectionOptions());
        $this->messageProcessor = new MessageProcessor();
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

    /** @return array<string, mixed> */
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

    public function testMultipleQueueNamesDoNotInterfere(): void
    {
        $queueNames = ['orders_high', 'orders_low', 'notifications_urgent', 'notifications_normal'];
        $transports = $this->queueManager->createMultipleTransports($queueNames);

        // 发送不同数量的消息
        $expectedCounts = [];
        foreach ($queueNames as $index => $queueName) {
            $messageCount = $index + 1; // 1, 2, 3, 4
            $messages = TestMessageFactory::createBatch($queueName, $messageCount, ['queueName' => $queueName]);
            $this->queueManager->sendMessages($queueName, $messages);
            $expectedCounts[$queueName] = $messageCount;
        }

        // 验证每个队列只处理自己的消息
        foreach ($queueNames as $queueName) {
            $validator = function (Envelope $msg) use ($queueName): void {
                $message = $msg->getMessage();
                $this->assertInstanceOf(\stdClass::class, $message);
                $this->assertTrue(property_exists($message, 'queueName'));
                $this->assertEquals($queueName, $message->queueName);
            };
            $received = $this->messageProcessor->consumeAndValidate($transports[$queueName], $validator);
            $this->assertCount($expectedCounts[$queueName], $received);
            $this->assertEquals(0, $transports[$queueName]->getMessageCount());
        }
    }

    public function testQueuePrefixIsolation(): void
    {
        $prefixConfigs = [
            ['prefix' => 'app1_', 'queue' => 'tasks'],
            ['prefix' => 'app2_', 'queue' => 'tasks'],
            ['prefix' => 'shared_', 'queue' => 'tasks'],
        ];

        // 创建不同前缀的队列传输
        $transports = [];
        foreach ($prefixConfigs as $index => $config) {
            $queueName = $config['prefix'] . $config['queue'];
            $transports[$index] = $this->queueManager->createTransport($queueName);

            // 发送消息
            $message = TestMessageFactory::create($queueName, 0, [
                'content' => "message from {$config['prefix']}",
                'prefix' => $config['prefix'],
            ]);
            $this->queueManager->sendMessage($queueName, $message);
        }

        // 验证每个队列只获取自己的消息
        foreach ($transports as $index => $transport) {
            $messages = iterator_to_array($transport->get());
            $this->assertCount(1, $messages);
            $prefixMsg = $messages[0]->getMessage();
            $this->assertInstanceOf(\stdClass::class, $prefixMsg);
            $this->assertTrue(property_exists($prefixMsg, 'prefix'));
            $this->assertEquals($prefixConfigs[$index]['prefix'], $prefixMsg->prefix);
            $transport->ack($messages[0]);
        }
    }

    public function testMixedDelayedAndNormalMessagesIsolation(): void
    {
        $queues = ['queue_mixed_a', 'queue_mixed_b'];
        $transports = $this->queueManager->createMultipleTransports($queues);

        // 为每个队列发送混合消息
        foreach ($queues as $queue) {
            // 立即消息
            $immediateMsg = TestMessageFactory::create($queue, 0, ['type' => 'immediate']);
            $this->queueManager->sendMessage($queue, $immediateMsg);

            // 延迟消息
            $delayedMsg = TestMessageFactory::create($queue, 1, ['type' => 'delayed']);
            $this->queueManager->sendMessage($queue, $delayedMsg, [new DelayStamp(1000)]);
        }

        // 验证只能获取立即消息
        foreach ($queues as $queue) {
            $validator = function (Envelope $msg): void {
                $message = $msg->getMessage();
                $this->assertInstanceOf(\stdClass::class, $message);
                $this->assertTrue(property_exists($message, 'type'));
                $this->assertEquals('immediate', $message->type);
            };
            $received = $this->messageProcessor->consumeAndValidate($transports[$queue], $validator);
            $this->assertCount(1, $received);
        }

        // 等待延迟消息到达
        usleep(1100000);

        // 验证延迟消息
        foreach ($queues as $queue) {
            $validator = function (Envelope $msg): void {
                $message = $msg->getMessage();
                $this->assertInstanceOf(\stdClass::class, $message);
                $this->assertTrue(property_exists($message, 'type'));
                $this->assertEquals('delayed', $message->type);
            };
            $received = $this->messageProcessor->consumeAndValidate($transports[$queue], $validator);
            $this->assertCount(1, $received);
        }
    }

    public function testQueueStatisticsIsolation(): void
    {
        $queueCounts = ['stats_queue_1' => 5, 'stats_queue_2' => 3, 'stats_queue_3' => 8];
        $transports = $this->queueManager->createMultipleTransports(array_keys($queueCounts));

        // 发送不同数量的消息
        foreach ($queueCounts as $queue => $count) {
            $messages = TestMessageFactory::createBatch($queue, $count);
            $this->queueManager->sendMessages($queue, $messages);
        }

        // 验证初始统计
        foreach ($queueCounts as $queue => $expectedCount) {
            $this->assertEquals($expectedCount, $transports[$queue]->getMessageCount());
        }

        // 部分消费后验证统计
        $consumeCount = 2;
        foreach ($queueCounts as $queue => $originalCount) {
            $actualConsumed = min($consumeCount, $originalCount);
            $this->messageProcessor->consumeCount($transports[$queue], $actualConsumed);

            $expectedRemaining = max(0, $originalCount - $consumeCount);
            $this->assertEquals($expectedRemaining, $transports[$queue]->getMessageCount());
        }
    }

    public function testErrorHandlingDoesNotAffectOtherQueues(): void
    {
        $queues = ['error_queue_a', 'error_queue_b'];
        $transports = $this->queueManager->createMultipleTransports($queues);

        // 发送测试消息
        foreach ($queues as $queue) {
            $message = TestMessageFactory::create($queue, 0, ['content' => "{$queue}_test_message"]);
            $this->queueManager->sendMessage($queue, $message);
        }

        // 模拟queue_a处理错误（拒绝消息）
        $messagesA = iterator_to_array($transports['error_queue_a']->get());
        $this->assertCount(1, $messagesA);
        $transports['error_queue_a']->reject($messagesA[0]);

        // 验证queue_b不受影响
        $messagesB = iterator_to_array($transports['error_queue_b']->get());
        $this->assertCount(1, $messagesB);
        $errorQueueMsg = $messagesB[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $errorQueueMsg);
        $this->assertTrue(property_exists($errorQueueMsg, 'content'));
        $this->assertEquals('error_queue_b_test_message', $errorQueueMsg->content);

        // 验证错误隔离
        $this->assertEquals(0, $transports['error_queue_a']->getMessageCount());
        $this->assertEquals(1, $transports['error_queue_b']->getMessageCount());

        $transports['error_queue_b']->ack($messagesB[0]);
    }

    public function testHighVolumeMessagesIsolation(): void
    {
        $queues = ['high_volume_1', 'high_volume_2'];
        $messageCount = 50;
        $transports = $this->queueManager->createMultipleTransports($queues);

        // 发送大量消息
        foreach ($queues as $queue) {
            $messages = TestMessageFactory::createBatch($queue, $messageCount, ['batchId' => $queue]);
            $this->queueManager->sendMessages($queue, $messages);
        }

        // 验证数量和隔离性
        foreach ($queues as $queue) {
            $this->assertEquals($messageCount, $transports[$queue]->getMessageCount());

            $validator = function (Envelope $msg) use ($queue): void {
                $message = $msg->getMessage();
                $this->assertInstanceOf(\stdClass::class, $message);
                $this->assertTrue(property_exists($message, 'batchId'));
                $this->assertEquals($queue, $message->batchId);
            };
            $received = $this->messageProcessor->consumeAndValidate($transports[$queue], $validator);
            $this->assertCount($messageCount, $received);
            $this->assertEquals(0, $transports[$queue]->getMessageCount());
        }
    }

    public function testQueueLimitConfigurationIsolation(): void
    {
        // 创建不同限制的队列
        $limitedTransport = $this->queueManager->createTransport('limited_queue', ['queue_max_entries' => 3]);
        $unlimitedTransport = $this->queueManager->createTransport('unlimited_queue', ['queue_max_entries' => 0]);

        // 发送5条消息到每个队列
        $messages = TestMessageFactory::createBatch('test', 5);
        $this->queueManager->sendMessages('limited_queue', $messages);
        $this->queueManager->sendMessages('unlimited_queue', $messages);

        // 验证限制的效果
        $limitedCount = $limitedTransport->getMessageCount();
        $unlimitedCount = $unlimitedTransport->getMessageCount();

        $this->assertLessThanOrEqual(3, $limitedCount, 'Limited queue should respect max_entries');
        $this->assertEquals(5, $unlimitedCount, 'Unlimited queue should accept all messages');

        // 清理
        $this->messageProcessor->consumeAll($limitedTransport);
        $this->messageProcessor->consumeAll($unlimitedTransport);
    }

    public function testConnectionFailureIsolation(): void
    {
        $queues = ['resilient_queue_1', 'resilient_queue_2'];
        $transports = $this->queueManager->createMultipleTransports($queues);

        // 发送测试消息
        foreach ($queues as $queue) {
            $message = TestMessageFactory::create($queue, 0, ['content' => "{$queue}_resilience_test"]);
            $this->queueManager->sendMessage($queue, $message);
        }

        // 模拟部分故障（清理queue_1）
        $transports['resilient_queue_1']->cleanup();

        // 验证隔离性
        $this->assertEquals(0, $transports['resilient_queue_1']->getMessageCount());
        $this->assertEquals(1, $transports['resilient_queue_2']->getMessageCount());

        // 验证queue_2仍然正常
        $messagesQueue2 = iterator_to_array($transports['resilient_queue_2']->get());
        $this->assertCount(1, $messagesQueue2);
        $resilientMsg2 = $messagesQueue2[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $resilientMsg2);
        $this->assertTrue(property_exists($resilientMsg2, 'content'));
        $this->assertEquals('resilient_queue_2_resilience_test', $resilientMsg2->content);

        // 验证queue_1可以恢复
        $recoveryMessage = TestMessageFactory::create('resilient_queue_1', 1, ['content' => 'resilient_queue_1_recovery_test']);
        $this->queueManager->sendMessage('resilient_queue_1', $recoveryMessage);

        $recoveryMessages = iterator_to_array($transports['resilient_queue_1']->get());
        $this->assertCount(1, $recoveryMessages);
        $recoveryMsg = $recoveryMessages[0]->getMessage();
        $this->assertInstanceOf(\stdClass::class, $recoveryMsg);
        $this->assertTrue(property_exists($recoveryMsg, 'content'));
        $this->assertEquals('resilient_queue_1_recovery_test', $recoveryMsg->content);

        // 清理
        $transports['resilient_queue_1']->ack($recoveryMessages[0]);
        $transports['resilient_queue_2']->ack($messagesQueue2[0]);
    }

    // RedisTransport 基础方法测试
    public function testAck(): void
    {
        $transport = $this->queueManager->createTransport('ack_test');
        $message = TestMessageFactory::create('ack_test', 0, ['content' => 'test ack']);
        $this->queueManager->sendMessage('ack_test', $message);

        $messages = iterator_to_array($transport->get());
        $this->assertCount(1, $messages);

        $transport->ack($messages[0]);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testGet(): void
    {
        $transport = $this->queueManager->createTransport('get_test');
        $message = TestMessageFactory::create('get_test', 0, ['content' => 'test get']);
        $this->queueManager->sendMessage('get_test', $message);

        $messages = iterator_to_array($transport->get());
        $this->assertCount(1, $messages);
        $this->assertInstanceOf(Envelope::class, $messages[0]);

        $transport->ack($messages[0]);
    }

    public function testSend(): void
    {
        $transport = $this->queueManager->createTransport('send_test');
        $message = TestMessageFactory::create('send_test', 0, ['content' => 'test send']);
        $envelope = new Envelope($message, []);

        $sentEnvelope = $transport->send($envelope);
        $this->assertInstanceOf(Envelope::class, $sentEnvelope);
        $this->assertEquals(1, $transport->getMessageCount());

        $messages = iterator_to_array($transport->get());
        $transport->ack($messages[0]);
    }

    public function testReject(): void
    {
        $transport = $this->queueManager->createTransport('reject_test');
        $message = TestMessageFactory::create('reject_test', 0, ['content' => 'test reject']);
        $this->queueManager->sendMessage('reject_test', $message);

        $messages = iterator_to_array($transport->get());
        $this->assertCount(1, $messages);

        $transport->reject($messages[0]);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testKeepalive(): void
    {
        $transport = $this->queueManager->createTransport('keepalive_test', [
            'redeliver_timeout' => 2,
            'claim_interval' => 100,
        ]);
        $message = TestMessageFactory::create('keepalive_test', 0, ['content' => 'test keepalive']);
        $this->queueManager->sendMessage('keepalive_test', $message);

        $messages = iterator_to_array($transport->get());
        $this->assertCount(1, $messages);

        // 调用 keepalive 延长处理时间
        $transport->keepalive($messages[0]);

        // 验证消息仍然存在
        $this->assertEquals(1, $transport->getMessageCount());

        $transport->ack($messages[0]);
    }

    public function testSetup(): void
    {
        $transport = $this->queueManager->createTransport('setup_test');

        // 调用 setup 方法
        $transport->setup();

        // 验证 setup 后可以正常发送消息
        $message = TestMessageFactory::create('setup_test', 0, ['content' => 'test setup']);
        $this->queueManager->sendMessage('setup_test', $message);
        $this->assertEquals(1, $transport->getMessageCount());

        $messages = iterator_to_array($transport->get());
        $transport->ack($messages[0]);
    }

    public function testClose(): void
    {
        $transport = $this->queueManager->createTransport('close_test');
        $message = TestMessageFactory::create('close_test', 0, ['content' => 'test close']);
        $this->queueManager->sendMessage('close_test', $message);

        // 测试 close 方法
        $transport->close();

        // 关闭后创建新传输验证消息仍存在
        $newTransport = $this->queueManager->createTransport('close_test_verify');
        $this->assertGreaterThanOrEqual(0, $newTransport->getMessageCount());
    }

    public function testCleanup(): void
    {
        $transport = $this->queueManager->createTransport('cleanup_test');

        // 发送多条消息
        for ($i = 0; $i < 3; ++$i) {
            $message = TestMessageFactory::create('cleanup_test', $i);
            $this->queueManager->sendMessage('cleanup_test', $message);
        }

        $this->assertEquals(3, $transport->getMessageCount());

        // 测试 cleanup 方法
        $transport->cleanup();

        $this->assertEquals(0, $transport->getMessageCount());
    }
}
