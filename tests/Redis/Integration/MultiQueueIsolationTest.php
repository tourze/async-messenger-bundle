<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;

class MultiQueueIsolationTest extends RedisIntegrationTestCase
{
    private PhpSerializer $serializer;
    
    public function test_queuesAreCompletelyIsolated(): void
    {
        // Arrange
        $queues = ['orders', 'emails', 'notifications'];
        $transports = [];
        $messagesByQueue = [];
        
        // 为每个队列创建 transport 并发送消息
        foreach ($queues as $queue) {
            $options = array_merge($this->getConnectionOptions(), [
                'queue' => $queue,
                'delayed_queue' => "{$queue}_delayed",
            ]);
            $connection = new Connection($this->redis, $options);
            $transport = new RedisTransport($connection, $this->serializer);
            $transports[$queue] = $transport;
            
            // 每个队列发送不同数量的消息
            $messageCount = count($queues) - array_search($queue, $queues);
            for ($i = 0; $i < $messageCount; $i++) {
                $message = new \stdClass();
                $message->queue = $queue;
                $message->content = "{$queue} message {$i}";
                $message->id = "{$queue}-{$i}";
                $transport->send(new Envelope($message));
                $messagesByQueue[$queue][] = "{$queue}-{$i}";
            }
        }
        
        // Act & Assert
        // 每个 transport 只能获取自己队列的消息
        foreach ($queues as $queue) {
            $transport = $transports[$queue];
            $receivedIds = [];
            
            while (true) {
                $messages = $transport->get();
                if (empty($messages)) {
                    break;
                }
                foreach ($messages as $msg) {
                    $this->assertEquals($queue, $msg->getMessage()->queue);
                    $receivedIds[] = $msg->getMessage()->id;
                    $transport->ack($msg);
                }
            }
            
            // 验证接收到的消息与发送的消息一致
            /** @var array $expectedIds */
            $expectedIds = $messagesByQueue[$queue];
            /** @var array $receivedIdsArray */
            $receivedIdsArray = $receivedIds;
            sort($expectedIds);
            sort($receivedIdsArray);
            $this->assertEquals($expectedIds, $receivedIdsArray);
            
            // 验证不能获取其他队列的消息
            $this->assertEquals(0, $transport->getMessageCount());
        }
    }
    
    public function test_delayedQueuesAreIsolated(): void
    {
        // Arrange
        $queues = ['queue_a', 'queue_b'];
        $transports = [];
        
        foreach ($queues as $queue) {
            $options = array_merge($this->getConnectionOptions(), [
                'queue' => $queue,
                'delayed_queue' => "{$queue}_delayed",
            ]);
            $connection = new Connection($this->redis, $options);
            $transport = new RedisTransport($connection, $this->serializer);
            $transports[$queue] = $transport;
        }
        
        // 发送延迟消息到不同队列
        $messageA = new \stdClass();
        $messageA->content = 'delayed A';
        $transports['queue_a']->send(new Envelope($messageA, [new DelayStamp(1000)]));
        
        $messageB = new \stdClass();
        $messageB->content = 'delayed B';
        $transports['queue_b']->send(new Envelope($messageB, [new DelayStamp(500)]));
        
        // Act
        // 立即检查 - 两个队列都应该为空
        $this->assertEmpty($transports['queue_a']->get());
        $this->assertEmpty($transports['queue_b']->get());
        
        // 等待 B 的延迟时间
        usleep(600000);
        
        // queue_b 应该有消息，queue_a 应该没有
        $messagesB = $transports['queue_b']->get();
        $this->assertNotEmpty($messagesB, 'Expected messages in queue_b but got none');
        $this->assertCount(1, $messagesB);
        $this->assertEquals('delayed B', $messagesB[0]->getMessage()->content);
        $transports['queue_b']->ack($messagesB[0]);
        
        $this->assertEmpty($transports['queue_a']->get());
        
        // 再等待 A 的延迟时间
        usleep(500000);
        
        // 现在 queue_a 应该有消息
        $messagesA = $transports['queue_a']->get();
        $this->assertNotEmpty($messagesA, 'Expected messages in queue_a but got none');
        $this->assertCount(1, $messagesA);
        $this->assertEquals('delayed A', $messagesA[0]->getMessage()->content);
        $transports['queue_a']->ack($messagesA[0]);
        
        // Assert - 两个队列都应该为空
        $this->assertEquals(0, $transports['queue_a']->getMessageCount());
        $this->assertEquals(0, $transports['queue_b']->getMessageCount());
    }
    
    public function test_concurrentOperationsOnMultipleQueues(): void
    {
        // Arrange
        $queues = ['high_priority', 'normal_priority', 'low_priority'];
        $transports = [];
        $connections = [];
        
        foreach ($queues as $queue) {
            $options = array_merge($this->getConnectionOptions(), [
                'queue' => $queue,
                'delayed_queue' => "{$queue}_delayed",
            ]);
            $connection = new Connection($this->redis, $options);
            $connections[$queue] = $connection;
            $transports[$queue] = new RedisTransport($connection, $this->serializer);
        }
        
        // Act - 并发发送消息到不同队列
        $totalMessages = 0;
        for ($i = 0; $i < 10; $i++) {
            foreach ($queues as $queue) {
                $message = new \stdClass();
                $message->content = "{$queue} concurrent {$i}";
                $message->priority = $queue;
                $transports[$queue]->send(new Envelope($message));
                $totalMessages++;
            }
        }
        
        // 并发消费
        $processedByQueue = [
            'high_priority' => 0,
            'normal_priority' => 0,
            'low_priority' => 0,
        ];
        
        // 模拟多轮并发消费
        for ($round = 0; $round < 20; $round++) {
            foreach ($queues as $queue) {
                $messages = $transports[$queue]->get();
                foreach ($messages as $msg) {
                    $this->assertEquals($queue, $msg->getMessage()->priority);
                    $processedByQueue[$queue]++;
                    $transports[$queue]->ack($msg);
                }
            }
        }
        
        // Assert
        foreach ($queues as $queue) {
            $this->assertEquals(10, $processedByQueue[$queue], 
                "Queue {$queue} should have processed exactly 10 messages");
            $this->assertEquals(0, $transports[$queue]->getMessageCount());
        }
        
        $this->assertEquals($totalMessages, array_sum($processedByQueue));
    }
    
    public function test_queueCleanupDoesNotAffectOtherQueues(): void
    {
        // Arrange
        $queues = ['queue_x', 'queue_y', 'queue_z'];
        $transports = [];
        
        foreach ($queues as $queue) {
            $options = array_merge($this->getConnectionOptions(), [
                'queue' => $queue,
                'delayed_queue' => "{$queue}_delayed",
            ]);
            $connection = new Connection($this->redis, $options);
            $transport = new RedisTransport($connection, $this->serializer);
            $transports[$queue] = $transport;
            
            // 发送消息到每个队列
            for ($i = 0; $i < 3; $i++) {
                $message = new \stdClass();
                $message->content = "{$queue} message {$i}";
                $transport->send(new Envelope($message));
            }
        }
        
        // Act - 清理 queue_y
        $transports['queue_y']->cleanup();
        
        // Assert
        // queue_y 应该为空
        $this->assertEquals(0, $transports['queue_y']->getMessageCount());
        
        // 其他队列应该保持不变
        $this->assertEquals(3, $transports['queue_x']->getMessageCount());
        $this->assertEquals(3, $transports['queue_z']->getMessageCount());
        
        // 验证可以正常消费其他队列的消息
        foreach (['queue_x', 'queue_z'] as $queue) {
            $consumedCount = 0;
            while (true) {
                $messages = $transports[$queue]->get();
                if (empty($messages)) {
                    break;
                }
                foreach ($messages as $msg) {
                    $this->assertStringContainsString($queue, $msg->getMessage()->content);
                    $transports[$queue]->ack($msg);
                    $consumedCount++;
                }
            }
            $this->assertEquals(3, $consumedCount);
        }
    }
    
    public function test_redeliveryIsolationAcrossQueues(): void
    {
        // Arrange
        $options1 = array_merge($this->getConnectionOptions(), [
            'queue' => 'queue_1',
            'delayed_queue' => 'queue_1_delayed',
            'redeliver_timeout' => 0.5,
            'claim_interval' => 100,
        ]);
        
        $options2 = array_merge($this->getConnectionOptions(), [
            'queue' => 'queue_2',
            'delayed_queue' => 'queue_2_delayed',
            'redeliver_timeout' => 1.5,
            'claim_interval' => 100,
        ]);
        
        $connection1 = new Connection($this->redis, $options1);
        $transport1 = new RedisTransport($connection1, $this->serializer);
        
        $connection2 = new Connection($this->redis, $options2);
        $transport2 = new RedisTransport($connection2, $this->serializer);
        
        // 发送消息
        $message1 = new \stdClass();
        $message1->content = 'queue 1 message';
        $transport1->send(new Envelope($message1));
        
        $message2 = new \stdClass();
        $message2->content = 'queue 2 message';
        $transport2->send(new Envelope($message2));
        
        // Act
        // 获取但不确认
        $consumer1 = new RedisTransport($connection1, $this->serializer);
        $consumer2 = new RedisTransport($connection2, $this->serializer);
        
        $messages1 = $consumer1->get();
        $messages2 = $consumer2->get();
        
        $this->assertCount(1, $messages1);
        $this->assertCount(1, $messages2);
        
        // 在 list-based 实现中，消息被 rPop 后就不在队列中了
        // 重投递需要显式地将消息重新加入队列（通过 reject 或超时机制）
        // 由于我们没有 ack 这些消息，它们在 processingMessages 中被跟踪
        
        // 等待超过 queue_1 的 redeliver_timeout
        usleep(700000); // 0.7秒
        
        // 触发重投递检查 - 需要调用 get() 来触发 claimOldPendingMessages
        $redelivered1 = $consumer1->get();
        $redelivered2 = $consumer2->get();
        
        // queue_1 的消息应该被重投递（因为超时了）
        // queue_2 的消息不应该被重投递（还没超时）
        $this->assertCount(1, $redelivered1, "Queue 1 message should be redelivered");
        $this->assertEmpty($redelivered2, "Queue 2 message should not be redelivered yet");
        
        // 清理
        $consumer1->ack($redelivered1[0]);
        
        // 再等待 queue_2 的重投递
        usleep(1000000); // 1秒，总计1.7秒，超过queue_2的1.5秒超时
        
        $redelivered2Again = $consumer2->get();
        $this->assertCount(1, $redelivered2Again, "Queue 2 message should now be redelivered");
        $consumer2->ack($redelivered2Again[0]);
    }
    
    public function test_messageCountsAreIsolated(): void
    {
        // Arrange
        $queues = [
            'queue_alpha' => 5,
            'queue_beta' => 3,
            'queue_gamma' => 7,
        ];
        
        $transports = [];
        
        foreach ($queues as $queue => $count) {
            $options = array_merge($this->getConnectionOptions(), [
                'queue' => $queue,
                'delayed_queue' => "{$queue}_delayed",
            ]);
            $connection = new Connection($this->redis, $options);
            $transport = new RedisTransport($connection, $this->serializer);
            $transports[$queue] = $transport;
            
            // 发送指定数量的消息
            for ($i = 0; $i < $count; $i++) {
                $message = new \stdClass();
                $message->content = "{$queue} {$i}";
                $transport->send(new Envelope($message));
            }
            
            // 发送一些延迟消息
            for ($i = 0; $i < 2; $i++) {
                $message = new \stdClass();
                $message->content = "{$queue} delayed {$i}";
                $transport->send(new Envelope($message, [new DelayStamp(10000)]));
            }
        }
        
        // Act & Assert
        // 验证每个队列的消息计数
        foreach ($queues as $queue => $expectedCount) {
            $actualCount = $transports[$queue]->getMessageCount();
            $expectedTotal = $expectedCount + 2; // 包括延迟消息
            $this->assertEquals($expectedTotal, $actualCount, 
                "Queue {$queue} should have {$expectedTotal} messages");
        }
        
        // 消费部分消息并重新验证计数
        foreach ($transports as $queue => $transport) {
            // 消费2个消息
            for ($i = 0; $i < 2; $i++) {
                $messages = $transport->get();
                if (!empty($messages)) {
                    $transport->ack($messages[0]);
                }
            }
            
            // 重新验证计数
            $newCount = $transport->getMessageCount();
            $expectedNewCount = $queues[$queue] - 2 + 2; // 减去消费的，加上延迟的
            $this->assertEquals($expectedNewCount, $newCount, 
                "Queue {$queue} should have {$expectedNewCount} messages after consumption");
        }
    }
    
    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new PhpSerializer();
    }
}