<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;

class ConcurrentConsumptionTest extends RedisIntegrationTestCase
{
    private PhpSerializer $serializer;
    
    public function test_multipleConsumers_processMessagesWithoutDuplication(): void
    {
        // Arrange
        $messageCount = 20;
        $connection = new Connection($this->redis, $this->getConnectionOptions());
        $transport = new RedisTransport($connection, $this->serializer);
        
        // 发送消息
        $sentIds = [];
        for ($i = 0; $i < $messageCount; $i++) {
            $message = new \stdClass();
            $message->content = "message {$i}";
            $message->id = $i;
            $envelope = new Envelope($message);
            $sentEnvelope = $transport->send($envelope);
            $sentIds[] = $i;
        }
        
        // Act - 模拟多个消费者并发消费
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $consumer2 = new RedisTransport($connection, $this->serializer);
        $consumer3 = new RedisTransport($connection, $this->serializer);
        
        $processedIds = [
            'consumer1' => [],
            'consumer2' => [],
            'consumer3' => [],
        ];
        
        // 并发消费所有消息
        $totalProcessed = 0;
        while ($totalProcessed < $messageCount) {
            // Consumer 1
            $messages1 = $consumer1->get();
            foreach ($messages1 as $msg) {
                $processedIds['consumer1'][] = $msg->getMessage()->id;
                $consumer1->ack($msg);
                $totalProcessed++;
            }
            
            // Consumer 2
            $messages2 = $consumer2->get();
            foreach ($messages2 as $msg) {
                $processedIds['consumer2'][] = $msg->getMessage()->id;
                $consumer2->ack($msg);
                $totalProcessed++;
            }
            
            // Consumer 3
            $messages3 = $consumer3->get();
            foreach ($messages3 as $msg) {
                $processedIds['consumer3'][] = $msg->getMessage()->id;
                $consumer3->ack($msg);
                $totalProcessed++;
            }
            
            // 如果没有消息了，避免无限循环
            if (empty($messages1) && empty($messages2) && empty($messages3)) {
                break;
            }
        }
        
        // Assert
        // 验证所有消息都被处理了
        $allProcessedIds = array_merge(
            $processedIds['consumer1'],
            $processedIds['consumer2'],
            $processedIds['consumer3']
        );
        
        $this->assertCount($messageCount, $allProcessedIds);
        /** @var array $sentIdsArray */
        $sentIdsArray = $sentIds;
        /** @var array $allProcessedIdsArray */
        $allProcessedIdsArray = $allProcessedIds;
        sort($sentIdsArray);
        sort($allProcessedIdsArray);
        $this->assertEquals($sentIdsArray, $allProcessedIdsArray);
        
        // 验证没有重复处理
        $this->assertCount($messageCount, array_unique($allProcessedIds));
        
        // 验证队列为空
        $this->assertEquals(0, $transport->getMessageCount());
        
        // 记录各消费者处理的消息数
        foreach ($processedIds as $consumer => $ids) {
            $count = count($ids);
            $this->assertGreaterThan(0, $count, "{$consumer} should process at least one message");
        }
    }
    
    public function test_consumerFailure_messagesAreNotLost(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1, // 1秒后重投递
            'claim_interval' => 100, // 100ms 检查间隔
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);
        
        // 发送消息
        $message = new \stdClass();
        $message->content = 'important message';
        $message->id = 'test-123';
        $envelope = new Envelope($message);
        $transport->send($envelope);
        
        // Act
        // Consumer 1 获取消息但不处理（模拟崩溃）
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages = $consumer1->get();
        $this->assertCount(1, $messages);
        $this->assertEquals('important message', $messages[0]->getMessage()->content);
        
        // 不调用 ack 或 reject，模拟消费者崩溃
        // 等待重投递超时
        sleep(2);
        
        // Consumer 2 应该能获取到相同的消息
        $consumer2 = new RedisTransport($connection, $this->serializer);
        $redeliveredMessages = $consumer2->get();
        
        // Assert
        $this->assertCount(1, $redeliveredMessages);
        $this->assertEquals('important message', $redeliveredMessages[0]->getMessage()->content);
        $this->assertEquals('test-123', $redeliveredMessages[0]->getMessage()->id);
        
        // 这次正确处理消息
        $consumer2->ack($redeliveredMessages[0]);
        
        // 验证消息已被处理
        $this->assertEquals(0, $transport->getMessageCount());
    }
    
    public function test_keepaliveWithConcurrentConsumers(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 2, // 2秒后重投递
            'claim_interval' => 500, // 500ms 检查间隔
        ]);
        $connection = new Connection($this->redis, $options);
        
        // 发送一个需要长时间处理的消息
        $message = new \stdClass();
        $message->content = 'long processing task';
        $message->processingTime = 3; // 需要3秒处理
        
        $transport = new RedisTransport($connection, $this->serializer);
        $transport->send(new Envelope($message));
        
        // Act
        // Consumer 1 获取消息并持续 keepalive
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages = $consumer1->get();
        $this->assertCount(1, $messages);
        $processingMessage = $messages[0];
        
        // 模拟长时间处理，期间调用 keepalive
        $processingComplete = false;
        $startTime = time();
        
        while (!$processingComplete) {
            sleep(1);
            $consumer1->keepalive($processingMessage);
            
            if (time() - $startTime >= 3) {
                $processingComplete = true;
            }
            
            // Consumer 2 尝试获取消息（不应该获取到）
            $consumer2 = new RedisTransport($connection, $this->serializer);
            $messages2 = $consumer2->get();
            $this->assertEmpty($messages2, "Consumer 2 should not get the message being processed");
        }
        
        // 完成处理
        $consumer1->ack($processingMessage);
        
        // Assert
        $this->assertEquals(0, $transport->getMessageCount());
    }
    
    public function test_concurrentProcessingOfDelayedMessages(): void
    {
        // Arrange
        $connection = new Connection($this->redis, $this->getConnectionOptions());
        $transport = new RedisTransport($connection, $this->serializer);
        
        // 发送多个延迟消息
        $delayedMessages = [];
        for ($i = 0; $i < 5; $i++) {
            $message = new \stdClass();
            $message->content = "delayed message {$i}";
            $message->id = $i;
            $delayStamp = new \Symfony\Component\Messenger\Stamp\DelayStamp(500); // 0.5秒延迟
            $envelope = new Envelope($message, [$delayStamp]);
            $transport->send($envelope);
            $delayedMessages[] = $i;
        }
        
        // 立即检查 - 应该没有可用消息
        $immediateCheck = $transport->get();
        $this->assertEmpty($immediateCheck);
        
        // 等待延迟时间
        usleep(600000); // 0.6秒
        
        // Act - 多个消费者并发获取延迟消息
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $consumer2 = new RedisTransport($connection, $this->serializer);
        
        $processedByConsumer1 = [];
        $processedByConsumer2 = [];
        
        // 并发消费
        for ($i = 0; $i < 10; $i++) { // 多次尝试以确保获取所有消息
            $messages1 = $consumer1->get();
            foreach ($messages1 as $msg) {
                $processedByConsumer1[] = $msg->getMessage()->id;
                $consumer1->ack($msg);
            }
            
            $messages2 = $consumer2->get();
            foreach ($messages2 as $msg) {
                $processedByConsumer2[] = $msg->getMessage()->id;
                $consumer2->ack($msg);
            }
            
            if (count($processedByConsumer1) + count($processedByConsumer2) >= 5) {
                break;
            }
        }
        
        // Assert
        $allProcessed = array_merge($processedByConsumer1, $processedByConsumer2);
        $this->assertCount(5, $allProcessed);
        /** @var array $delayedMessagesArray */
        $delayedMessagesArray = $delayedMessages;
        /** @var array $allProcessedArray */
        $allProcessedArray = $allProcessed;
        sort($delayedMessagesArray);
        sort($allProcessedArray);
        $this->assertEquals($delayedMessagesArray, $allProcessedArray);
        
        // 验证没有重复
        $this->assertCount(5, array_unique($allProcessed));
        
        // 两个消费者都应该处理了一些消息
        $this->assertNotEmpty($processedByConsumer1, "Consumer 1 should process some messages");
        $this->assertNotEmpty($processedByConsumer2, "Consumer 2 should process some messages");
        
        // 验证队列为空
        $this->assertEquals(0, $transport->getMessageCount());
    }
    
    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new PhpSerializer();
    }
}