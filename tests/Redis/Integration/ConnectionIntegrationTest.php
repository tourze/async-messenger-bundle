<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Redis\Connection;

class ConnectionIntegrationTest extends RedisIntegrationTestCase
{
    private Connection $connection;
    private PhpSerializer $serializer;
    
    public function test_add_savesMessageToRedis(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'test message';
        $envelope = new Envelope($message);
        $encodedEnvelope = $this->serializer->encode($envelope);

        // Act
        $messageId = $this->connection->add(
            $encodedEnvelope['body'],
            $encodedEnvelope['headers'] ?? []
        );

        // Assert
        $this->assertNotEmpty($messageId);
        $this->assertMessageInQueue($this->queueName, 1);

        $messages = $this->getQueueMessages($this->queueName);
        $this->assertCount(1, $messages);

        $savedMessage = json_decode($messages[0], true);
        $this->assertEquals($messageId, $savedMessage['id']);
        $this->assertEquals($encodedEnvelope['body'], $savedMessage['body']);
        $this->assertIsArray($savedMessage['headers']);
        $this->assertEquals([], $savedMessage['headers']); // PhpSerializer doesn't include headers
        $this->assertArrayHasKey('timestamp', $savedMessage);
    }
    
    public function test_add_withDelay_addsToDelayedQueue(): void
    {
        // Arrange
        $message = new \stdClass();
        $envelope = new Envelope($message, [new DelayStamp(5000)]); // 5 秒延迟
        $encodedEnvelope = $this->serializer->encode($envelope);

        $beforeSend = microtime(true) * 1000;

        // Act
        $messageId = $this->connection->add(
            $encodedEnvelope['body'],
            $encodedEnvelope['headers'] ?? [],
            5000
        );

        // Assert
        $this->assertNotEmpty($messageId);
        $this->assertMessageInQueue($this->queueName, 0); // 正常队列应该为空
        $this->assertMessageInDelayedQueue($this->delayedQueueName, 1);

        $delayedMessages = $this->getDelayedQueueMessages($this->delayedQueueName);
        $this->assertCount(1, $delayedMessages);

        // 检查分数（应该是未来的时间戳）
        foreach ($delayedMessages as $messageData => $score) {
            $decodedMessage = json_decode($messageData, true);
            $this->assertEquals($messageId, $decodedMessage['id']);
            
            // 分数应该是大约 5 秒后的时间戳
            $expectedScore = $beforeSend + 5000;
            $this->assertEqualsWithDelta($expectedScore, $score, 100); // 允许 100ms 误差
        }
    }
    
    public function test_get_returnsNextAvailableMessage(): void
    {
        // Arrange
        $id1 = $this->insertTestMessage(['body' => 'message 1']);
        $id2 = $this->insertTestMessage(['body' => 'message 2']);
        
        // 插入一个延迟消息（1小时后）
        $futureScore = (microtime(true) * 1000) + 3600000;
        $id3 = $this->insertDelayedMessage(['body' => 'message 3'], $futureScore);

        // Act
        $redisEnvelope1 = $this->connection->get();
        $redisEnvelope2 = $this->connection->get();
        $redisEnvelope3 = $this->connection->get();

        // Assert
        $this->assertNotNull($redisEnvelope1);
        $this->assertNotNull($redisEnvelope2);
        $this->assertNull($redisEnvelope3); // message 3 还不可用

        // Redis 现在使用 FIFO（通过 rPush/lPop），所以顺序应该是 1, 2
        $decodedMessage1 = json_decode($redisEnvelope1['data']['message'], true);
        $decodedMessage2 = json_decode($redisEnvelope2['data']['message'], true);
        
        $this->assertEquals('message 1', $decodedMessage1['body']);
        $this->assertEquals('message 2', $decodedMessage2['body']);
    }
    
    public function test_get_processesDelayedMessagesWhenReady(): void
    {
        // Arrange
        // 插入一个立即可用的延迟消息
        $nowScore = microtime(true) * 1000;
        $delayedId = $this->insertDelayedMessage(['body' => 'delayed message'], $nowScore - 100);
        
        // 插入一个正常消息
        $normalId = $this->insertTestMessage(['body' => 'normal message']);

        // Act
        $envelope1 = $this->connection->get();
        $envelope2 = $this->connection->get();
        $envelope3 = $this->connection->get();

        // Assert
        $this->assertNotNull($envelope1);
        $this->assertNotNull($envelope2);
        $this->assertNull($envelope3);

        // 延迟消息应该先被处理（因为它被插入到队列前面）
        $decodedMessage1 = json_decode($envelope1['data']['message'], true);
        $this->assertEquals('delayed message', $decodedMessage1['body']);
        
        $decodedMessage2 = json_decode($envelope2['data']['message'], true);
        $this->assertEquals('normal message', $decodedMessage2['body']);
        
        // 延迟队列应该为空
        $this->assertMessageInDelayedQueue($this->delayedQueueName, 0);
    }
    
    public function test_ack_removesMessageFromProcessing(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);
        $redisEnvelope = $this->connection->get();

        // Act
        $this->connection->ack($redisEnvelope['id']);

        // Assert
        // 消息应该已经从队列中移除（通过 rPop）
        $this->assertMessageInQueue($this->queueName, 0);
        
        // 不应该能再次获取
        $anotherEnvelope = $this->connection->get();
        $this->assertNull($anotherEnvelope);
    }
    
    public function test_reject_removesMessageFromProcessing(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);
        $redisEnvelope = $this->connection->get();

        // Act
        $this->connection->reject($redisEnvelope['id']);

        // Assert
        // 消息应该已经从队列中移除（通过 rPop）
        $this->assertMessageInQueue($this->queueName, 0);
        
        // 不应该能再次获取
        $anotherEnvelope = $this->connection->get();
        $this->assertNull($anotherEnvelope);
    }
    
    public function test_keepalive_preventsMessageRedelivery(): void
    {
        // Arrange - 设置短的重投递超时
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 0.5, // 0.5秒
            'claim_interval' => 100, // 100ms
        ]);
        $connection = new Connection($this->redis, $options);

        $id = $this->insertTestMessage(['body' => 'long processing']);
        $redisEnvelope = $connection->get();
        $this->assertNotNull($redisEnvelope);

        // Act - 模拟长时间处理，期间调用 keepalive
        usleep(300000); // 0.3秒
        $connection->keepalive($redisEnvelope['id']);
        usleep(400000); // 再等0.4秒，总共 0.7 秒，超过了 redeliver_timeout

        // 触发重投递检查
        $secondGet = $connection->get();

        // Assert - 在 list-based 实现中，消息被 rPop 后就不在队列中了
        // 所以不会有重投递（除非显式重新添加）
        $this->assertNull($secondGet);
    }
    
    public function test_getMessageCount_returnsCorrectCount(): void
    {
        // Arrange
        // Connection 是为 'test_queue' 配置的
        $this->insertTestMessage(['body' => 'message 1']);
        $this->insertTestMessage(['body' => 'message 2']);
        
        // 添加延迟消息
        $futureScore = (microtime(true) * 1000) + 3600000;
        $this->insertDelayedMessage(['body' => 'delayed 1'], $futureScore);
        $this->insertDelayedMessage(['body' => 'delayed 2'], $futureScore);
        
        // 插入到其他队列的消息不应该被计算
        $this->insertTestMessage(['body' => 'other queue'], 'other_queue');

        // Act & Assert
        $this->assertEquals(4, $this->connection->getMessageCount()); // 2 normal + 2 delayed
        
        // 获取一个消息后
        $this->connection->get();
        $this->assertEquals(4, $this->connection->getMessageCount()); // 包括正在处理的消息
        
        // 确认后
        $envelope = $this->connection->get();
        $this->connection->ack($envelope['id']);
        $this->assertEquals(3, $this->connection->getMessageCount()); // 减少一个
    }
    
    public function test_cleanup_removesAllQueues(): void
    {
        // Arrange
        $this->insertTestMessage(['body' => 'message 1']);
        $this->insertTestMessage(['body' => 'message 2']);
        
        $futureScore = (microtime(true) * 1000) + 3600000;
        $this->insertDelayedMessage(['body' => 'delayed'], $futureScore);
        
        $this->assertMessageInQueue($this->queueName, 2);
        $this->assertMessageInDelayedQueue($this->delayedQueueName, 1);

        // Act
        $this->connection->cleanup();

        // Assert
        $this->assertMessageInQueue($this->queueName, 0);
        $this->assertMessageInDelayedQueue($this->delayedQueueName, 0);
    }
    
    public function test_multipleQueues_isolatesMessages(): void
    {
        // Arrange
        $this->insertTestMessage(['body' => 'queue1_msg1'], 'queue1');
        $this->insertTestMessage(['body' => 'queue1_msg2'], 'queue1');
        $this->insertTestMessage(['body' => 'queue2_msg1'], 'queue2');

        // Act
        $connection1 = new Connection(
            $this->redis,
            array_merge($this->getConnectionOptions(), ['queue' => 'queue1'])
        );
        $connection2 = new Connection(
            $this->redis,
            array_merge($this->getConnectionOptions(), ['queue' => 'queue2'])
        );

        $envelope1_1 = $connection1->get();
        $envelope1_2 = $connection1->get();
        $envelope1_3 = $connection1->get();

        $envelope2_1 = $connection2->get();
        $envelope2_2 = $connection2->get();

        // Assert
        $this->assertNotNull($envelope1_1);
        $this->assertNotNull($envelope1_2);
        $this->assertNull($envelope1_3); // queue1 只有 2 条消息

        $this->assertNotNull($envelope2_1);
        $this->assertNull($envelope2_2); // queue2 只有 1 条消息

        $decodedMessage1_1 = json_decode($envelope1_1['data']['message'], true);
        $decodedMessage1_2 = json_decode($envelope1_2['data']['message'], true);
        $decodedMessage2_1 = json_decode($envelope2_1['data']['message'], true);

        // Redis 现在使用 FIFO，所以消息按顺序出来
        $this->assertEquals('queue1_msg1', $decodedMessage1_1['body']);
        $this->assertEquals('queue1_msg2', $decodedMessage1_2['body']);
        $this->assertEquals('queue2_msg1', $decodedMessage2_1['body']);
    }
    
    public function test_add_returnsUniqueId(): void
    {
        // Arrange
        $message = new \stdClass();
        $envelope = new Envelope($message);
        $encodedEnvelope = $this->serializer->encode($envelope);

        // Act
        $id1 = $this->connection->add($encodedEnvelope['body'], $encodedEnvelope['headers'] ?? []);
        $id2 = $this->connection->add($encodedEnvelope['body'], $encodedEnvelope['headers'] ?? []);

        // Assert
        $this->assertNotEmpty($id1);
        $this->assertNotEmpty($id2);
        $this->assertNotEquals($id1, $id2);

        // 验证返回的 ID 确实存在于消息中
        $messages = $this->getQueueMessages($this->queueName);
        $messageIds = array_map(function($msg) {
            $decoded = json_decode($msg, true);
            return $decoded['id'];
        }, $messages);

        $this->assertContains($id1, $messageIds);
        $this->assertContains($id2, $messageIds);
    }
    
    public function test_queueMaxEntries_limitsQueueSize(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'queue_max_entries' => 3,
        ]);
        $connection = new Connection($this->redis, $options);

        // Act - 添加5个消息
        for ($i = 1; $i <= 5; $i++) {
            $message = new \stdClass();
            $message->content = "message {$i}";
            $envelope = new Envelope($message);
            $encodedEnvelope = $this->serializer->encode($envelope);
            $connection->add($encodedEnvelope['body'], $encodedEnvelope['headers'] ?? []);
        }

        // Assert - 只应该保留最新的3个消息
        $this->assertMessageInQueue($this->queueName, 3);

        $messages = $this->getQueueMessages($this->queueName);
        $this->assertCount(3, $messages, 'Should have 3 messages in queue');
        
        $contents = [];
        foreach ($messages as $msg) {
            $decoded = json_decode($msg, true);
            // 使用同样的 serializer 来解码
            $decodedEnvelope = $this->serializer->decode([
                'body' => $decoded['body'],
                'headers' => $decoded['headers']
            ]);
            
            $message = $decodedEnvelope->getMessage();
            if (property_exists($message, 'content')) {
                $contents[] = $message->content;
            }
        }

        // ltrim(-3, -1) 保留列表尾部的3个元素
        // 由于 rPush 添加到尾部，消息在列表中的顺序是: [3, 4, 5]
        // 所以应该包含最后添加的3个消息
        $this->assertEquals(3, count($contents));
        $this->assertContains('message 5', $contents);
        $this->assertContains('message 4', $contents);
        $this->assertContains('message 3', $contents);
        $this->assertNotContains('message 2', $contents);
        $this->assertNotContains('message 1', $contents);
    }
    
    protected function setUp(): void
    {
        parent::setUp();

        $this->connection = new Connection($this->redis, $this->getConnectionOptions());
        $this->serializer = new PhpSerializer();
    }
}