<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use PHPUnit\Framework\TestCase;

abstract class RedisIntegrationTestCase extends TestCase
{
    protected \Redis $redis;
    protected string $queueName = 'test_queue';
    protected string $delayedQueueName = 'test_queue_delayed';
    
    protected function setUp(): void
    {
        parent::setUp();
        
        // 创建 Redis 连接
        $this->redis = new \Redis();
        
        try {
            // 尝试连接到本地 Redis
            if (!$this->redis->connect('127.0.0.1', 6379)) {
                $this->markTestSkipped('Redis server is not available.');
            }
            
            // 使用独立的测试数据库
            $this->redis->select(15);
            
            // 清理测试数据
            $this->redis->flushDB();
        } catch (\RedisException $e) {
            $this->markTestSkipped('Redis server is not available: ' . $e->getMessage());
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
    
    protected function getConnectionOptions(): array
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
    
    protected function assertMessageInQueue(string $queueName, int $expectedCount): void
    {
        $actualCount = $this->redis->lLen($queueName);
        $this->assertEquals($expectedCount, $actualCount, "Expected {$expectedCount} messages in queue {$queueName}, but found {$actualCount}");
    }
    
    protected function assertMessageInDelayedQueue(string $queueName, int $expectedCount): void
    {
        $actualCount = $this->redis->zCard($queueName);
        $this->assertEquals($expectedCount, $actualCount, "Expected {$expectedCount} messages in delayed queue {$queueName}, but found {$actualCount}");
    }
    
    protected function getQueueMessages(string $queueName): array
    {
        return $this->redis->lrange($queueName, 0, -1);
    }
    
    protected function getDelayedQueueMessages(string $queueName): array
    {
        return $this->redis->zRange($queueName, 0, -1, true);
    }
    
    protected function insertTestMessage(array $data, ?string $queueName = null): string
    {
        $queueName = $queueName ?? $this->queueName;
        
        $defaultData = [
            'id' => $this->generateId(),
            'body' => 'test body',
            'headers' => [],
            'timestamp' => microtime(true) * 1000,
        ];
        
        $data = array_merge($defaultData, $data);
        $message = json_encode($data);
        
        $this->redis->rPush($queueName, $message);
        
        return $data['id'];
    }
    
    protected function generateId(): string
    {
        return base64_encode(random_bytes(12));
    }
    
    protected function insertDelayedMessage(array $data, float $score, ?string $queueName = null): string
    {
        $queueName = $queueName ?? $this->delayedQueueName;

        $defaultData = [
            'id' => $this->generateId(),
            'body' => 'test body',
            'headers' => [],
            'uniqid' => uniqid('', true),
        ];

        $data = array_merge($defaultData, $data);
        $message = json_encode($data);

        $this->redis->zAdd($queueName, $score, $message);

        return $data['id'];
    }
    
    protected function getMessageCount(?string $queueName = null): int
    {
        $queueName = $queueName ?? $this->queueName;
        return (int) $this->redis->lLen($queueName);
    }
    
    protected function getDelayedMessageCount(?string $queueName = null): int
    {
        $queueName = $queueName ?? $this->delayedQueueName;
        return (int) $this->redis->zCard($queueName);
    }
}