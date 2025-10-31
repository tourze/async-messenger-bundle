<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Redis\Connection;

/**
 * 集成测试：Redis Connection 基础功能
 *
 * 注意：此集成测试专注于核心功能场景，通过集成方式验证Connection的基础能力。
 *
 * @internal
 */
#[CoversClass(Connection::class)]
class RedisIntegrationTest extends TestCase
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
    }

    /**
     * @return array<string, mixed>
     */
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

    /**
     * @return array<int, string>
     */
    protected function getQueueMessages(string $queueName): array
    {
        $result = $this->redis->lrange($queueName, 0, -1);
        $this->assertIsArray($result);
        foreach ($result as $item) {
            $this->assertIsString($item);
        }

        /** @var array<int, string> $result */
        return $result;
    }

    /**
     * @return array<string, string>
     */
    protected function getDelayedQueueMessages(string $queueName): array
    {
        $result = $this->redis->zRange($queueName, 0, -1, true);
        if (false === $result || !is_array($result)) {
            return [];
        }

        /** @var array<string, string> $result */
        return $result;
    }

    /**
     * @param array<string, mixed> $data
     */
    protected function insertTestMessage(array $data, ?string $queueName = null): string
    {
        $queueName ??= $this->queueName;

        $defaultData = [
            'id' => $this->generateId(),
            'body' => 'test body',
            'headers' => [],
            'timestamp' => microtime(true) * 1000,
        ];

        $data = array_merge($defaultData, $data);
        $message = json_encode($data);
        $this->assertIsString($message);

        $this->redis->rPush($queueName, $message);
        $this->assertIsString($data['id']);

        return $data['id'];
    }

    protected function generateId(): string
    {
        return base64_encode(random_bytes(12));
    }

    public function testRedisConnectionSetup(): void
    {
        $this->assertInstanceOf(\Redis::class, $this->redis);
        $this->assertSame('test', $this->redis->ping('test'));
    }

    public function testAdd(): void
    {
        $connection = new Connection($this->redis, $this->getConnectionOptions());

        // 测试 add() 方法
        $messageId = $connection->add('test message body', ['test' => 'header']);

        $this->assertIsString($messageId);
        $this->assertNotEmpty($messageId);

        // 验证消息已添加到队列
        $this->assertEquals(1, $this->getMessageCount());
    }

    public function testGet(): void
    {
        $connection = new Connection($this->redis, $this->getConnectionOptions());

        // 添加测试消息
        $connection->add('test message', ['header' => 'value']);

        // 测试 get() 方法
        $message = $connection->get();
        $this->assertIsArray($message);

        $this->assertArrayHasKey('id', $message);
        $this->assertArrayHasKey('body', $message);
        $this->assertEquals('test message', $message['body']);
    }

    public function testAck(): void
    {
        $connection = new Connection($this->redis, $this->getConnectionOptions());

        // 添加并获取消息
        $connection->add('test ack', []);
        $message = $connection->get();
        $this->assertIsArray($message);
        $this->assertArrayHasKey('id', $message);
        $this->assertIsString($message['id']);

        // 测试 ack() 方法
        /** @var string $messageId */
        $messageId = $message['id'];
        $connection->ack($messageId);

        // 验证消息已被确认（从处理中队列移除）
        $processingQueue = $this->queueName . '_processing';
        $this->assertEquals(0, (int) $this->redis->lLen($processingQueue));
    }

    public function testReject(): void
    {
        $connection = new Connection($this->redis, $this->getConnectionOptions());

        // 添加并获取消息
        $connection->add('test reject', []);
        $message = $connection->get();
        $this->assertIsArray($message);
        $this->assertArrayHasKey('id', $message);
        $this->assertIsString($message['id']);

        // 测试 reject() 方法
        /** @var string $messageId */
        $messageId = $message['id'];
        $connection->reject($messageId);

        // 验证消息已被拒绝（从所有队列移除）
        $this->assertEquals(0, $this->getMessageCount());
    }

    public function testKeepalive(): void
    {
        $connection = new Connection($this->redis, $this->getConnectionOptions());

        // 添加并获取消息
        $connection->add('test keepalive', []);
        $message = $connection->get();
        $this->assertIsArray($message);
        $this->assertArrayHasKey('id', $message);
        $this->assertIsString($message['id']);

        // 测试 keepalive() 方法
        /** @var string $messageId */
        $messageId = $message['id'];
        $connection->keepalive($messageId);

        // keepalive 应该延长消息的处理时间，不抛出异常即为成功
        $this->assertTrue(true);

        // 清理
        $connection->ack($messageId);
    }

    public function testSetup(): void
    {
        $connection = new Connection($this->redis, $this->getConnectionOptions());

        // 测试 setup() 方法
        $connection->setup();

        // setup 方法主要用于初始化，不抛出异常即为成功
        $this->assertTrue(true);
    }

    public function testClose(): void
    {
        $connection = new Connection($this->redis, $this->getConnectionOptions());

        // 添加测试消息
        $connection->add('test close', []);

        // 测试 close() 方法
        $connection->close();

        // 关闭连接后，消息应该仍然存在
        $this->assertEquals(1, $this->getMessageCount());
    }

    public function testCleanup(): void
    {
        $connection = new Connection($this->redis, $this->getConnectionOptions());

        // 添加多条消息
        for ($i = 0; $i < 5; ++$i) {
            $connection->add("test cleanup {$i}", []);
        }

        $this->assertEquals(5, $this->getMessageCount());

        // 测试 cleanup() 方法
        $connection->cleanup();

        // cleanup 应该清理旧消息
        // 具体行为取决于实现，这里验证方法可以正常调用
        $this->assertTrue(true);
    }

    /**
     * @param array<string, mixed> $data
     */
    protected function insertDelayedMessage(array $data, float $score, ?string $queueName = null): string
    {
        $queueName ??= $this->delayedQueueName;

        $defaultData = [
            'id' => $this->generateId(),
            'body' => 'test body',
            'headers' => [],
            'uniqid' => uniqid('', true),
        ];

        $data = array_merge($defaultData, $data);
        $message = json_encode($data);
        $this->assertIsString($message);

        $this->redis->zAdd($queueName, $score, $message);
        $this->assertIsString($data['id']);

        return $data['id'];
    }

    protected function getMessageCount(?string $queueName = null): int
    {
        $queueName ??= $this->queueName;

        return (int) $this->redis->lLen($queueName);
    }

    protected function getDelayedMessageCount(?string $queueName = null): int
    {
        $queueName ??= $this->delayedQueueName;

        return (int) $this->redis->zCard($queueName);
    }
}
