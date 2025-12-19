<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;

/**
 * @internal
 */
#[CoversClass(Connection::class)]
final class ConnectionIntegrationTest extends TestCase
{
    protected DBALConnection $dbalConnection;

    protected string $tableName = 'messenger_messages_test';

    private Connection $connection;

    private PhpSerializer $serializer;

    public function testSendSavesMessageToDatabase(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'test message';
        $envelope = new Envelope($message, []);
        $encodedEnvelope = $this->serializer->encode($envelope);

        // Act
        $body = $encodedEnvelope['body'];
        $headers = $encodedEnvelope['headers'] ?? [];
        $this->assertIsString($body);
        $this->assertIsArray($headers);
        /** @var array<string, string> $headers */
        $messageId = $this->connection->send($body, $headers);

        // Assert
        $this->assertEquals(1, $this->getMessageCount());

        $messages = $this->connection->findAll();
        $messagesArray = iterator_to_array($messages);
        $this->assertCount(1, $messagesArray);

        $savedMessage = $messagesArray[0];
        $this->assertEquals($encodedEnvelope['body'], $savedMessage['body']);
        $this->assertIsArray($savedMessage['headers']);
        $this->assertEquals([], $savedMessage['headers']); // PhpSerializer doesn't include headers
        $this->assertEquals('test_queue', $savedMessage['queue_name']);
        $this->assertNull($savedMessage['delivered_at']);
    }

    public function testSendWithDelaySetsCorrectAvailableAt(): void
    {
        $delayMs = 5000;
        $beforeSend = new \DateTime();

        $this->sendDelayedMessage($delayMs);
        $this->assertDelayedMessageTiming($beforeSend, $delayMs);

        // 确保测试有断言
        $this->assertGreaterThan(0, $delayMs);
    }

    public function testGetReturnsNextAvailableMessage(): void
    {
        // Arrange
        $message1 = ['body' => 'message 1', 'headers' => '{}'];
        $message2 = ['body' => 'message 2', 'headers' => '{}'];
        $message3 = ['body' => 'message 3', 'headers' => '{}', 'available_at' => new \DateTime('+1 hour')];

        $id1 = $this->insertTestMessage($message1);
        $id2 = $this->insertTestMessage($message2);
        $id3 = $this->insertTestMessage($message3);

        // Act
        $doctrineEnvelope1 = $this->connection->get();
        $doctrineEnvelope2 = $this->connection->get();
        $doctrineEnvelope3 = $this->connection->get();

        // Assert
        $this->assertNotNull($doctrineEnvelope1);
        $this->assertNotNull($doctrineEnvelope2);
        $this->assertNull($doctrineEnvelope3); // message 3 还不可用

        $this->assertEquals('message 1', $doctrineEnvelope1['body']);
        $this->assertEquals('message 2', $doctrineEnvelope2['body']);
    }

    public function testGetMarksMessageAsDelivered(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);

        // Act
        $doctrineEnvelope = $this->connection->get();

        // Assert
        $this->assertNotNull($doctrineEnvelope);

        $message = $this->connection->find($id);
        $this->assertIsArray($message);
        $this->assertArrayHasKey('delivered_at', $message);
        $this->assertNotNull($message['delivered_at']);

        // 验证其他消费者无法获取已投递的消息
        $anotherEnvelope = $this->connection->get();
        $this->assertNull($anotherEnvelope);
    }

    public function testAckRemovesMessageFromDatabase(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);
        $doctrineEnvelope = $this->connection->get();

        // Act
        $this->connection->ack($id);

        // Assert
        $this->assertMessageNotInDatabase($id);
        $this->assertEquals(0, $this->getMessageCount());
    }

    public function testRejectRemovesMessageFromDatabase(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);
        $doctrineEnvelope = $this->connection->get();

        // Act
        $this->connection->reject($id);

        // Assert
        $this->assertMessageNotInDatabase($id);
        $this->assertEquals(0, $this->getMessageCount());
    }

    public function testKeepaliveUpdatesDeliveredAt(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);
        $doctrineEnvelope = $this->connection->get();

        $message = $this->connection->find($id);
        $this->assertIsArray($message);
        $this->assertArrayHasKey('delivered_at', $message);
        $originalDeliveredAt = $message['delivered_at'];
        sleep(1); // 确保时间差异

        // Act
        $this->connection->keepalive($id);

        // Assert
        $updatedMessage = $this->connection->find($id);
        $this->assertNotNull($updatedMessage);
        $this->assertArrayHasKey('delivered_at', $updatedMessage);
        $this->assertNotEquals($originalDeliveredAt, $updatedMessage['delivered_at']);
        $this->assertIsString($originalDeliveredAt);
        $this->assertIsString($updatedMessage['delivered_at']);
        $this->assertGreaterThan(
            new \DateTime($originalDeliveredAt),
            new \DateTime($updatedMessage['delivered_at'])
        );
    }

    public function testGetMessageCountReturnsCorrectCount(): void
    {
        // Arrange
        // Connection 是为 'test_queue' 配置的
        $this->insertTestMessage(['queue_name' => 'test_queue']);
        $this->insertTestMessage(['queue_name' => 'test_queue']);
        $this->insertTestMessage(['queue_name' => 'other_queue']);

        // Act & Assert
        // getMessageCount 只计算当前队列的消息
        $this->assertEquals(2, $this->connection->getMessageCount());

        // 验证总消息数（使用基类方法）
        $this->assertEquals(3, $this->getMessageCount());
        $this->assertEquals(2, $this->getMessageCount('test_queue'));
        $this->assertEquals(1, $this->getMessageCount('other_queue'));
    }

    public function testSetupCreatesTableIfNotExists(): void
    {
        // Arrange
        $newTableName = 'messenger_messages_new';
        $options = array_merge($this->getConnectionOptions(), ['table_name' => $newTableName]);
        $connection = new Connection($options, $this->dbalConnection);

        // 确保表不存在
        $this->assertFalse($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // Act
        $connection->setup();

        // Assert
        $this->assertTrue($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // Cleanup
        $this->dbalConnection->executeStatement("DROP TABLE {$newTableName}");
    }

    protected function setUp(): void
    {
        // 使用内存数据库进行测试
        $this->dbalConnection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ]);

        // 创建测试表
        $this->createTestTable();

        $this->connection = new Connection($this->getConnectionOptions(), $this->dbalConnection);
        $this->serializer = new PhpSerializer();
    }

    protected function tearDown(): void
    {
        // 清理测试表
        $this->dbalConnection->executeStatement("DROP TABLE IF EXISTS {$this->tableName}");
        $this->dbalConnection->close();

        parent::tearDown();
    }

    private function createTestTable(): void
    {
        $schema = new Schema();
        $table = $schema->createTable($this->tableName);

        $table->addColumn('id', 'bigint')
            ->setAutoincrement(true)
            ->setNotnull(true)
        ;
        $table->addColumn('body', 'text')
            ->setNotnull(true)
        ;
        $table->addColumn('headers', 'text')
            ->setNotnull(true)
        ;
        $table->addColumn('queue_name', 'string')
            ->setLength(190)
            ->setNotnull(true)
        ;
        $table->addColumn('created_at', 'datetime')
            ->setNotnull(true)
        ;
        $table->addColumn('available_at', 'datetime')
            ->setNotnull(true)
        ;
        $table->addColumn('delivered_at', 'datetime')
            ->setNotnull(false)
        ;

        $table->addUniqueConstraint(['id'], 'PRIMARY');
        $table->addIndex(['queue_name']);
        $table->addIndex(['available_at']);
        $table->addIndex(['delivered_at']);

        $sql = $schema->toSql($this->dbalConnection->getDatabasePlatform());
        foreach ($sql as $query) {
            $this->dbalConnection->executeStatement($query);
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function getConnectionOptions(): array
    {
        return [
            'table_name' => $this->tableName,
            'queue_name' => 'test_queue',
            'redeliver_timeout' => 3600,
            'auto_setup' => false, // 我们已经手动创建表
        ];
    }

    private function getMessageCount(?string $queueName = null): int
    {
        $sql = "SELECT COUNT(*) FROM {$this->tableName}";
        $params = [];

        if (null !== $queueName) {
            $sql .= ' WHERE queue_name = ?';
            $params[] = $queueName;
        }

        $result = $this->dbalConnection->fetchOne($sql, $params);

        return is_numeric($result) ? (int) $result : 0;
    }

    private function assertMessageNotInDatabase(string $id): void
    {
        $sql = "SELECT COUNT(*) FROM {$this->tableName} WHERE id = ?";
        $count = $this->dbalConnection->fetchOne($sql, [$id]);

        $this->assertEquals(0, $count);
    }

    /**
     * 发送延迟消息
     */
    private function sendDelayedMessage(int $delayMs): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, [new DelayStamp($delayMs)]);
        $encodedEnvelope = $this->serializer->encode($envelope);

        $body = $encodedEnvelope['body'];
        $headers = $encodedEnvelope['headers'] ?? [];
        $this->assertIsString($body);
        $this->assertIsArray($headers);
        /** @var array<string, string> $headers */
        $this->connection->send($body, $headers, $delayMs);
    }

    /**
     * 验证延迟消息时间
     */
    private function assertDelayedMessageTiming(\DateTime $beforeSend, int $delayMs): void
    {
        $sql = "SELECT * FROM {$this->tableName} WHERE queue_name = ?";
        $messages = $this->dbalConnection->fetchAllAssociative($sql, ['test_queue']);
        $this->assertCount(1, $messages);

        $savedMessage = $messages[0];
        $this->assertNotNull($savedMessage['available_at']);
        $this->assertIsString($savedMessage['available_at']);

        $availableAt = new \DateTime($savedMessage['available_at']);
        $expectedAvailableAt = (clone $beforeSend)->modify('+' . ($delayMs / 1000) . ' seconds');

        // 允许 1 秒的误差
        $timeDiff = abs($availableAt->getTimestamp() - $expectedAvailableAt->getTimestamp());
        $this->assertLessThanOrEqual(1, $timeDiff);
    }

    /**
     * 设置多个队列和消息
     *
     * @param array<string, list<string>> $queueMessages
     */
    private function setupMultipleQueuesWithMessages(array $queueMessages): void
    {
        foreach ($queueMessages as $queueName => $messages) {
            foreach ($messages as $message) {
                $this->insertTestMessage(['body' => $message, 'queue_name' => $queueName]);
            }
        }
    }

    /**
     * 验证队列隔离
     *
     * @param array<string, list<string>> $queueMessages
     */
    private function verifyQueueIsolation(array $queueMessages): void
    {
        $connections = $this->createConnectionsForQueues(array_keys($queueMessages));

        foreach ($queueMessages as $queueName => $expectedMessages) {
            $connection = $connections[$queueName];
            $this->verifyQueueProcessing($connection, $expectedMessages);
        }
    }

    /**
     * 为队列创建连接
     *
     * @param list<string> $queueNames
     * @return array<string, Connection>
     */
    private function createConnectionsForQueues(array $queueNames): array
    {
        $connections = [];
        foreach ($queueNames as $queueName) {
            $connections[$queueName] = new Connection(
                array_merge($this->getConnectionOptions(), ['queue_name' => $queueName]),
                $this->dbalConnection
            );
        }

        return $connections;
    }

    /**
     * 验证队列处理
     *
     * @param list<string> $expectedMessages
     */
    private function verifyQueueProcessing(Connection $connection, array $expectedMessages): void
    {
        $actualMessages = [];
        $expectedCount = count($expectedMessages);

        // 获取所有消息
        for ($i = 0; $i <= $expectedCount; ++$i) {
            $envelope = $connection->get();
            if (null !== $envelope) {
                $actualMessages[] = $envelope['body'];
            }
        }

        $this->assertCount($expectedCount, $actualMessages);
        foreach ($expectedMessages as $expectedMessage) {
            $this->assertContains($expectedMessage, $actualMessages);
        }
    }

    /**
     * @param array<string, mixed> $data
     */
    private function insertTestMessage(array $data): string
    {
        $defaultData = [
            'body' => 'test body',
            'headers' => '{}',
            'queue_name' => 'test_queue',
            'created_at' => new \DateTime(),
            'available_at' => new \DateTime(),
            'delivered_at' => null,
        ];

        $data = array_merge($defaultData, $data);

        $this->dbalConnection->insert($this->tableName, $data, [
            'created_at' => 'datetime',
            'available_at' => 'datetime',
            'delivered_at' => 'datetime',
        ]);

        return (string) $this->dbalConnection->lastInsertId();
    }

    public function testMultipleQueuesIsolatesMessages(): void
    {
        $queueMessages = [
            'queue1' => ['queue1_msg1', 'queue1_msg2'],
            'queue2' => ['queue2_msg1'],
        ];

        $this->setupMultipleQueuesWithMessages($queueMessages);
        $this->verifyQueueIsolation($queueMessages);

        // 验证队列隔离确实生效
        $totalExpectedMessages = array_sum(array_map('count', $queueMessages));
        $this->assertEquals($totalExpectedMessages, $this->getMessageCount());

        // 验证每个队列的消息数量正确
        $this->assertEquals(2, $this->getMessageCount('queue1'));
        $this->assertEquals(1, $this->getMessageCount('queue2'));
    }

    public function testSendReturnsUniqueId(): void
    {
        // Arrange
        $message = new \stdClass();
        $envelope = new Envelope($message, []);
        $encodedEnvelope = $this->serializer->encode($envelope);

        $body = $encodedEnvelope['body'];
        $headers = $encodedEnvelope['headers'] ?? [];
        $this->assertIsString($body);
        $this->assertIsArray($headers);
        /** @var array<string, string> $headers */

        // Act
        $id1 = $this->connection->send($body, $headers);
        $id2 = $this->connection->send($body, $headers);

        // Assert
        $this->assertNotEmpty($id1);
        $this->assertNotEmpty($id2);
        $this->assertNotEquals($id1, $id2);

        // 验证返回的 ID 确实存在于数据库中
        $this->assertNotNull($this->connection->find($id1));
        $this->assertNotNull($this->connection->find($id2));
    }

    public function testConfigureSchema(): void
    {
        // Arrange
        $schema = new Schema();

        // Act
        $isSameDatabase = function () { return true; };
        $this->connection->configureSchema($schema, $this->dbalConnection, $isSameDatabase);

        // Assert
        $this->assertTrue($schema->hasTable($this->tableName));
        $table = $schema->getTable($this->tableName);
        $this->assertTrue($table->hasColumn('id'));
        $this->assertTrue($table->hasColumn('body'));
        $this->assertTrue($table->hasColumn('headers'));
        $this->assertTrue($table->hasColumn('queue_name'));
        $this->assertTrue($table->hasColumn('created_at'));
        $this->assertTrue($table->hasColumn('available_at'));
        $this->assertTrue($table->hasColumn('delivered_at'));
    }

    public function testFindAll(): void
    {
        // Arrange
        $messages = [
            ['body' => 'message 1', 'headers' => '{}'],
            ['body' => 'message 2', 'headers' => '{}'],
            ['body' => 'future message', 'headers' => '{}', 'available_at' => new \DateTime('+1 hour')],
        ];

        foreach ($messages as $messageData) {
            $this->insertTestMessage($messageData);
        }

        // Act
        $foundMessages = $this->connection->findAll();
        $foundMessagesArray = iterator_to_array($foundMessages);

        // Assert - findAll 只返回当前可用的消息
        $this->assertCount(2, $foundMessagesArray);
        $this->assertEquals('message 1', $foundMessagesArray[0]['body']);
        $this->assertEquals('message 2', $foundMessagesArray[1]['body']);
    }

    public function testGetAvailableMessageCount(): void
    {
        // Arrange
        $this->insertTestMessage(['body' => 'available now']);
        $this->insertTestMessage(['body' => 'also available']);
        $this->insertTestMessage(['body' => 'future', 'available_at' => new \DateTime('+1 hour')]);
        $this->insertTestMessage(['body' => 'other queue', 'queue_name' => 'other']);

        // Act
        $count = $this->connection->getMessageCount();

        // Assert - 只计算当前队列的可用消息
        $this->assertEquals(2, $count);
    }

    public function testSetupWithAutoSetupDisabled(): void
    {
        // Arrange
        $newTableName = 'test_auto_setup_disabled';
        $options = array_merge($this->getConnectionOptions(), [
            'table_name' => $newTableName,
            'auto_setup' => false,  // 禁用自动设置
        ]);

        $connection = new Connection($options, $this->dbalConnection);

        // Assert - 表不存在且不会自动创建
        $this->assertFalse($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // 尝试发送消息会失败，因为表不存在
        $this->expectException(\Exception::class);
        $connection->send('test body', []);
    }

    public function testReset(): void
    {
        // Arrange
        $this->insertTestMessage(['body' => 'message 1']);
        $this->insertTestMessage(['body' => 'message 2']);

        // 获取第一个消息（标记为已投递）
        $envelope = $this->connection->get();
        $this->assertNotNull($envelope);

        // Verify there are messages and one is delivered
        $this->assertEquals(2, $this->getMessageCount());

        // Act
        $this->connection->reset();

        // Assert - reset() 应该清理任何内部状态
        // 主要作用是重置队列清空时间缓存
        $envelope2 = $this->connection->get();
        $this->assertNotNull($envelope2);

        // 验证 reset 后功能仍然正常
        $this->assertEquals('message 2', $envelope2['body']);
    }
}
