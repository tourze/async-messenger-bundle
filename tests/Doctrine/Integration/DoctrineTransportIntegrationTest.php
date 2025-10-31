<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;
use Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp;

/**
 * @internal
 */
#[CoversClass(DoctrineTransport::class)]
final class DoctrineTransportIntegrationTest extends TestCase
{
    protected \Doctrine\DBAL\Connection $dbalConnection;

    protected string $tableName = 'messenger_messages_test';

    private DoctrineTransport $transport;

    private Connection $connection;

    private PhpSerializer $serializer;

    public function testSendAndReceiveCompleteMessageLifecycle(): void
    {
        // Arrange
        $message = new \stdClass();
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
        $receivedEnvelopes = iterator_to_array($this->transport->get());

        // Assert - 验证接收结果
        $this->assertCount(1, $receivedEnvelopes);
        $receivedEnvelope = $receivedEnvelopes[0];

        $this->assertInstanceOf(Envelope::class, $receivedEnvelope);
        $receivedMessage = $receivedEnvelope->getMessage();
        $this->assertTrue(property_exists($receivedMessage, 'content'));
        $this->assertTrue(property_exists($receivedMessage, 'id'));
        $this->assertEquals($message->content, $receivedMessage->content);
        $this->assertEquals($message->id, $receivedMessage->id);

        // 验证 stamps
        $receivedStamp = $receivedEnvelope->last(DoctrineReceivedStamp::class);
        $this->assertNotNull($receivedStamp);
        $this->assertEquals($transportIdStamp->getId(), $receivedStamp->getId());

        // Act - 确认消息
        $this->transport->ack($receivedEnvelope);

        // Assert - 验证消息已被删除
        $this->assertEquals(0, $this->getMessageCount());
    }

    public function testSendWithDelayDelaysMessageDelivery(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'delayed message';
        $delayInSeconds = 2;
        $envelope = new Envelope($message, [new DelayStamp($delayInSeconds * 1000)]);

        // Act - 发送延迟消息
        $this->transport->send($envelope);

        // Assert - 立即获取应该返回空
        $immediateResult = iterator_to_array($this->transport->get());
        $this->assertEmpty($immediateResult);

        // 等待延迟时间
        sleep($delayInSeconds + 1);

        // Assert - 延迟后应该能获取到消息
        $delayedResult = iterator_to_array($this->transport->get());
        $this->assertCount(1, $delayedResult);
        $delayedMessage = $delayedResult[0]->getMessage();
        $this->assertTrue(property_exists($delayedMessage, 'content'));
        $this->assertEquals('delayed message', $delayedMessage->content);
    }

    public function testRejectRemovesMessageWithoutProcessing(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'to be rejected';
        $envelope = new Envelope($message, []);

        // Act
        $this->transport->send($envelope);
        $receivedEnvelopes = iterator_to_array($this->transport->get());
        $this->assertCount(1, $receivedEnvelopes);

        $this->transport->reject($receivedEnvelopes[0]);

        // Assert
        $this->assertEquals(0, $this->getMessageCount());
        $afterReject = $this->transport->get();
        $this->assertEmpty($afterReject);
    }

    public function testKeepalivePreventsMessageRedelivery(): void
    {
        // Arrange - 设置短的重投递超时
        $options = array_merge($this->getConnectionOptions(), ['redeliver_timeout' => 2]);
        $connection = new Connection($options, $this->dbalConnection);
        $transport = new DoctrineTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'long processing';
        $envelope = new Envelope($message, []);

        // Act
        $transport->send($envelope);
        $receivedEnvelopes = iterator_to_array($transport->get());
        $this->assertCount(1, $receivedEnvelopes);
        $receivedEnvelope = $receivedEnvelopes[0];

        // 模拟长时间处理，期间调用 keepalive
        sleep(1);
        $transport->keepalive($receivedEnvelope);
        sleep(2); // 总共 3 秒，超过了 redeliver_timeout

        // Assert - 消息不应该被重新投递
        $secondGet = $transport->get();
        $this->assertEmpty($secondGet);

        // 确认消息仍然在数据库中（但被锁定，所以 getMessageCount 返回 0）
        $sql = "SELECT COUNT(*) FROM {$this->tableName} WHERE queue_name = ?";
        $count = $this->dbalConnection->fetchOne($sql, [$options['queue_name']]);
        $this->assertEquals(1, $count);
    }

    public function testGetMessageCountReturnsCorrectCount(): void
    {
        // Arrange
        $messages = [];
        for ($i = 0; $i < 5; ++$i) {
            $message = new \stdClass();
            $message->content = "message {$i}";
            $messages[] = new Envelope($message, []);
        }

        // Act - 发送消息
        foreach ($messages as $envelope) {
            $this->transport->send($envelope);
        }

        // Assert
        $this->assertEquals(5, $this->transport->getMessageCount());

        // Act - 接收并确认部分消息
        $received = iterator_to_array($this->transport->get());
        $this->transport->ack($received[0]);

        // Assert
        $this->assertEquals(4, $this->transport->getMessageCount());
    }

    public function testAllReturnsAllPendingMessages(): void
    {
        // Arrange
        $messageCount = 3;
        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new \stdClass();
            $message->content = "message {$i}";
            $this->transport->send(new Envelope($message, []));
        }

        // Act
        $allMessages = iterator_to_array($this->transport->all());

        // Assert
        $this->assertCount($messageCount, $allMessages);

        foreach ($allMessages as $index => $envelope) {
            $this->assertInstanceOf(Envelope::class, $envelope);
            $message = $envelope->getMessage();
            $this->assertTrue(property_exists($message, 'content'));
            $this->assertEquals("message {$index}", $message->content);
        }
    }

    public function testFindReturnsSpecificMessage(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'findable message';
        $sentEnvelope = $this->transport->send(new Envelope($message, []));

        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $this->assertNotNull($transportIdStamp);
        $messageId = $transportIdStamp->getId();

        // Act
        $foundEnvelope = $this->transport->find($messageId);

        // Assert
        $this->assertInstanceOf(Envelope::class, $foundEnvelope);
        $foundMessage = $foundEnvelope->getMessage();
        $this->assertTrue(property_exists($foundMessage, 'content'));
        $this->assertEquals('findable message', $foundMessage->content);

        // 验证找不到的消息返回 null
        $notFound = $this->transport->find('non-existent-id');
        $this->assertNull($notFound);
    }

    public function testSetupCreatesRequiredInfrastructure(): void
    {
        // Arrange
        $newTableName = 'messenger_test_setup';
        $options = array_merge($this->getConnectionOptions(), [
            'table_name' => $newTableName,
            'auto_setup' => false,
        ]);

        $connection = new Connection($options, $this->dbalConnection);
        $transport = new DoctrineTransport($connection, $this->serializer);

        // Assert - 表不存在
        $this->assertFalse($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // Act
        $transport->setup();

        // Assert - 表已创建
        $this->assertTrue($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // 测试表可以正常使用
        $message = new \stdClass();
        $transport->send(new Envelope($message, []));
        $this->assertEquals(1, $transport->getMessageCount());

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
        $this->transport = new DoctrineTransport($this->connection, $this->serializer);
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
     * @return array{
     *     table_name: string,
     *     queue_name: string,
     *     redeliver_timeout: int,
     *     auto_setup: bool
     * }
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

    public function testMultipleSendersCanSendConcurrently(): void
    {
        // Arrange
        $messageCount = 10;
        $envelopes = [];

        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new \stdClass();
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
        while ($messages = iterator_to_array($this->transport->get())) {
            $receivedCount += count($messages);
            foreach ($messages as $message) {
                $this->transport->ack($message);
            }
        }

        $this->assertEquals($messageCount, $receivedCount);
    }

    public function testConfigureSchema(): void
    {
        // Arrange
        $schema = new Schema();

        // Act
        $isSameDatabase = function () { return true; };
        $this->transport->configureSchema($schema, $this->dbalConnection, $isSameDatabase);

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

    public function testGetReturnsAvailableEnvelopes(): void
    {
        // Arrange
        $messageCount = 5;
        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new \stdClass();
            $message->content = "message {$i}";
            $this->transport->send(new Envelope($message, []));
        }

        // Act
        $envelopes = iterator_to_array($this->transport->get());

        // Assert - get() 应该返回一条消息
        $this->assertCount(1, $envelopes);
        $this->assertInstanceOf(Envelope::class, $envelopes[0]);

        // 验证消息内容
        $message = $envelopes[0]->getMessage();
        if (property_exists($message, 'content') && is_string($message->content)) {
            $this->assertStringStartsWith('message ', $message->content);
        }

        // 验证包含必要的 stamps
        $receivedStamp = $envelopes[0]->last(DoctrineReceivedStamp::class);
        $this->assertNotNull($receivedStamp);
        $this->assertNotEmpty($receivedStamp->getId());
    }

    public function testAck(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'test ack message';
        $envelope = new Envelope($message, []);

        $this->transport->send($envelope);
        $this->assertEquals(1, $this->transport->getMessageCount());

        $receivedEnvelopes = iterator_to_array($this->transport->get());
        $this->assertCount(1, $receivedEnvelopes);
        $receivedEnvelope = $receivedEnvelopes[0];

        // Act
        $this->transport->ack($receivedEnvelope);

        // Assert - 消息应该被从数据库中删除
        $this->assertEquals(0, $this->transport->getMessageCount());
        $this->assertEquals(0, $this->getMessageCount());

        // 验证无法再次获取该消息
        $afterAckEnvelopes = iterator_to_array($this->transport->get());
        $this->assertEmpty($afterAckEnvelopes);
    }
}
