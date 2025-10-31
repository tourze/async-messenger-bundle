<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;
use Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp;

/**
 * @internal
 */
#[CoversClass(Connection::class)]
final class MessageRedeliveryTest extends TestCase
{
    protected \Doctrine\DBAL\Connection $dbalConnection;

    protected string $tableName = 'messenger_messages_test';

    private PhpSerializer $serializer;

    public function testMessageRedeliveryAfterTimeout(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(2); // 2秒超时

        $message = new \stdClass();
        $message->content = 'will be redelivered';
        $envelope = new Envelope($message, []);

        // Act - 发送消息
        $transport->send($envelope);

        // 第一次获取消息
        $firstDelivery = iterator_to_array($transport->get());
        $this->assertCount(1, $firstDelivery);
        $firstEnvelope = $firstDelivery[0];

        // 记录第一次投递的信息
        $receivedStamp = $firstEnvelope->last(DoctrineReceivedStamp::class);
        $firstDeliveryId = null !== $receivedStamp ? $receivedStamp->getId() : '';

        // 不确认消息，模拟处理失败
        // 立即尝试再次获取，应该为空（消息被锁定）
        $immediateRetry = $transport->get();
        $this->assertEmpty($immediateRetry);

        // 等待超过重投递超时时间
        sleep(3);

        // Assert - 消息应该可以被重新获取
        $secondDelivery = iterator_to_array($transport->get());
        $this->assertCount(1, $secondDelivery);
        $secondEnvelope = $secondDelivery[0];

        // 验证是同一条消息
        $secondMessage = $secondEnvelope->getMessage();
        if (property_exists($secondMessage, 'content')) {
            $this->assertEquals($message->content, $secondMessage->content);
        }

        // 验证消息ID相同（同一条数据库记录）
        $secondReceivedStamp = $secondEnvelope->last(DoctrineReceivedStamp::class);
        if (null !== $secondReceivedStamp) {
            $this->assertEquals($firstDeliveryId, $secondReceivedStamp->getId());
        }

        // 这次确认消息
        $transport->ack($secondEnvelope);

        // 验证消息已被删除
        $this->assertEquals(0, $transport->getMessageCount());
    }

    private function createTransportWithTimeout(int $redeliverTimeout): DoctrineTransport
    {
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => $redeliverTimeout,
        ]);

        $connection = new Connection($options, $this->dbalConnection);

        return new DoctrineTransport($connection, $this->serializer);
    }

    public function testKeepalivePreventsRedelivery(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(2); // 2秒超时

        $message = new \stdClass();
        $message->content = 'kept alive';
        $envelope = new Envelope($message, []);

        // Act
        $transport->send($envelope);
        $envelopes = iterator_to_array($transport->get());
        $this->assertCount(1, $envelopes);
        $receivedEnvelope = $envelopes[0];

        // 在超时前多次调用 keepalive
        for ($i = 0; $i < 3; ++$i) {
            sleep(1);
            $transport->keepalive($receivedEnvelope);
        }

        // 总共已经过了3秒，超过了2秒的超时时间
        // 但由于调用了 keepalive，消息不应该被重投递

        // Assert
        $shouldBeEmpty = $transport->get();
        $this->assertEmpty($shouldBeEmpty);

        // 最终确认消息
        $transport->ack($receivedEnvelope);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testMultipleRedeliveriesUntilSuccess(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(1); // 1秒超时

        $message = new \stdClass();
        $message->content = 'retry multiple times';
        $message->id = uniqid();
        $envelope = new Envelope($message, []);

        // Act
        $transport->send($envelope);

        $deliveryAttempts = [];
        $maxAttempts = 3;

        for ($attempt = 1; $attempt <= $maxAttempts; ++$attempt) {
            $envelopes = iterator_to_array($transport->get());

            if ([] === $envelopes) {
                // 等待重投递
                sleep(2);
                $envelopes = iterator_to_array($transport->get());
            }

            $this->assertCount(1, $envelopes);
            $receivedEnvelope = $envelopes[0];

            // 记录投递尝试
            $deliveryAttempts[] = [
                'attempt' => $attempt,
                'message_id' => property_exists($receivedEnvelope->getMessage(), 'id') ? $receivedEnvelope->getMessage()->id : '',
                'content' => property_exists($receivedEnvelope->getMessage(), 'content') ? $receivedEnvelope->getMessage()->content : '',
            ];

            if ($attempt < $maxAttempts) {
                // 前几次不确认，模拟处理失败
                sleep(2); // 等待超时
            } else {
                // 最后一次确认
                $transport->ack($receivedEnvelope);
            }
        }

        // Assert
        $this->assertCount($maxAttempts, $deliveryAttempts);

        // 验证每次都是同一条消息
        foreach ($deliveryAttempts as $delivery) {
            $this->assertEquals($message->id, $delivery['message_id']);
            if (property_exists($message, 'content')) {
                $this->assertEquals($message->content, $delivery['content']);
            }
        }

        // 验证消息已被删除
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testRedeliveryTimeoutPerMessage(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(2); // 2秒默认超时

        // 发送多条消息
        $messages = [];
        for ($i = 0; $i < 3; ++$i) {
            $message = new \stdClass();
            $message->content = "message_{$i}";
            $messages[] = $message;
            $transport->send(new Envelope($message, []));
        }

        // Act - 获取所有消息但不确认
        $receivedEnvelopes = [];
        for ($i = 0; $i < 3; ++$i) {
            $envelopes = iterator_to_array($transport->get());
            $this->assertCount(1, $envelopes);
            $receivedEnvelopes[] = $envelopes[0];
            usleep(500000); // 0.5秒间隔
        }

        // 只对第二条消息调用 keepalive
        sleep(1);
        $transport->keepalive($receivedEnvelopes[1]);

        // 再等待，使第一条和第三条消息超时
        sleep(2);

        // Assert - 应该能重新获取第一条和第三条消息
        $redelivered = [];
        while ($envelopes = iterator_to_array($transport->get())) {
            $redeliveredMessage = $envelopes[0]->getMessage();
            if (property_exists($redeliveredMessage, 'content')) {
                $redelivered[] = $redeliveredMessage->content;
            }
            $transport->ack($envelopes[0]);
        }

        $this->assertCount(2, $redelivered);
        $this->assertContains('message_0', $redelivered);
        $this->assertContains('message_2', $redelivered);
        $this->assertNotContains('message_1', $redelivered); // 这条被 keepalive 了

        // 确认第二条消息
        $transport->ack($receivedEnvelopes[1]);

        // 验证所有消息都已处理
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testImmediateRedeliveryNotPossible(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(3600); // 1小时超时

        $message = new \stdClass();
        $message->content = 'locked message';
        $transport->send(new Envelope($message, []));

        // Act
        $firstGet = iterator_to_array($transport->get());
        $this->assertCount(1, $firstGet);

        // 立即尝试多次获取
        $attempts = [];
        for ($i = 0; $i < 5; ++$i) {
            $attempts[] = $transport->get();
            usleep(100000); // 100ms
        }

        // Assert - 所有尝试都应该失败
        foreach ($attempts as $attempt) {
            $this->assertEmpty($attempt);
        }

        // 清理
        $transport->ack($firstGet[0]);
    }

    public function testRedeliveryAfterDatabaseReconnection(): void
    {
        // Arrange
        $shortTimeout = 2;
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => $shortTimeout,
        ]);

        // 第一个连接
        $connection1 = new Connection($options, $this->dbalConnection);
        $transport1 = new DoctrineTransport($connection1, $this->serializer);

        $message = new \stdClass();
        $message->content = 'survive reconnection';
        $transport1->send(new Envelope($message, []));

        // Act - 第一个连接获取消息
        $envelopes = iterator_to_array($transport1->get());
        $this->assertCount(1, $envelopes);

        // 模拟连接断开（不确认消息）
        unset($transport1, $connection1);

        // 等待超时
        sleep($shortTimeout + 1);

        // 新连接
        $connection2 = new Connection($options, $this->dbalConnection);
        $transport2 = new DoctrineTransport($connection2, $this->serializer);

        // Assert - 新连接应该能获取到消息
        $redelivered = iterator_to_array($transport2->get());
        $this->assertCount(1, $redelivered);
        $redeliveredMessage = $redelivered[0]->getMessage();
        if (property_exists($redeliveredMessage, 'content')) {
            $this->assertEquals('survive reconnection', $redeliveredMessage->content);
        }

        // 确认消息
        $transport2->ack($redelivered[0]);
        $this->assertEquals(0, $transport2->getMessageCount());
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

    public function testAck(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(3600);
        $message = new \stdClass();
        $message->content = 'test ack';
        $transport->send(new Envelope($message, []));

        $envelopes = iterator_to_array($transport->get());
        $this->assertCount(1, $envelopes);
        $envelope = $envelopes[0];

        // Act
        $transport->ack($envelope);

        // Assert
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testAll(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(3600);
        $messageCount = 3;

        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new \stdClass();
            $message->content = "message {$i}";
            $transport->send(new Envelope($message, []));
        }

        // Act
        $allMessages = iterator_to_array($transport->all());

        // Assert
        $this->assertCount($messageCount, $allMessages);
        foreach ($allMessages as $envelope) {
            $this->assertInstanceOf(Envelope::class, $envelope);
        }
    }

    public function testConfigureSchema(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(3600);
        $schema = new Schema();

        // Act
        $isSameDatabase = function () { return true; };
        $transport->configureSchema($schema, $this->dbalConnection, $isSameDatabase);

        // Assert
        $this->assertTrue($schema->hasTable($this->tableName));
        $table = $schema->getTable($this->tableName);
        $this->assertTrue($table->hasColumn('id'));
        $this->assertTrue($table->hasColumn('body'));
        $this->assertTrue($table->hasColumn('headers'));
    }

    public function testFind(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(3600);
        $message = new \stdClass();
        $message->content = 'findable message';
        $sentEnvelope = $transport->send(new Envelope($message, []));

        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $messageId = null !== $transportIdStamp ? $transportIdStamp->getId() : '';

        // Act
        $foundEnvelope = $transport->find($messageId);

        // Assert
        $this->assertInstanceOf(Envelope::class, $foundEnvelope);
        $foundMessage = $foundEnvelope->getMessage();
        if (property_exists($foundMessage, 'content')) {
            $this->assertEquals('findable message', $foundMessage->content);
        }

        // 测试找不到的消息
        $notFound = $transport->find('non-existent-id');
        $this->assertNull($notFound);
    }

    public function testGet(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(3600);
        $message = new \stdClass();
        $message->content = 'test get';
        $transport->send(new Envelope($message, []));

        // Act
        $envelopes = iterator_to_array($transport->get());

        // Assert
        $this->assertCount(1, $envelopes);
        $this->assertInstanceOf(Envelope::class, $envelopes[0]);
        $getMessage = $envelopes[0]->getMessage();
        if (property_exists($getMessage, 'content')) {
            $this->assertEquals('test get', $getMessage->content);
        }
    }

    public function testReject(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(3600);
        $message = new \stdClass();
        $message->content = 'to be rejected';
        $transport->send(new Envelope($message, []));

        $envelopes = iterator_to_array($transport->get());
        $envelope = $envelopes[0];

        // Act
        $transport->reject($envelope);

        // Assert
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testSend(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(3600);
        $message = new \stdClass();
        $message->content = 'test send';
        $envelope = new Envelope($message, []);

        // Act
        $sentEnvelope = $transport->send($envelope);

        // Assert
        $this->assertInstanceOf(Envelope::class, $sentEnvelope);
        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $this->assertNotNull($transportIdStamp);
        $this->assertEquals(1, $transport->getMessageCount());
    }

    public function testSetup(): void
    {
        // Arrange
        $newTableName = 'messenger_redelivery_setup';
        $options = array_merge($this->getConnectionOptions(), [
            'table_name' => $newTableName,
            'auto_setup' => false,
        ]);

        $connection = new Connection($options, $this->dbalConnection);
        $transport = new DoctrineTransport($connection, new PhpSerializer());

        // Assert - 表不存在
        $this->assertFalse($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // Act
        $transport->setup();

        // Assert - 表已创建
        $this->assertTrue($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // Cleanup
        $this->dbalConnection->executeStatement("DROP TABLE {$newTableName}");
    }

    public function testFindAll(): void
    {
        // Arrange
        $connection = new Connection($this->getConnectionOptions(), $this->dbalConnection);
        $messageData = [
            ['body' => 'message 1', 'headers' => '{}'],
            ['body' => 'message 2', 'headers' => '{}'],
            ['body' => 'future message', 'headers' => '{}', 'available_at' => new \DateTime('+1 hour')],
        ];

        foreach ($messageData as $data) {
            $this->insertTestMessage($data, $connection);
        }

        // Act
        $foundMessages = $connection->findAll();
        $foundMessagesArray = iterator_to_array($foundMessages);

        // Assert - findAll 只返回当前可用的消息
        $this->assertCount(2, $foundMessagesArray);
        $this->assertEquals('message 1', $foundMessagesArray[0]['body']);
        $this->assertEquals('message 2', $foundMessagesArray[1]['body']);
    }

    public function testReset(): void
    {
        // Arrange
        $connection = new Connection($this->getConnectionOptions(), $this->dbalConnection);
        $this->insertTestMessage(['body' => 'message 1'], $connection);

        // 获取第一个消息（标记为已投递）
        $envelope = $connection->get();
        $this->assertNotNull($envelope);

        // Act
        $connection->reset();

        // Assert - reset() 应该清理任何内部状态
        // 主要作用是重置队列清空时间缓存
        $envelope2 = $connection->get();
        $this->assertNull($envelope2); // 消息已被第一次get标记为delivered

        // 验证 reset 后功能仍然正常（发送新消息）
        $connection->send('new message after reset', []);
        $envelope3 = $connection->get();
        $this->assertNotNull($envelope3);
        $this->assertEquals('new message after reset', $envelope3['body']);
    }

    /**
     * @param array<string, mixed> $data
     */
    private function insertTestMessage(array $data, Connection $connection): string
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
}
