<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;

/**
 * 集成测试：Doctrine 多队列隔离场景
 *
 * @internal
 */
#[CoversClass(DoctrineTransport::class)]
final class MultiQueueIsolationTest extends TestCase
{
    protected DBALConnection $dbalConnection;

    protected string $tableName = 'messenger_messages_test';

    private PhpSerializer $serializer;

    public function testDifferentQueuesCompleteIsolation(): void
    {
        $transport1 = $this->createTransport('queue_a');
        $transport2 = $this->createTransport('queue_b');

        $message1 = new TestMessage('message for queue A');
        $message2 = new TestMessage('message for queue B');

        $transport1->send(new Envelope($message1));
        $transport2->send(new Envelope($message2));

        // 验证队列隔离
        $this->assertEquals(1, $transport1->getMessageCount());
        $this->assertEquals(1, $transport2->getMessageCount());

        $envelopes1 = iterator_to_array($transport1->get());
        $envelopes2 = iterator_to_array($transport2->get());

        $this->assertCount(1, $envelopes1);
        $this->assertCount(1, $envelopes2);

        /** @var TestMessage $messageA */
        $messageA = $envelopes1[0]->getMessage();
        /** @var TestMessage $messageB */
        $messageB = $envelopes2[0]->getMessage();

        $this->assertEquals('message for queue A', $messageA->content);
        $this->assertEquals('message for queue B', $messageB->content);
    }

    public function testQueueSpecificMessageCount(): void
    {
        $transport = $this->createTransport('count_test');

        // 发送多条消息
        for ($i = 0; $i < 3; ++$i) {
            $message = new TestMessage('', '', $i);
            $transport->send(new Envelope($message));
        }

        $this->assertEquals(3, $transport->getMessageCount());

        // 消费一条消息
        $envelopes = iterator_to_array($transport->get());
        $transport->ack($envelopes[0]);

        $this->assertEquals(2, $transport->getMessageCount());
    }

    public function testConcurrentProcessingAcrossQueues(): void
    {
        $transportA = $this->createTransport('concurrent_a');
        $transportB = $this->createTransport('concurrent_b');

        // 发送消息到两个队列
        $messageA = new TestMessage('', 'A');
        $messageB = new TestMessage('', 'B');

        $transportA->send(new Envelope($messageA));
        $transportB->send(new Envelope($messageB));

        // 并发处理
        $envelopesA = iterator_to_array($transportA->get());
        $envelopesB = iterator_to_array($transportB->get());

        $this->assertCount(1, $envelopesA);
        $this->assertCount(1, $envelopesB);

        /** @var TestMessage $messageA */
        $messageA = $envelopesA[0]->getMessage();
        /** @var TestMessage $messageB */
        $messageB = $envelopesB[0]->getMessage();

        $this->assertEquals('A', $messageA->type);
        $this->assertEquals('B', $messageB->type);
    }

    public function testAck(): void
    {
        $transport = $this->createTransport('ack_test');
        $message = new TestMessage('test ack');

        $transport->send(new Envelope($message));
        $envelopes = iterator_to_array($transport->get());
        $this->assertCount(1, $envelopes);

        $transport->ack($envelopes[0]);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testSend(): void
    {
        $transport = $this->createTransport('send_test');
        $message = new TestMessage('test send');
        $envelope = new Envelope($message);

        $sentEnvelope = $transport->send($envelope);

        $this->assertInstanceOf(Envelope::class, $sentEnvelope);
        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $this->assertNotNull($transportIdStamp);
        $this->assertEquals(1, $transport->getMessageCount());
    }

    public function testAll(): void
    {
        $transport = $this->createTransport('all_test');

        // 发送多条消息
        for ($i = 0; $i < 3; ++$i) {
            $message = new TestMessage('', '', $i);
            $transport->send(new Envelope($message));
        }

        // 测试 all() 方法
        $allMessages = iterator_to_array($transport->all());
        $this->assertCount(3, $allMessages);

        foreach ($allMessages as $envelope) {
            $this->assertInstanceOf(Envelope::class, $envelope);
        }
    }

    public function testFind(): void
    {
        $transport = $this->createTransport('find_test');
        $message = new TestMessage('test find');
        $envelope = new Envelope($message);

        // 发送消息并获取ID
        $sentEnvelope = $transport->send($envelope);
        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $this->assertNotNull($transportIdStamp);
        $messageId = $transportIdStamp->getId();

        // 使用 find() 查找消息
        $foundEnvelope = $transport->find($messageId);
        $this->assertInstanceOf(Envelope::class, $foundEnvelope);

        /** @var TestMessage $foundMessage */
        $foundMessage = $foundEnvelope->getMessage();
        $this->assertEquals($message->content, $foundMessage->content);
    }

    public function testGet(): void
    {
        $transport = $this->createTransport('get_test');
        $message = new TestMessage('test get');

        $transport->send(new Envelope($message));

        $envelopes = iterator_to_array($transport->get());
        $this->assertCount(1, $envelopes);
        $this->assertInstanceOf(Envelope::class, $envelopes[0]);

        /** @var TestMessage $getMessage */
        $getMessage = $envelopes[0]->getMessage();
        $this->assertEquals('test get', $getMessage->content);
    }

    public function testKeepalive(): void
    {
        $transport = $this->createTransport('keepalive_test');
        $message = new TestMessage('test keepalive');

        $transport->send(new Envelope($message));
        $envelopes = iterator_to_array($transport->get());
        $this->assertCount(1, $envelopes);

        // 调用 keepalive（不应该抛出异常）
        $transport->keepalive($envelopes[0]);
        $this->assertTrue(true); // keepalive 成功调用即可

        // 清理
        $transport->ack($envelopes[0]);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testReject(): void
    {
        $transport = $this->createTransport('reject_test');
        $message = new TestMessage('test reject');

        $transport->send(new Envelope($message));
        $envelopes = iterator_to_array($transport->get());
        $this->assertCount(1, $envelopes);

        $transport->reject($envelopes[0]);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function testSetup(): void
    {
        $testTableName = 'messenger_setup_test';
        $connection = new Connection([
            'table_name' => $testTableName,
            'queue_name' => 'test_queue',
        ], $this->dbalConnection);

        $transport = new DoctrineTransport($connection, $this->serializer);
        $transport->setup();

        // 验证表已创建
        $schemaManager = $this->dbalConnection->createSchemaManager();
        $tables = $schemaManager->listTableNames();
        $this->assertContains($testTableName, $tables);

        // 清理
        $this->dbalConnection->executeStatement("DROP TABLE IF EXISTS {$testTableName}");
    }

    public function testConfigureSchema(): void
    {
        $transport = $this->createTransport('configure_schema_test');
        $schema = new Schema();

        $isSameDatabase = fn () => true;
        $transport->configureSchema($schema, $this->dbalConnection, $isSameDatabase);

        $this->assertTrue($schema->hasTable($this->tableName));
    }

    protected function setUp(): void
    {
        $this->initializeConnection();
        $this->createTestTable();
        $this->serializer = new PhpSerializer();
    }

    protected function tearDown(): void
    {
        $this->dbalConnection->executeStatement("DROP TABLE IF EXISTS {$this->tableName}");
        $this->dbalConnection->close();
        parent::tearDown();
    }

    private function createTransport(string $queueName): DoctrineTransport
    {
        $connection = new Connection([
            'table_name' => $this->tableName,
            'queue_name' => $queueName,
        ], $this->dbalConnection);

        return new DoctrineTransport($connection, $this->serializer);
    }

    private function initializeConnection(): void
    {
        $this->dbalConnection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ]);
    }

    private function createTestTable(): void
    {
        $schema = new Schema();
        $table = $schema->createTable($this->tableName);

        $table->addColumn('id', 'bigint')->setAutoincrement(true)->setNotnull(true);
        $table->addColumn('body', 'text')->setNotnull(true);
        $table->addColumn('headers', 'text')->setNotnull(true);
        $table->addColumn('queue_name', 'string')->setLength(190)->setNotnull(true);
        $table->addColumn('created_at', 'datetime')->setNotnull(true);
        $table->addColumn('available_at', 'datetime')->setNotnull(true);
        $table->addColumn('delivered_at', 'datetime')->setNotnull(false);

        $table->addUniqueConstraint(['id'], 'PRIMARY');
        $table->addIndex(['queue_name']);
        $table->addIndex(['available_at']);
        $table->addIndex(['delivered_at']);

        $sql = $schema->toSql($this->dbalConnection->getDatabasePlatform());
        foreach ($sql as $query) {
            $this->dbalConnection->executeStatement($query);
        }
    }
}
