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
use Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration\Fixtures\TestMessage;

/**
 * 集成测试：测试 Doctrine 传输在并发消费场景下的行为
 *
 * 本测试类主要验证多个消费者并发消费消息时的正确性，包括：
 * - 消息不会被重复处理
 * - 负载均衡
 * - 消费者故障时的消息恢复
 * - 高吞吐量下的消息完整性
 *
 * 注意：此集成测试专注于并发消费场景的端到端验证。
 *
 * @internal
 */
#[CoversClass(DoctrineTransport::class)]
final class ConcurrentConsumptionTest extends TestCase
{
    protected DBALConnection $dbalConnection;

    protected string $tableName = 'messenger_messages_test';

    private PhpSerializer $serializer;

    public function testMultipleConsumersProcessMessagesConcurrently(): void
    {
        $messageCount = 20;
        $producer = $this->arrangeMultipleConsumerTest($messageCount);
        $processedMessages = $this->actConcurrentConsumption($messageCount);
        $this->assertConcurrentProcessingResults($processedMessages, $messageCount, $producer);

        // 验证测试完成
        $this->assertGreaterThan(0, array_sum(array_map('count', $processedMessages)));
    }

    /**
     * 准备多消费者并发测试
     */
    private function arrangeMultipleConsumerTest(int $messageCount): DoctrineTransport
    {
        $producer = $this->createTransport('producer');

        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new TestMessage("message_{$i}", $i);
            $producer->send(new Envelope($message, []));
        }

        return $producer;
    }

    /**
     * 执行并发消费
     *
     * @return array<string, array<int>>
     */
    private function actConcurrentConsumption(int $messageCount): array
    {
        $consumers = $this->createNamedConsumers(['consumer1', 'consumer2', 'consumer3']);

        return $this->processConcurrentlyUntilComplete($consumers, $messageCount);
    }

    /**
     * 验证并发处理结果
     *
     * @param array<string, array<int>> $processedMessages
     */
    private function assertConcurrentProcessingResults(array $processedMessages, int $messageCount, DoctrineTransport $producer): void
    {
        // 所有消息都被处理
        $totalProcessed = array_sum(array_map('count', $processedMessages));
        $this->assertEquals($messageCount, $totalProcessed);

        // 没有消息被重复处理
        $allProcessedIndexes = $this->extractAllProcessedIndexes($processedMessages);
        $this->assertCount($messageCount, $allProcessedIndexes);
        $this->assertCount($messageCount, array_unique($allProcessedIndexes));

        // 每个消费者都处理了一些消息（负载均衡）
        $this->assertLoadBalancing($processedMessages);

        // 数据库中没有剩余消息
        $this->assertEquals(0, $producer->getMessageCount());
    }

    /**
     * 准备慢消费者测试
     */
    private function arrangeSlowConsumerTest(int $messageCount): DoctrineTransport
    {
        $producer = $this->createTransport();

        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new TestMessage("message_{$i}", $i);
            $producer->send(new Envelope($message, []));
        }

        return $producer;
    }

    /**
     * 执行慢快消费者测试
     *
     * @return array{slowConsumer: DoctrineTransport, slowEnvelopes: array<Envelope>, slowMessage: mixed, fastProcessedCount: int, fastProcessedMessages: array<mixed>}
     */
    private function actSlowAndFastConsumption(): array
    {
        $slowConsumer = $this->createTransport('slow');
        $fastConsumer = $this->createTransport('fast');

        // 慢消费者获取消息但不确认
        $slowEnvelopes = iterator_to_array($slowConsumer->get());
        $this->assertCount(1, $slowEnvelopes);
        $slowMessage = $slowEnvelopes[0]->getMessage();
        $this->assertInstanceOf(TestMessage::class, $slowMessage);
        $slowMessageContent = $slowMessage->content;

        // 快消费者处理消息
        $fastResult = $this->processFastConsumer($fastConsumer, 5);

        return [
            'slowConsumer' => $slowConsumer,
            'slowEnvelopes' => $slowEnvelopes,
            'slowMessage' => $slowMessage,
            'fastProcessedCount' => $fastResult['count'],
            'fastProcessedMessages' => $fastResult['messages'],
        ];
    }

    /**
     * 验证慢消费者不阻塞其他消费者
     *
     * @param array{slowConsumer: DoctrineTransport, slowEnvelopes: array<Envelope>, slowMessage: mixed, fastProcessedCount: int, fastProcessedMessages: array<mixed>} $result
     */
    private function assertSlowConsumerDoesNotBlock(array $result, int $messageCount, DoctrineTransport $producer): void
    {
        $this->assertEquals(5, $result['fastProcessedCount']);
        $this->assertNotContains($result['slowMessage'], $result['fastProcessedMessages']);

        // 慢消费者确认消息
        $this->assertInstanceOf(DoctrineTransport::class, $result['slowConsumer']);
        $this->assertIsArray($result['slowEnvelopes']);
        $this->assertArrayHasKey(0, $result['slowEnvelopes']);
        $result['slowConsumer']->ack($result['slowEnvelopes'][0]);

        // 验证剩余消息数量
        $this->assertEquals($messageCount - 6, $producer->getMessageCount());
    }

    /**
     * 创建指定名称的消费者
     *
     * @param list<string> $names
     * @return array<string, DoctrineTransport>
     */
    private function createNamedConsumers(array $names): array
    {
        $consumers = [];
        foreach ($names as $name) {
            $consumers[$name] = $this->createTransport($name);
        }

        return $consumers;
    }

    /**
     * 并发处理直到完成
     *
     * @param array<string, DoctrineTransport> $consumers
     * @return array<string, array<int>>
     */
    private function processConcurrentlyUntilComplete(array $consumers, int $targetCount): array
    {
        /** @var array<string, array<int>> $processedMessages */
        $processedMessages = [];
        foreach (array_keys($consumers) as $consumerName) {
            /** @var array<int> $emptyArray */
            $emptyArray = [];
            $processedMessages[$consumerName] = $emptyArray;
        }
        $totalProcessed = 0;

        while ($totalProcessed < $targetCount) {
            foreach ($consumers as $name => $consumer) {
                /** @var array<string, array<int>> $processedMessages */
                $result = $this->processOneMessageFromConsumer($consumer, $name, $processedMessages);
                if ($result['processed']) {
                    $processedMessages = $result['updatedMessages'];
                    ++$totalProcessed;
                }
            }
        }

        return $processedMessages;
    }

    /**
     * 从单个消费者处理一条消息
     *
     * @param array<string, array<int>> $processedMessages
     * @return array{processed: bool, updatedMessages: array<string, array<int>>}
     */
    private function processOneMessageFromConsumer(DoctrineTransport $consumer, string $name, array $processedMessages): array
    {
        $envelopes = iterator_to_array($consumer->get());
        if ([] === $envelopes) {
            return ['processed' => false, 'updatedMessages' => $processedMessages];
        }

        $envelope = $envelopes[0];
        $message = $envelope->getMessage();
        $this->assertTrue(property_exists($message, 'index'));
        if (!isset($processedMessages[$name])) {
            /** @var array<int> $emptyList */
            $emptyList = [];
            $processedMessages[$name] = $emptyList;
        }
        $this->assertInstanceOf(TestMessage::class, $message);
        $processedMessages[$name][] = $message->index;

        usleep(10000); // 模拟处理时间
        $consumer->ack($envelope);

        return ['processed' => true, 'updatedMessages' => $processedMessages];
    }

    /**
     * 处理快速消费者
     *
     * @return array{count: int, messages: array<string>}
     */
    private function processFastConsumer(DoctrineTransport $consumer, int $maxCount): array
    {
        $processedCount = 0;
        /** @var array<string> $processedMessages */
        $processedMessages = [];

        while ($processedCount < $maxCount) {
            $envelopes = iterator_to_array($consumer->get());
            if ([] === $envelopes) {
                break;
            }

            $envelope = $envelopes[0];
            $message = $envelope->getMessage();
            $this->assertInstanceOf(TestMessage::class, $message);
            // content已经是字符串类型
            $processedMessages[] = $message->content;
            $consumer->ack($envelope);
            ++$processedCount;
        }

        return ['count' => $processedCount, 'messages' => $processedMessages];
    }

    /**
     * 提取所有已处理的索引
     *
     * @param array<string, array<int>> $processedMessages
     * @return array<int>
     */
    private function extractAllProcessedIndexes(array $processedMessages): array
    {
        return array_merge(...array_values($processedMessages));
    }

    /**
     * 验证负载均衡
     *
     * @param array<string, array<int>> $processedMessages
     */
    private function assertLoadBalancing(array $processedMessages): void
    {
        foreach ($processedMessages as $consumerName => $messages) {
            $this->assertNotEmpty($messages, "Consumer {$consumerName} should have processed at least one message");
        }
    }

    /**
     * 创建独立的 transport 实例，模拟多个消费者
     */
    private function createTransport(string $consumerId = 'default'): DoctrineTransport
    {
        $options = array_merge($this->getConnectionOptions(), [
            'consumer_id' => $consumerId, // 用于调试
        ]);

        $connection = new Connection($options, $this->dbalConnection);

        return new DoctrineTransport($connection, $this->serializer);
    }

    public function testConcurrentGetDoesNotReturnSameMessage(): void
    {
        // Arrange
        $transport = $this->createTransport();

        // 发送一条消息
        $message = new TestMessage('single message');
        $transport->send(new Envelope($message, []));

        // Act - 创建多个连接同时获取
        $connection1 = new Connection($this->getConnectionOptions(), $this->dbalConnection);
        $connection2 = new Connection($this->getConnectionOptions(), $this->dbalConnection);

        // 模拟并发获取
        $envelope1 = $connection1->get();
        $envelope2 = $connection2->get();

        // Assert
        // 只有一个连接能获取到消息
        $this->assertTrue(
            (null !== $envelope1 && null === $envelope2)
            || (null === $envelope1 && null !== $envelope2),
            'Only one connection should get the message'
        );
    }

    public function testSlowConsumerDoesNotBlockOthers(): void
    {
        $messageCount = 10;
        $producer = $this->arrangeSlowConsumerTest($messageCount);
        $result = $this->actSlowAndFastConsumption();
        $this->assertSlowConsumerDoesNotBlock($result, $messageCount, $producer);

        // 确保测试有断言
        $this->assertNotEmpty($result);
    }

    public function testConnectionFailureDoesNotLoseMessages(): void
    {
        // Arrange
        $transport = $this->createTransport();

        $message = new TestMessage('important message');
        $transport->send(new Envelope($message, []));

        // Act - 消费者1获取消息但"崩溃"（不调用ack）
        $consumer1 = $this->createTransport('consumer1');
        $envelopes = iterator_to_array($consumer1->get());
        $this->assertCount(1, $envelopes);

        // 模拟消费者崩溃 - 不调用 ack，连接被释放
        unset($consumer1);

        // 设置较短的重投递超时以加快测试
        $options = array_merge($this->getConnectionOptions(), ['redeliver_timeout' => 1]);
        $connection2 = new Connection($options, $this->dbalConnection);
        $consumer2 = new DoctrineTransport($connection2, $this->serializer);

        // 立即尝试获取 - 应该失败因为消息还在被"处理"
        $immediateResult = $consumer2->get();
        $this->assertEmpty($immediateResult);

        // 等待重投递超时
        sleep(2);

        // Assert - 消息应该可以被重新获取
        $redeliveredEnvelopes = $consumer2->get();
        $this->assertCount(1, $redeliveredEnvelopes);
        $redeliveredEnvelopesArray = iterator_to_array($redeliveredEnvelopes);
        $redeliveredMessage = $redeliveredEnvelopesArray[0]->getMessage();
        $this->assertTrue(property_exists($redeliveredMessage, 'content'));
        $this->assertInstanceOf(TestMessage::class, $redeliveredMessage);
        $this->assertEquals('important message', $redeliveredMessage->content);

        // 确认消息
        $consumer2->ack($redeliveredEnvelopesArray[0]);
        $this->assertEquals(0, $consumer2->getMessageCount());
    }

    public function testHighThroughputMaintainsMessageIntegrity(): void
    {
        $messageCount = 100;
        $sentMessages = $this->prepareHighThroughputMessages($messageCount);
        $processedMessages = $this->processMessagesWithMultipleConsumers($messageCount, 5);
        $this->assertMessageIntegrity($sentMessages, $processedMessages, $messageCount);

        // 确保测试有断言
        $this->assertCount($messageCount, $sentMessages);
    }

    /**
     * 准备高吞吐量测试的消息
     *
     * @return array<string, TestMessage>
     */
    private function prepareHighThroughputMessages(int $messageCount): array
    {
        $producer = $this->createTransport();
        $sentMessages = [];

        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new TestMessage('', $i);
            $sentMessages[$message->id] = $message;

            $producer->send(new Envelope($message, []));
        }

        return $sentMessages;
    }

    /**
     * 使用多个消费者处理消息
     *
     * @return array<string, array<int>>
     */
    private function processMessagesWithMultipleConsumers(int $messageCount, int $consumerCount): array
    {
        $consumers = $this->createMultipleConsumers($consumerCount);
        /** @var array<string, array<int>> $processedMessages */
        $processedMessages = [];
        for ($i = 0; $i < $consumerCount; ++$i) {
            /** @var array<int> $emptyArray */
            $emptyArray = [];
            $processedMessages['consumer_' . $i] = $emptyArray;
        }
        $processedCount = 0;

        while ($processedCount < $messageCount) {
            $result = $this->processSingleRound($consumers, $processedMessages, $processedCount);
            $processedMessages = $result['updatedMessages'];
            $processedCount = $result['updatedCount'];

            if (!$result['hasProcessed']) {
                usleep(1000); // 1ms
            }
        }

        return $processedMessages;
    }

    /**
     * 创建多个消费者
     */
    /**
     * @return array<DoctrineTransport>
     */
    private function createMultipleConsumers(int $count): array
    {
        $consumers = [];
        for ($i = 0; $i < $count; ++$i) {
            $consumers[] = $this->createTransport("consumer_{$i}");
        }

        return $consumers;
    }

    /**
     * 处理单轮消费
     *
     * @param array<DoctrineTransport> $consumers
     * @param array<string, array<int>> $processedMessages
     * @return array{hasProcessed: bool, updatedMessages: array<string, array<int>>, updatedCount: int}
     */
    private function processSingleRound(array $consumers, array $processedMessages, int $processedCount): array
    {
        $hasProcessed = false;

        foreach ($consumers as $consumer) {
            $envelopes = $consumer->get();
            $envelopesArray = iterator_to_array($envelopes);
            if ([] !== $envelopesArray) {
                /** @var array<string, array<int>> $processedMessages */
                $result = $this->processEnvelope($consumer, $envelopesArray[0], $processedMessages, $processedCount);
                $processedMessages = $result['updatedMessages'];
                $processedCount = $result['updatedCount'];
                $hasProcessed = true;
            }
        }

        return ['hasProcessed' => $hasProcessed, 'updatedMessages' => $processedMessages, 'updatedCount' => $processedCount];
    }

    /**
     * 处理单个信封
     *
     * @param array<string, array<int>> $processedMessages
     * @return array{updatedMessages: array<string, array<int>>, updatedCount: int}
     */
    private function processEnvelope(DoctrineTransport $consumer, Envelope $envelope, array $processedMessages, int $processedCount): array
    {
        $message = $envelope->getMessage();
        $this->assertTrue(property_exists($message, 'index'));
        $consumerName = 'consumer_' . ($processedCount % 5);
        if (!isset($processedMessages[$consumerName])) {
            /** @var array<int> $emptyList */
            $emptyList = [];
            $processedMessages[$consumerName] = $emptyList;
        }
        $this->assertInstanceOf(TestMessage::class, $message);
        $processedMessages[$consumerName][] = $message->index;
        $consumer->ack($envelope);
        ++$processedCount;

        return ['updatedMessages' => $processedMessages, 'updatedCount' => $processedCount];
    }

    /**
     * 验证消息完整性
     *
     * @param array<string, TestMessage> $sentMessages
     * @param array<string, array<int>> $processedMessages
     */
    private function assertMessageIntegrity(array $sentMessages, array $processedMessages, int $messageCount): void
    {
        // 1. 收集所有处理的消息索引
        $allProcessedIndexes = [];
        foreach ($processedMessages as $indexes) {
            $allProcessedIndexes = array_merge($allProcessedIndexes, $indexes);
        }

        // 2. 验证处理数量
        $this->assertCount($messageCount, $allProcessedIndexes);

        // 3. 验证没有重复
        $uniqueIndexes = array_unique($allProcessedIndexes);
        $this->assertCount($messageCount, $uniqueIndexes);

        // 4. 验证所有消息都被处理
        $expectedIndexes = range(0, $messageCount - 1);
        sort($allProcessedIndexes);
        $this->assertEquals($expectedIndexes, $allProcessedIndexes);

        // 5. 数据库为空
        $producer = $this->createTransport();
        $this->assertEquals(0, $producer->getMessageCount());
    }

    public function testAck(): void
    {
        // Arrange
        $transport = $this->createTransport();
        $message = new TestMessage('test ack');
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
        $transport = $this->createTransport();
        $messageCount = 3;

        for ($i = 0; $i < $messageCount; ++$i) {
            $message = new TestMessage("message {$i}", $i);
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
        $transport = $this->createTransport();
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
        $transport = $this->createTransport();
        $message = new TestMessage('findable message');
        $sentEnvelope = $transport->send(new Envelope($message, []));

        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $this->assertNotNull($transportIdStamp);
        $messageId = $transportIdStamp->getId();

        // Act
        $foundEnvelope = $transport->find($messageId);

        // Assert
        $this->assertInstanceOf(Envelope::class, $foundEnvelope);
        $message = $foundEnvelope->getMessage();
        $this->assertTrue(property_exists($message, 'content'));
        $this->assertInstanceOf(TestMessage::class, $message);
        $this->assertEquals('findable message', $message->content);

        // 测试找不到的消息
        $notFound = $transport->find('non-existent-id');
        $this->assertNull($notFound);
    }

    public function testGet(): void
    {
        // Arrange
        $transport = $this->createTransport();
        $message = new TestMessage('test get');
        $transport->send(new Envelope($message, []));

        // Act
        $envelopes = iterator_to_array($transport->get());

        // Assert
        $this->assertCount(1, $envelopes);
        $this->assertInstanceOf(Envelope::class, $envelopes[0]);
        $message = $envelopes[0]->getMessage();
        $this->assertTrue(property_exists($message, 'content'));
        $this->assertInstanceOf(TestMessage::class, $message);
        $this->assertEquals('test get', $message->content);
    }

    public function testKeepalive(): void
    {
        // Arrange
        $transport = $this->createTransport();
        $message = new TestMessage('keepalive test');
        $transport->send(new Envelope($message, []));

        $envelopes = iterator_to_array($transport->get());
        $envelope = $envelopes[0];

        // Act
        $transport->keepalive($envelope);

        // Assert - 消息仍在数据库中且被锁定
        $sql = "SELECT COUNT(*) FROM {$this->tableName} WHERE delivered_at IS NOT NULL";
        $lockedCount = $this->dbalConnection->fetchOne($sql);
        $this->assertEquals(1, $lockedCount);
    }

    public function testReject(): void
    {
        // Arrange
        $transport = $this->createTransport();
        $message = new TestMessage('to be rejected');
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
        $transport = $this->createTransport();
        $message = new TestMessage('test send');
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
        $newTableName = 'messenger_test_setup';
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
}
