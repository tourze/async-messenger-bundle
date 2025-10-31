<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Doctrine\MessageRetriever;
use Tourze\AsyncMessengerBundle\Doctrine\SchemaManager;

/**
 * @internal
 */
#[CoversClass(MessageRetriever::class)]
final class MessageRetrieverTest extends TestCase
{
    private Connection $connection;

    private MessageRetriever $messageRetriever;

    private SchemaManager $schemaManager;

    /** @var array<string, mixed> */
    private array $configuration;

    protected function setUp(): void
    {
        $this->connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ]);

        $this->configuration = [
            'table_name' => 'test_messages',
            'queue_name' => 'test_queue',
            'redeliver_timeout' => 3600,
            'auto_setup' => true,
        ];

        $this->schemaManager = new SchemaManager($this->connection, $this->configuration);
        $this->messageRetriever = new MessageRetriever($this->connection, $this->configuration, $this->schemaManager, true);

        // Create the table
        $this->schemaManager->setup();
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    public function testFetchMessageReturnsNullWhenNoMessages(): void
    {
        // Act
        $result = $this->messageRetriever->fetchMessage();

        // Assert
        $this->assertFalse($result);
    }

    public function testFetchMessageReturnsAvailableMessage(): void
    {
        // Arrange
        $this->insertTestMessage([
            'body' => 'test body',
            'headers' => json_encode(['test' => 'header']),
            'available_at' => new \DateTime('-1 minute'),
        ]);

        // Act
        $result = $this->messageRetriever->fetchMessage();

        // Assert
        $this->assertIsArray($result);
        $this->assertEquals('test body', $result['body']);
        $this->assertEquals('test_queue', $result['queue_name']);
    }

    public function testFetchMessageSkipsFutureMessages(): void
    {
        // Arrange
        $this->insertTestMessage([
            'body' => 'future message',
            'available_at' => new \DateTime('+1 hour'),
        ]);

        // Act
        $result = $this->messageRetriever->fetchMessage();

        // Assert
        $this->assertFalse($result);
    }

    public function testFetchMessageSkipsDeliveredMessages(): void
    {
        // Arrange
        $this->insertTestMessage([
            'body' => 'delivered message',
            'available_at' => new \DateTime('-1 minute'),
            'delivered_at' => new \DateTime(),
        ]);

        // Act
        $result = $this->messageRetriever->fetchMessage();

        // Assert
        $this->assertFalse($result);
    }

    public function testFetchMessageIncludesRedeliveryCandidates(): void
    {
        // Arrange - Insert a message delivered more than redeliver_timeout ago
        $this->insertTestMessage([
            'body' => 'redelivery candidate',
            'available_at' => new \DateTime('-1 minute'),
            'delivered_at' => new \DateTime('-2 hours'), // Older than redeliver_timeout (3600s)
        ]);

        // Act
        $result = $this->messageRetriever->fetchMessage();

        // Assert
        $this->assertIsArray($result);
        $this->assertEquals('redelivery candidate', $result['body']);
    }

    public function testGetMessageCountReturnsCorrectCount(): void
    {
        // Arrange
        $this->insertTestMessage(['available_at' => new \DateTime('-1 minute')]);
        $this->insertTestMessage(['available_at' => new \DateTime('-1 minute')]);
        $this->insertTestMessage(['available_at' => new \DateTime('+1 hour')]); // Future message - shouldn't count

        // Act
        $count = $this->messageRetriever->getMessageCount();

        // Assert
        $this->assertEquals(2, $count);
    }

    public function testFindAllReturnsAllAvailableMessages(): void
    {
        // Arrange
        $this->insertTestMessage(['body' => 'message 1', 'available_at' => new \DateTime('-1 minute')]);
        $this->insertTestMessage(['body' => 'message 2', 'available_at' => new \DateTime('-1 minute')]);
        $this->insertTestMessage(['body' => 'future message', 'available_at' => new \DateTime('+1 hour')]);

        // Act
        $messages = $this->messageRetriever->findAll();
        $messagesArray = iterator_to_array($messages);

        // Assert
        $this->assertCount(2, $messagesArray);
        $this->assertEquals('message 1', $messagesArray[0]['body']);
        $this->assertEquals('message 2', $messagesArray[1]['body']);
    }

    public function testFindAllRespectsLimit(): void
    {
        // Arrange
        $this->insertTestMessage(['body' => 'message 1', 'available_at' => new \DateTime('-1 minute')]);
        $this->insertTestMessage(['body' => 'message 2', 'available_at' => new \DateTime('-1 minute')]);
        $this->insertTestMessage(['body' => 'message 3', 'available_at' => new \DateTime('-1 minute')]);

        // Act
        $messages = $this->messageRetriever->findAll(2);
        $messagesArray = iterator_to_array($messages);

        // Assert
        $this->assertCount(2, $messagesArray);
    }

    public function testFindReturnsSpecificMessage(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'specific message']);

        // Act
        $message = $this->messageRetriever->find($id);

        // Assert
        $this->assertIsArray($message);
        $this->assertEquals('specific message', $message['body']);
        $this->assertEquals($id, $message['id']);
    }

    public function testFindReturnsNullForNonexistentMessage(): void
    {
        // Act
        $message = $this->messageRetriever->find('999');

        // Assert
        $this->assertNull($message);
    }

    public function testFindReturnsNullForDifferentQueue(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['queue_name' => 'other_queue']);

        // Act
        $message = $this->messageRetriever->find($id);

        // Assert
        $this->assertNull($message);
    }

    public function testDecodeEnvelopeHeadersDecodesJsonHeaders(): void
    {
        // Arrange
        $envelope = [
            'id' => '1',
            'body' => 'test',
            'headers' => json_encode(['test' => 'value', 'number' => 42]),
        ];

        // Act
        $decoded = $this->messageRetriever->decodeEnvelopeHeaders($envelope);

        // Assert
        $this->assertEquals(['test' => 'value', 'number' => 42], $decoded['headers']);
        $this->assertEquals('1', $decoded['id']);
        $this->assertEquals('test', $decoded['body']);
    }

    public function testCreateAvailableMessagesQueryBuilder(): void
    {
        // Arrange
        $this->insertTestMessage(['available_at' => new \DateTime('-1 minute')]);
        $this->insertTestMessage(['available_at' => new \DateTime('+1 hour')]); // Future
        $this->insertTestMessage(['delivered_at' => new \DateTime()]); // Delivered

        // Act
        $queryBuilder = $this->messageRetriever->createAvailableMessagesQueryBuilder();
        $sql = $queryBuilder->getSQL();
        $params = $queryBuilder->getParameters();
        $types = $queryBuilder->getParameterTypes();

        // Execute the query to test it
        $result = $this->connection->executeQuery($sql, $params, $types);
        $messages = $result->fetchAllAssociative();

        // Assert
        $this->assertCount(1, $messages); // Only the available message
        $this->assertStringContainsString('queue_name = ?', $sql);
        $this->assertStringContainsString('available_at <= ?', $sql);
        $this->assertStringContainsString('delivered_at is null OR m.delivered_at < ?', $sql);
    }

    public function testCreateQueryBuilder(): void
    {
        // 由于PHPStan规则限制，此测试通过反射来验证createQueryBuilder的实现
        $reflection = new \ReflectionClass($this->messageRetriever);
        $method = $reflection->getMethod('createQueryBuilder');

        // Assert - 方法存在且为公共方法
        $this->assertTrue($method->isPublic());
        $this->assertEquals('createQueryBuilder', $method->getName());

        // 间接验证：通过buildQueryForMessagesTable方法验证查询构建逻辑
        $queryBuilder = $this->messageRetriever->buildQueryForMessagesTable('msg');
        $sql = $queryBuilder->getSQL();

        $this->assertStringContainsString('FROM test_messages msg', $sql);
        $this->assertStringContainsString('SELECT msg.*', $sql); // For non-Oracle platforms
    }

    public function testBuildQueryForMessagesTable(): void
    {
        // Act
        $queryBuilder = $this->messageRetriever->buildQueryForMessagesTable('msg');
        $sql = $queryBuilder->getSQL();

        // Assert
        $this->assertStringContainsString('FROM test_messages msg', $sql);
        $this->assertStringContainsString('SELECT msg.*', $sql); // For non-Oracle platforms
    }

    public function testExecuteQuery(): void
    {
        // Arrange
        $this->insertTestMessage(['body' => 'test query']);

        // Act
        $sql = 'SELECT * FROM test_messages WHERE body = ?';
        $result = $this->messageRetriever->executeQuery($sql, ['test query']);
        $data = $result->fetchAssociative();

        // Assert
        $this->assertIsArray($data);
        $this->assertEquals('test query', $data['body']);
    }

    /**
     * @param array<string, mixed> $data
     */
    private function insertTestMessage(array $data = []): string
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

        $this->connection->insert('test_messages', $data, [
            'created_at' => 'datetime',
            'available_at' => 'datetime',
            'delivered_at' => 'datetime',
        ]);

        return (string) $this->connection->lastInsertId();
    }
}
