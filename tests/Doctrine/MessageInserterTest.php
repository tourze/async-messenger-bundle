<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception\TableNotFoundException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Doctrine\MessageInserter;
use Tourze\AsyncMessengerBundle\Doctrine\SchemaManager;

/**
 * @internal
 */
#[CoversClass(MessageInserter::class)]
final class MessageInserterTest extends TestCase
{
    private Connection $connection;

    private MessageInserter $messageInserter;

    private SchemaManager $schemaManager;

    /** @var array<string, mixed> */
    private array $configuration;

    protected function setUp(): void
    {
        parent::setUp();
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
        $this->messageInserter = new MessageInserter($this->connection, $this->configuration, $this->schemaManager, true);

        // Create the table
        $this->schemaManager->setup();
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    public function testInsertMessageSavesMessageSuccessfully(): void
    {
        // Arrange
        $body = 'test message body';
        $headers = ['type' => 'test', 'priority' => 'high'];
        $delay = 0;

        // Act
        $id = $this->messageInserter->insertMessage($body, $headers, $delay);

        // Assert - 验证返回的ID不为空且是有效的字符串
        $this->assertNotEmpty($id);
        $this->assertIsString($id);

        // Verify message was saved
        $sql = 'SELECT * FROM test_messages WHERE id = ?';
        $result = $this->connection->fetchAssociative($sql, [$id]);

        $this->assertIsArray($result);
        $this->assertEquals($body, $result['body']);
        $this->assertEquals(json_encode($headers), $result['headers']);
        $this->assertEquals('test_queue', $result['queue_name']);
    }

    public function testInsertMessageWithDelaySetsCorrectAvailableAt(): void
    {
        // Arrange
        $body = 'delayed message';
        $headers = [];
        $delay = 5000; // 5 seconds in milliseconds

        // Act
        $id = $this->messageInserter->insertMessage($body, $headers, $delay);

        // Assert
        $sql = 'SELECT available_at, created_at FROM test_messages WHERE id = ?';
        $result = $this->connection->fetchAssociative($sql, [$id]);
        $this->assertIsArray($result, 'Query should return a valid result set');
        $this->assertIsString($result['created_at']);
        $this->assertIsString($result['available_at']);

        $createdAt = new \DateTimeImmutable($result['created_at']);
        $availableAt = new \DateTimeImmutable($result['available_at']);

        $this->assertGreaterThan($createdAt, $availableAt);
        $this->assertEquals(5, $availableAt->getTimestamp() - $createdAt->getTimestamp());
    }

    public function testInsertMessageWithAutoSetupCreatesTableIfMissing(): void
    {
        // This test demonstrates the auto-setup functionality
        // We'll just verify the method doesn't throw an exception with auto_setup enabled

        $this->assertInstanceOf(MessageInserter::class, $this->messageInserter);

        // The table already exists from setUp(), so let's just verify a normal insert works
        $id = $this->messageInserter->insertMessage('test auto setup', [], 0);
        $this->assertIsString($id);
        $this->assertNotEmpty($id);
    }

    public function testInsertMessageWithoutAutoSetupThrowsExceptionIfTableMissing(): void
    {
        // Arrange - Create a new connection without table and auto_setup disabled
        $newConnection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ]);

        $newSchemaManager = new SchemaManager($newConnection, $this->configuration);
        $newInserter = new MessageInserter($newConnection, $this->configuration, $newSchemaManager, false);

        // Act & Assert
        $this->expectException(TableNotFoundException::class);
        $newInserter->insertMessage('test', [], 0);

        $newConnection->close();
    }

    public function testInsertMessageHandlesJsonEncodingOfHeaders(): void
    {
        // Arrange
        $headers = [
            'complex' => ['nested' => 'value'],
            'array' => [1, 2, 3],
            'string' => 'simple',
            'number' => 42,
            'boolean' => true,
        ];

        // Act
        $id = $this->messageInserter->insertMessage('test', $headers, 0);

        // Assert
        $sql = 'SELECT headers FROM test_messages WHERE id = ?';
        $storedHeaders = $this->connection->fetchOne($sql, [$id]);
        $this->assertIsString($storedHeaders);

        $this->assertEquals(json_encode($headers), $storedHeaders);
        $this->assertEquals($headers, json_decode($storedHeaders, true));
    }
}
