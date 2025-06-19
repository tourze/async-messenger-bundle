<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\TestCase;

abstract class DoctrineIntegrationTestCase extends TestCase
{
    protected DBALConnection $dbalConnection;
    protected string $tableName = 'messenger_messages_test';
    
    protected function setUp(): void
    {
        parent::setUp();
        
        // 使用内存数据库进行测试
        $this->dbalConnection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ]);
        
        // 创建测试表
        $this->createTestTable();
    }
    
    protected function createTestTable(): void
    {
        $schema = new Schema();
        $table = $schema->createTable($this->tableName);

        $table->addColumn('id', 'bigint')
            ->setAutoincrement(true)
            ->setNotnull(true);
        $table->addColumn('body', 'text')
            ->setNotnull(true);
        $table->addColumn('headers', 'text')
            ->setNotnull(true);
        $table->addColumn('queue_name', 'string')
            ->setLength(190)
            ->setNotnull(true);
        $table->addColumn('created_at', 'datetime')
            ->setNotnull(true);
        $table->addColumn('available_at', 'datetime')
            ->setNotnull(true);
        $table->addColumn('delivered_at', 'datetime')
            ->setNotnull(false);

        $table->setPrimaryKey(['id']);
        $table->addIndex(['queue_name']);
        $table->addIndex(['available_at']);
        $table->addIndex(['delivered_at']);

        $sql = $schema->toSql($this->dbalConnection->getDatabasePlatform());
        foreach ($sql as $query) {
            $this->dbalConnection->executeStatement($query);
        }
    }
    
    protected function tearDown(): void
    {
        // 清理测试表
        $this->dbalConnection->executeStatement("DROP TABLE IF EXISTS {$this->tableName}");
        $this->dbalConnection->close();

        parent::tearDown();
    }
    
    protected function getConnectionOptions(): array
    {
        return [
            'table_name' => $this->tableName,
            'queue_name' => 'test_queue',
            'redeliver_timeout' => 3600,
            'auto_setup' => false, // 我们已经手动创建表
        ];
    }
    
    protected function assertMessageInDatabase(string $id, array $expectedData): void
    {
        $sql = "SELECT * FROM {$this->tableName} WHERE id = ?";
        $message = $this->dbalConnection->fetchAssociative($sql, [$id]);
        
        $this->assertIsArray($message);
        
        foreach ($expectedData as $key => $expectedValue) {
            $this->assertArrayHasKey($key, $message);
            $this->assertEquals($expectedValue, $message[$key]);
        }
    }
    
    protected function assertMessageNotInDatabase(string $id): void
    {
        $sql = "SELECT COUNT(*) FROM {$this->tableName} WHERE id = ?";
        $count = $this->dbalConnection->fetchOne($sql, [$id]);
        
        $this->assertEquals(0, $count);
    }
    
    protected function getMessageCount(?string $queueName = null): int
    {
        $sql = "SELECT COUNT(*) FROM {$this->tableName}";
        $params = [];
        
        if ($queueName !== null) {
            $sql .= " WHERE queue_name = ?";
            $params[] = $queueName;
        }
        
        return (int) $this->dbalConnection->fetchOne($sql, $params);
    }
    
    protected function insertTestMessage(array $data): string
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