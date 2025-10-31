<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Schema\Schema;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Doctrine\SchemaManager;

/**
 * @internal
 */
#[CoversClass(SchemaManager::class)]
final class SchemaManagerTest extends TestCase
{
    private Connection $connection;

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
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    public function testSetupCreatesTableSuccessfully(): void
    {
        // Act
        $this->schemaManager->setup();

        // Assert
        $schemaManager = $this->connection->createSchemaManager();
        $this->assertTrue($schemaManager->tablesExist(['test_messages']));
    }

    public function testAddTableToSchemaCreatesCorrectTableStructure(): void
    {
        // Arrange
        $schema = new Schema();

        // Act
        $this->schemaManager->addTableToSchema($schema);

        // Assert
        $this->assertTrue($schema->hasTable('test_messages'));
        $table = $schema->getTable('test_messages');

        // Check columns
        $this->assertTrue($table->hasColumn('id'));
        $this->assertTrue($table->hasColumn('body'));
        $this->assertTrue($table->hasColumn('headers'));
        $this->assertTrue($table->hasColumn('queue_name'));
        $this->assertTrue($table->hasColumn('created_at'));
        $this->assertTrue($table->hasColumn('available_at'));
        $this->assertTrue($table->hasColumn('delivered_at'));

        // Check indexes - we expect at least a few indexes
        $indexes = $table->getIndexes();
        $this->assertGreaterThanOrEqual(1, count($indexes));

        // Check that we have a primary key by checking the unique constraints
        $uniqueConstraints = $table->getUniqueConstraints();
        $this->assertGreaterThanOrEqual(1, count($uniqueConstraints));
    }

    public function testSetupWorksWithSqlitePlatform(): void
    {
        // Act & Assert - Should not throw exception
        $this->schemaManager->setup();

        // Verify the platform
        $this->assertInstanceOf(SQLitePlatform::class, $this->connection->getDatabasePlatform());
    }

    public function testSetupRestoresOriginalSchemaFilter(): void
    {
        // Arrange
        $originalFilter = $this->connection->getConfiguration()->getSchemaAssetsFilter();

        // Act
        $this->schemaManager->setup();

        // Assert
        $currentFilter = $this->connection->getConfiguration()->getSchemaAssetsFilter();
        $this->assertSame($originalFilter, $currentFilter);
    }
}
