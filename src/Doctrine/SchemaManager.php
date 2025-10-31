<?php

namespace Tourze\AsyncMessengerBundle\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Schema\AbstractAsset;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaDiff;
use Doctrine\DBAL\Types\Types;

/**
 * 负责数据库架构管理的类
 */
class SchemaManager
{
    private const ORACLE_SEQUENCES_SUFFIX = '_seq';
    private const TABLE_OPTION_NAME = '_symfony_messenger_table_name';

    /**
     * @param array<string, mixed> $configuration
     */
    public function __construct(
        private readonly DBALConnection $connection,
        private readonly array $configuration,
    ) {
    }

    public function setup(): void
    {
        $configuration = $this->connection->getConfiguration();
        $assetFilter = $configuration->getSchemaAssetsFilter();
        $configuration->setSchemaAssetsFilter(function ($tableName) {
            if ($tableName instanceof AbstractAsset) {
                $tableName = $tableName->getName();
            }

            if (!\is_string($tableName)) {
                throw new \TypeError(\sprintf('The table name must be an instance of "%s" or a string ("%s" given).', AbstractAsset::class, get_debug_type($tableName)));
            }

            $expectedTableName = $this->configuration['table_name'] ?? 'messenger_messages';

            return $tableName === $expectedTableName;
        });
        $this->updateSchema();
        $configuration->setSchemaAssetsFilter($assetFilter);
    }

    private function updateSchema(): void
    {
        $schemaManager = $this->connection->createSchemaManager();
        $schemaDiff = $schemaManager->createComparator()
            ->compareSchemas($schemaManager->introspectSchema(), $this->getSchema())
        ;
        $platform = $this->connection->getDatabasePlatform();

        $this->createSchemas($platform, $schemaDiff);
        $this->createSequences($platform, $schemaDiff);
        $this->createTables($platform, $schemaDiff);
        $this->alterTables($platform, $schemaDiff);
    }

    private function createSchemas(AbstractPlatform $platform, SchemaDiff $schemaDiff): void
    {
        if (!$platform->supportsSchemas()) {
            return;
        }

        foreach ($schemaDiff->getCreatedSchemas() as $schema) {
            $this->connection->executeStatement($platform->getCreateSchemaSQL($schema));
        }
    }

    private function createSequences(AbstractPlatform $platform, SchemaDiff $schemaDiff): void
    {
        if (!$platform->supportsSequences()) {
            return;
        }

        foreach ($schemaDiff->getAlteredSequences() as $sequence) {
            $this->connection->executeStatement($platform->getAlterSequenceSQL($sequence));
        }

        foreach ($schemaDiff->getCreatedSequences() as $sequence) {
            $this->connection->executeStatement($platform->getCreateSequenceSQL($sequence));
        }
    }

    private function createTables(AbstractPlatform $platform, SchemaDiff $schemaDiff): void
    {
        foreach ($platform->getCreateTablesSQL($schemaDiff->getCreatedTables()) as $sql) {
            $this->connection->executeStatement($sql);
        }
    }

    private function alterTables(AbstractPlatform $platform, SchemaDiff $schemaDiff): void
    {
        foreach ($schemaDiff->getAlteredTables() as $tableDiff) {
            foreach ($platform->getAlterTableSQL($tableDiff) as $sql) {
                $this->connection->executeStatement($sql);
            }
        }
    }

    private function getSchema(): Schema
    {
        $schema = new Schema([], [], $this->connection->createSchemaManager()->createSchemaConfig());
        $this->addTableToSchema($schema);

        return $schema;
    }

    public function addTableToSchema(Schema $schema): void
    {
        $tableNameValue = $this->configuration['table_name'] ?? 'messenger_messages';
        $tableName = is_scalar($tableNameValue) ? (string) $tableNameValue : 'messenger_messages';
        $table = $schema->createTable($tableName);
        // add an internal option to mark that we created this & the non-namespaced table name
        $table->addOption(self::TABLE_OPTION_NAME, $tableName);
        $idColumn = $table->addColumn('id', Types::BIGINT)
            ->setAutoincrement(true)
            ->setNotnull(true)
        ;
        $table->addColumn('body', Types::TEXT)
            ->setNotnull(true)
        ;
        $table->addColumn('headers', Types::TEXT)
            ->setNotnull(true)
        ;
        $table->addColumn('queue_name', Types::STRING)
            ->setLength(190) // MySQL 5.6 only supports 191 characters on an indexed column in utf8mb4 mode
            ->setNotnull(true)
        ;
        $table->addColumn('created_at', Types::DATETIME_IMMUTABLE)
            ->setNotnull(true)
        ;
        $table->addColumn('available_at', Types::DATETIME_IMMUTABLE)
            ->setNotnull(true)
        ;
        $table->addColumn('delivered_at', Types::DATETIME_IMMUTABLE)
            ->setNotnull(false)
        ;
        $table->addUniqueConstraint(['id'], 'PRIMARY');
        $table->addIndex(['queue_name']);
        $table->addIndex(['available_at']);
        $table->addIndex(['delivered_at']);

        // We need to create a sequence for Oracle and set the id column to get the correct nextval
        if ($this->connection->getDatabasePlatform() instanceof OraclePlatform) {
            $idColumn->setDefault($tableName . self::ORACLE_SEQUENCES_SUFFIX . '.nextval');

            $schema->createSequence($tableName . self::ORACLE_SEQUENCES_SUFFIX);
        }
    }
}
