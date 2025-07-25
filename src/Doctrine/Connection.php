<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Tourze\AsyncMessengerBundle\Doctrine;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Query\ForUpdate\ConflictResolutionMode;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\AbstractAsset;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Types;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Contracts\Service\ResetInterface;

/**
 * @internal
 *
 * @author Vincent Touzet <vincent.touzet@gmail.com>
 * @author Kévin Dunglas <dunglas@gmail.com>
 * @author Herberto Graca <herberto.graca@gmail.com>
 * @author Alexander Malyk <shu.rick.ifmo@gmail.com>
 */
class Connection implements ResetInterface
{
    private const ORACLE_SEQUENCES_SUFFIX = '_seq';
    protected const TABLE_OPTION_NAME = '_symfony_messenger_table_name';

    protected const DEFAULT_OPTIONS = [
        'table_name' => 'messenger_messages',
        'queue_name' => 'default',
        'redeliver_timeout' => 3600,
        'auto_setup' => true,
    ];

    protected ?float $queueEmptiedAt = null;

    private bool $autoSetup;
    private bool $doMysqlCleanup = false;

    /**
     * Constructor.
     *
     * Available options:
     *
     * * table_name: name of the table
     * * connection: name of the Doctrine's entity manager
     * * queue_name: name of the queue
     * * redeliver_timeout: Timeout before redeliver messages still in handling state (i.e: delivered_at is not null and message is still in table). Default: 3600
     * * auto_setup: Whether the table should be created automatically during send / get. Default: true
     */
    public function __construct(
        protected array $configuration,
        protected DBALConnection $driverConnection,
    ) {
        $this->configuration = array_replace_recursive(static::DEFAULT_OPTIONS, $configuration);
        $this->autoSetup = $this->configuration['auto_setup'];
    }

    public static function buildConfiguration(#[\SensitiveParameter] string $dsn, array $options = []): array
    {
        if (false === $params = parse_url($dsn)) {
            return [];
        }

        $query = [];
        if (isset($params['query'])) {
            parse_str($params['query'], $query);
        }

        $configuration = [];
        $configuration += $query + $options + static::DEFAULT_OPTIONS;

        $configuration['auto_setup'] = filter_var($configuration['auto_setup'], \FILTER_VALIDATE_BOOL);

        // check for extra keys in options
        $optionsExtraKeys = array_diff(array_keys($options), array_keys(static::DEFAULT_OPTIONS));
        if (0 < \count($optionsExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found: [%s]. Allowed options are [%s].', implode(', ', $optionsExtraKeys), implode(', ', array_keys(static::DEFAULT_OPTIONS))));
        }

        // check for extra keys in options
        $queryExtraKeys = array_diff(array_keys($query), array_keys(static::DEFAULT_OPTIONS));
        if (0 < \count($queryExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found in DSN: [%s]. Allowed options are [%s].', implode(', ', $queryExtraKeys), implode(', ', array_keys(static::DEFAULT_OPTIONS))));
        }

        return $configuration;
    }

    public function reset(): void
    {
        $this->queueEmptiedAt = null;
        $this->doMysqlCleanup = false;
    }

    /**
     * @param int $delay The delay in milliseconds
     *
     * @return string The inserted id
     *
     * @throws DBALException
     */
    public function send(string $body, array $headers, int $delay = 0): string
    {
        $now = new \DateTimeImmutable();
        $availableAt = $now->modify(\sprintf('%+d seconds', $delay / 1000));

        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->insert($this->configuration['table_name'])
            ->values([
                'body' => '?',
                'headers' => '?',
                'queue_name' => '?',
                'created_at' => '?',
                'available_at' => '?',
            ]);

        return $this->executeInsert($queryBuilder->getSQL(), [
            $body,
            json_encode($headers),
            $this->configuration['queue_name'],
            $now,
            $availableAt,
        ], [
            Types::STRING,
            Types::STRING,
            Types::STRING,
            Types::DATETIME_IMMUTABLE,
            Types::DATETIME_IMMUTABLE,
        ]);
    }

    private function createQueryBuilder(string $alias = 'm'): QueryBuilder
    {
        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->from($this->configuration['table_name'], $alias);

        $alias .= '.';

        if (!$this->driverConnection->getDatabasePlatform() instanceof OraclePlatform) {
            return $queryBuilder->select($alias.'*');
        }

        // Oracle databases use UPPER CASE on tables and column identifiers.
        // Column alias is added to force the result to be lowercase even when the actual field is all caps.

        return $queryBuilder->select(str_replace(', ', ', '.$alias,
            $alias.'id AS "id", body AS "body", headers AS "headers", queue_name AS "queue_name", '.
            'created_at AS "created_at", available_at AS "available_at", '.
            'delivered_at AS "delivered_at"'
        ));
    }

    private function executeInsert(string $sql, array $parameters = [], array $types = []): string
    {
        // Use PostgreSQL RETURNING clause instead of lastInsertId() to get the
        // inserted id in one operation instead of two.
        if ($this->driverConnection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            $sql .= ' RETURNING id';
        }

        insert:
        $this->driverConnection->beginTransaction();

        try {
            if ($this->driverConnection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
                $first = $this->driverConnection->fetchFirstColumn($sql, $parameters, $types);

                $id = $first[0] ?? null;

                if (!$id) { // @phpstan-ignore-line
                    throw new TransportException('no id was returned by PostgreSQL from RETURNING clause.');
                }
            } elseif ($this->driverConnection->getDatabasePlatform() instanceof OraclePlatform) {
                $sequenceName = $this->configuration['table_name'].self::ORACLE_SEQUENCES_SUFFIX;

                $this->driverConnection->executeStatement($sql, $parameters, $types);

                $result = $this->driverConnection->fetchOne('SELECT '.$sequenceName.'.CURRVAL FROM DUAL');

                $id = (int) $result;

                if (!$id) { // @phpstan-ignore-line
                    throw new TransportException('no id was returned by Oracle from sequence: '.$sequenceName);
                }
            } else {
                $this->driverConnection->executeStatement($sql, $parameters, $types);

                if (!$id = $this->driverConnection->lastInsertId()) { // @phpstan-ignore-line
                    throw new TransportException('lastInsertId() returned false, no id was returned.');
                }
            }

            $this->driverConnection->commit();
        } catch (\Throwable $e) {
            $this->driverConnection->rollBack();

            // handle setup after transaction is no longer open
            if ($this->autoSetup && $e instanceof TableNotFoundException) {
                $this->setup();
                goto insert;
            }

            throw $e;
        }

        return $id;
    }

    protected function executeStatement(string $sql, array $parameters = [], array $types = []): int|string
    {
        try {
            return $this->driverConnection->executeStatement($sql, $parameters, $types);
        } catch (TableNotFoundException $e) {
            if (!$this->autoSetup || $this->driverConnection->isTransactionActive()) {
                throw $e;
            }
        }

        $this->setup();

        return $this->driverConnection->executeStatement($sql, $parameters, $types);
    }

    public function setup(): void
    {
        $configuration = $this->driverConnection->getConfiguration();
        $assetFilter = $configuration->getSchemaAssetsFilter();
        $configuration->setSchemaAssetsFilter(function ($tableName) {
            if ($tableName instanceof AbstractAsset) {
                $tableName = $tableName->getName();
            }

            if (!\is_string($tableName)) {
                throw new \TypeError(\sprintf('The table name must be an instance of "%s" or a string ("%s" given).', AbstractAsset::class, get_debug_type($tableName)));
            }

            return $tableName === $this->configuration['table_name'];
        });
        $this->updateSchema();
        $configuration->setSchemaAssetsFilter($assetFilter);
        $this->autoSetup = false;
    }

    public function getConfiguration(): array
    {
        return $this->configuration;
    }

    private function updateSchema(): void
    {
        $schemaManager = $this->driverConnection->createSchemaManager();
        $schemaDiff = $schemaManager->createComparator()
            ->compareSchemas($schemaManager->introspectSchema(), $this->getSchema());
        $platform = $this->driverConnection->getDatabasePlatform();

        if ($platform->supportsSchemas()) {
            foreach ($schemaDiff->getCreatedSchemas() as $schema) {
                $this->driverConnection->executeStatement($platform->getCreateSchemaSQL($schema));
            }
        }

        if ($platform->supportsSequences()) {
            foreach ($schemaDiff->getAlteredSequences() as $sequence) {
                $this->driverConnection->executeStatement($platform->getAlterSequenceSQL($sequence));
            }

            foreach ($schemaDiff->getCreatedSequences() as $sequence) {
                $this->driverConnection->executeStatement($platform->getCreateSequenceSQL($sequence));
            }
        }

        foreach ($platform->getCreateTablesSQL($schemaDiff->getCreatedTables()) as $sql) {
            $this->driverConnection->executeStatement($sql);
        }

        foreach ($schemaDiff->getAlteredTables() as $tableDiff) {
            foreach ($platform->getAlterTableSQL($tableDiff) as $sql) {
                $this->driverConnection->executeStatement($sql);
            }
        }
    }

    private function getSchema(): Schema
    {
        $schema = new Schema([], [], $this->driverConnection->createSchemaManager()->createSchemaConfig());
        $this->addTableToSchema($schema);

        return $schema;
    }

    private function addTableToSchema(Schema $schema): void
    {
        $table = $schema->createTable($this->configuration['table_name']);
        // add an internal option to mark that we created this & the non-namespaced table name
        $table->addOption(self::TABLE_OPTION_NAME, $this->configuration['table_name']);
        $idColumn = $table->addColumn('id', Types::BIGINT)
            ->setAutoincrement(true)
            ->setNotnull(true);
        $table->addColumn('body', Types::TEXT)
            ->setNotnull(true);
        $table->addColumn('headers', Types::TEXT)
            ->setNotnull(true);
        $table->addColumn('queue_name', Types::STRING)
            ->setLength(190) // MySQL 5.6 only supports 191 characters on an indexed column in utf8mb4 mode
            ->setNotnull(true);
        $table->addColumn('created_at', Types::DATETIME_IMMUTABLE)
            ->setNotnull(true);
        $table->addColumn('available_at', Types::DATETIME_IMMUTABLE)
            ->setNotnull(true);
        $table->addColumn('delivered_at', Types::DATETIME_IMMUTABLE)
            ->setNotnull(false);
        $table->setPrimaryKey(['id']);
        $table->addIndex(['queue_name']);
        $table->addIndex(['available_at']);
        $table->addIndex(['delivered_at']);

        // We need to create a sequence for Oracle and set the id column to get the correct nextval
        if ($this->driverConnection->getDatabasePlatform() instanceof OraclePlatform) {
            $idColumn->setDefault($this->configuration['table_name'].self::ORACLE_SEQUENCES_SUFFIX.'.nextval');

            $schema->createSequence($this->configuration['table_name'].self::ORACLE_SEQUENCES_SUFFIX);
        }
    }

    public function get(): ?array
    {
        if ($this->doMysqlCleanup && $this->driverConnection->getDatabasePlatform() instanceof AbstractMySQLPlatform) {
            try {
                $this->driverConnection->delete($this->configuration['table_name'], ['delivered_at' => '9999-12-31 23:59:59']);
                $this->doMysqlCleanup = false;
            } catch (DriverException $e) {
                // Ignore the exception
            }
        }

        get:
        $this->driverConnection->beginTransaction();
        try {
            $query = $this->createAvailableMessagesQueryBuilder()
                ->orderBy('available_at', 'ASC')
                ->setMaxResults(1);

            if ($this->driverConnection->getDatabasePlatform() instanceof OraclePlatform) {
                $query->select('m.id');
            }

            // Append pessimistic write lock to FROM clause if db platform supports it
            $sql = $query->getSQL();

            // Wrap the rownum query in a sub-query to allow writelocks without ORA-02014 error
            if ($this->driverConnection->getDatabasePlatform() instanceof OraclePlatform) {
                $query = $this->createQueryBuilder('w')
                    ->where('w.id IN ('.str_replace('SELECT a.* FROM', 'SELECT a.id FROM', $sql).')')
                    ->setParameters($query->getParameters(), $query->getParameterTypes());

                $sql = $query->getSQL();
            }

            $sql = $this->addLockMode($query, $sql);

            $doctrineEnvelope = $this->executeQuery(
                $sql,
                $query->getParameters(),
                $query->getParameterTypes()
            )->fetchAssociative();

            if (false === $doctrineEnvelope) {
                $this->driverConnection->commit();
                $this->queueEmptiedAt = microtime(true) * 1000;

                return null;
            }
            // Postgres can "group" notifications having the same channel and payload
            // We need to be sure to empty the queue before blocking again
            $this->queueEmptiedAt = null;

            $doctrineEnvelope = $this->decodeEnvelopeHeaders($doctrineEnvelope);

            $queryBuilder = $this->driverConnection->createQueryBuilder()
                ->update($this->configuration['table_name'])
                ->set('delivered_at', '?')
                ->where('id = ?');
            $now = new \DateTimeImmutable();
            $this->executeStatement($queryBuilder->getSQL(), [
                $now,
                $doctrineEnvelope['id'],
            ], [
                Types::DATETIME_IMMUTABLE,
            ]);

            $this->driverConnection->commit();

            return $doctrineEnvelope;
        } catch (\Throwable $e) {
            $this->driverConnection->rollBack();

            if ($this->autoSetup && $e instanceof TableNotFoundException) {
                $this->setup();
                goto get;
            }

            throw $e;
        }
    }

    private function createAvailableMessagesQueryBuilder(): QueryBuilder
    {
        $now = new \DateTimeImmutable();
        $redeliverLimit = $now->modify(\sprintf('-%d seconds', $this->configuration['redeliver_timeout']));

        return $this->createQueryBuilder()
            ->where('m.queue_name = ?')
            ->andWhere('m.delivered_at is null OR m.delivered_at < ?')
            ->andWhere('m.available_at <= ?')
            ->setParameters([
                $this->configuration['queue_name'],
                $redeliverLimit,
                $now,
            ], [
                Types::STRING,
                Types::DATETIME_IMMUTABLE,
                Types::DATETIME_IMMUTABLE,
            ]);
    }

    private function addLockMode(QueryBuilder $query, string $sql): string
    {
        $query->forUpdate(ConflictResolutionMode::SKIP_LOCKED);
        try {
            return $query->getSQL();
        } catch (DBALException) {
            return $this->fallBackToForUpdate($query, $sql);
        }
    }

    private function fallBackToForUpdate(QueryBuilder $query, string $sql): string
    {
        $query->forUpdate();
        try {
            return $query->getSQL();
        } catch (DBALException) {
            return $sql;
        }
    }

    private function executeQuery(string $sql, array $parameters = [], array $types = []): Result
    {
        try {
            return $this->driverConnection->executeQuery($sql, $parameters, $types);
        } catch (TableNotFoundException $e) {
            if (!$this->autoSetup || $this->driverConnection->isTransactionActive()) {
                throw $e;
            }
        }

        $this->setup();

        return $this->driverConnection->executeQuery($sql, $parameters, $types);
    }

    private function decodeEnvelopeHeaders(array $doctrineEnvelope): array
    {
        $doctrineEnvelope['headers'] = json_decode($doctrineEnvelope['headers'], true);

        return $doctrineEnvelope;
    }

    public function ack(string $id): bool
    {
        try {
            if ($this->driverConnection->getDatabasePlatform() instanceof AbstractMySQLPlatform) {
                if ($updated = $this->driverConnection->update($this->configuration['table_name'], ['delivered_at' => '9999-12-31 23:59:59'], ['id' => $id]) > 0) {
                    $this->doMysqlCleanup = true;
                }

                return $updated;
            }

            return $this->driverConnection->delete($this->configuration['table_name'], ['id' => $id]) > 0;
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function reject(string $id): bool
    {
        try {
            if ($this->driverConnection->getDatabasePlatform() instanceof AbstractMySQLPlatform) {
                if ($updated = $this->driverConnection->update($this->configuration['table_name'], ['delivered_at' => '9999-12-31 23:59:59'], ['id' => $id]) > 0) {
                    $this->doMysqlCleanup = true;
                }

                return $updated;
            }

            return $this->driverConnection->delete($this->configuration['table_name'], ['id' => $id]) > 0;
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function keepalive(string $id, ?int $seconds = null): void
    {
        // Check if the redeliver timeout is smaller than the keepalive interval
        if (null !== $seconds && $this->configuration['redeliver_timeout'] < $seconds) {
            throw new TransportException(\sprintf('Doctrine redeliver_timeout (%ds) cannot be smaller than the keepalive interval (%ds).', $this->configuration['redeliver_timeout'], $seconds));
        }

        $this->driverConnection->beginTransaction();
        try {
            $queryBuilder = $this->driverConnection->createQueryBuilder()
                ->update($this->configuration['table_name'])
                ->set('delivered_at', '?')
                ->where('id = ?');
            $now = new \DateTimeImmutable();
            $this->executeStatement($queryBuilder->getSQL(), [
                $now,
                $id,
            ], [
                Types::DATETIME_IMMUTABLE,
            ]);

            $this->driverConnection->commit();
        } catch (\Throwable $e) {
            $this->driverConnection->rollBack();
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    public function getMessageCount(): int
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder()
            ->select('COUNT(m.id) AS message_count')
            ->setMaxResults(1);

        return $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes())->fetchOne();
    }

    public function findAll(?int $limit = null): array
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder();

        if (null !== $limit) {
            $queryBuilder->setMaxResults($limit);
        }

        return array_map(
            $this->decodeEnvelopeHeaders(...),
            $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes())->fetchAllAssociative()
        );
    }

    public function find(mixed $id): ?array
    {
        $queryBuilder = $this->createQueryBuilder()
            ->where('m.id = ? and m.queue_name = ?');

        $data = $this->executeQuery($queryBuilder->getSQL(), [$id, $this->configuration['queue_name']])->fetchAssociative();

        return false === $data ? null : $this->decodeEnvelopeHeaders($data);
    }

    /**
     * @internal
     */
    public function configureSchema(Schema $schema, DBALConnection $forConnection, \Closure $isSameDatabase): void
    {
        if ($schema->hasTable($this->configuration['table_name'])) {
            return;
        }

        if ($forConnection !== $this->driverConnection && !$isSameDatabase($this->executeStatement(...))) {
            return;
        }

        $this->addTableToSchema($schema);
    }

    /**
     * @internal
     */
    public function getExtraSetupSqlForTable(Table $createdTable): array
    {
        return [];
    }
}
