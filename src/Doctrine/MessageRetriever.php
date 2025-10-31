<?php

namespace Tourze\AsyncMessengerBundle\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Query\ForUpdate\ConflictResolutionMode;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;

/**
 * 负责消息检索操作的类
 */
class MessageRetriever
{
    /**
     * @param array<string, mixed> $configuration
     */
    public function __construct(
        private readonly DBALConnection $connection,
        private readonly array $configuration,
        private readonly SchemaManager $schemaManager,
        private readonly bool $autoSetup = true,
    ) {
    }

    private function getQueueName(): string
    {
        $queueName = $this->configuration['queue_name'] ?? 'default';

        return is_string($queueName) ? $queueName : 'default';
    }

    /**
     * @return array<string, mixed>|false
     */
    public function fetchMessage(): array|false
    {
        $query = $this->createAvailableMessagesQueryBuilder()
            ->orderBy('available_at', 'ASC')
            ->setMaxResults(1)
        ;

        if ($this->connection->getDatabasePlatform() instanceof OraclePlatform) {
            $query->select('m.id');
        }

        $sql = $this->prepareQueryForPlatform($query);
        $sql = $this->addLockMode($query, $sql);

        return $this->executeQuery(
            $sql,
            $query->getParameters(),
            $query->getParameterTypes()
        )->fetchAssociative();
    }

    public function createQueryBuilder(string $alias = 'm'): QueryBuilder
    {
        return $this->buildQueryForMessagesTable($alias);
    }

    /**
     * 构建消息表的查询构建器
     * 此方法避免与Doctrine Repository的createQueryBuilder方法混淆
     */
    public function buildQueryForMessagesTable(string $alias = 'm'): QueryBuilder
    {
        $tableName = $this->configuration['table_name'] ?? 'messenger_messages';
        $tableName = is_string($tableName) ? $tableName : 'messenger_messages';
        $queryBuilder = $this->connection->createQueryBuilder()
            ->from($tableName, $alias)
        ;

        $alias .= '.';

        if (!$this->connection->getDatabasePlatform() instanceof OraclePlatform) {
            return $queryBuilder->select($alias . '*');
        }

        // Oracle databases use UPPER CASE on tables and column identifiers.
        // Column alias is added to force the result to be lowercase even when the actual field is all caps.

        return $queryBuilder->select(str_replace(', ', ', ' . $alias,
            $alias . 'id AS "id", body AS "body", headers AS "headers", queue_name AS "queue_name", ' .
            'created_at AS "created_at", available_at AS "available_at", ' .
            'delivered_at AS "delivered_at"'
        ));
    }

    public function createAvailableMessagesQueryBuilder(): QueryBuilder
    {
        $now = new \DateTimeImmutable();
        $redeliverTimeoutValue = $this->configuration['redeliver_timeout'] ?? 3600;
        $redeliverTimeout = is_numeric($redeliverTimeoutValue) ? (int) $redeliverTimeoutValue : 3600;
        $redeliverLimit = $now->modify(\sprintf('-%d seconds', $redeliverTimeout));

        return $this->buildQueryForMessagesTable()
            ->where('m.queue_name = ?')
            ->andWhere('m.delivered_at is null OR m.delivered_at < ?')
            ->andWhere('m.available_at <= ?')
            ->setParameters([
                $this->getQueueName(),
                $redeliverLimit,
                $now,
            ], [
                Types::STRING,
                Types::DATETIME_IMMUTABLE,
                Types::DATETIME_IMMUTABLE,
            ])
        ;
    }

    /**
     * @param array<int<0, max>|string, mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $types
     */
    public function executeQuery(string $sql, array $parameters = [], array $types = []): Result
    {
        try {
            return $this->connection->executeQuery($sql, $parameters, $types);
        } catch (TableNotFoundException $e) {
            if (!$this->autoSetup || $this->connection->isTransactionActive()) {
                throw $e;
            }
        }

        $this->schemaManager->setup();

        return $this->connection->executeQuery($sql, $parameters, $types);
    }

    /**
     * @return \Generator<array<string, mixed>>
     */
    public function findAll(?int $limit = null): \Generator
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder();

        if (null !== $limit) {
            $queryBuilder->setMaxResults($limit);
        }

        $result = $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes());

        while (($row = $result->fetchAssociative()) !== false) {
            yield $this->decodeEnvelopeHeaders($row);
        }
    }

    /**
     * @return array<string, mixed>|null
     */
    public function find(mixed $id): ?array
    {
        $queryBuilder = $this->buildQueryForMessagesTable()
            ->where('m.id = ? and m.queue_name = ?')
        ;

        $data = $this->executeQuery($queryBuilder->getSQL(), [$id, $this->getQueueName()])->fetchAssociative();

        return false === $data ? null : $this->decodeEnvelopeHeaders($data);
    }

    public function getMessageCount(): int
    {
        $queryBuilder = $this->createAvailableMessagesQueryBuilder()
            ->select('COUNT(m.id) AS message_count')
            ->setMaxResults(1)
        ;

        $result = $this->executeQuery($queryBuilder->getSQL(), $queryBuilder->getParameters(), $queryBuilder->getParameterTypes())->fetchOne();

        return is_numeric($result) ? (int) $result : 0;
    }

    /**
     * @param array<string, mixed> $doctrineEnvelope
     * @return array<string, mixed>
     */
    public function decodeEnvelopeHeaders(array $doctrineEnvelope): array
    {
        $headersValue = $doctrineEnvelope['headers'] ?? '{}';
        $headers = is_string($headersValue) ? $headersValue : '{}';
        $doctrineEnvelope['headers'] = json_decode($headers, true) ?? [];

        return $doctrineEnvelope;
    }

    private function prepareQueryForPlatform(QueryBuilder $query): string
    {
        $sql = $query->getSQL();

        if ($this->connection->getDatabasePlatform() instanceof OraclePlatform) {
            $query = $this->buildQueryForMessagesTable('w')
                ->where('w.id IN (' . str_replace('SELECT a.* FROM', 'SELECT a.id FROM', $sql) . ')')
                ->setParameters($query->getParameters(), $query->getParameterTypes())
            ;
            $sql = $query->getSQL();
        }

        return $sql;
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
}
