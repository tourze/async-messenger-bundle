<?php

namespace Tourze\AsyncMessengerBundle\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * 负责消息插入操作的类
 */
class MessageInserter
{
    private const ORACLE_SEQUENCES_SUFFIX = '_seq';

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

    /**
     * @param array<string, mixed> $headers
     */
    public function insertMessage(string $body, array $headers, int $delay = 0): string
    {
        $now = new \DateTimeImmutable();
        $availableAt = $now->modify(\sprintf('%+d seconds', $delay / 1000));

        $tableNameValue = $this->configuration['table_name'] ?? 'messenger_messages';
        $tableName = is_scalar($tableNameValue) ? (string) $tableNameValue : 'messenger_messages';
        $queryBuilder = $this->connection->createQueryBuilder()
            ->insert($tableName)
            ->values([
                'body' => '?',
                'headers' => '?',
                'queue_name' => '?',
                'created_at' => '?',
                'available_at' => '?',
            ])
        ;

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

    /**
     * @param array<int|string, mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $types
     */
    private function executeInsert(string $sql, array $parameters = [], array $types = []): string
    {
        $sql = $this->prepareInsertSql($sql);

        $retryOnTableNotFound = true;

        while ($retryOnTableNotFound) {
            $this->connection->beginTransaction();

            try {
                $id = $this->executeInsertForPlatform($sql, $parameters, $types);
                $this->connection->commit();

                return $id;
            } catch (\Throwable $e) {
                $this->connection->rollBack();

                if ($this->autoSetup && $e instanceof TableNotFoundException && $retryOnTableNotFound) {
                    $this->schemaManager->setup();
                    $retryOnTableNotFound = false;
                    continue;
                }

                throw $e;
            }
        }

        throw new TransportException('Failed to insert message');
    }

    private function prepareInsertSql(string $sql): string
    {
        if ($this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            $sql .= ' RETURNING id';
        }

        return $sql;
    }

    /**
     * @param array<int|string, mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $types
     */
    private function executeInsertForPlatform(string $sql, array $parameters, array $types): string
    {
        if ($this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            return $this->executePostgreSQLInsert($sql, $parameters, $types);
        }

        if ($this->connection->getDatabasePlatform() instanceof OraclePlatform) {
            return $this->executeOracleInsert($sql, $parameters, $types);
        }

        return $this->executeGenericInsert($sql, $parameters, $types);
    }

    /**
     * @param array<int|string, mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $types
     */
    private function executePostgreSQLInsert(string $sql, array $parameters, array $types): string
    {
        /** @var array<int<0, max>|string, mixed> $params */
        $params = $parameters;
        /** @var array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $paramTypes */
        $paramTypes = $types;
        $first = $this->connection->fetchFirstColumn($sql, $params, $paramTypes);
        $id = $first[0] ?? null;

        if (null === $id) {
            throw new TransportException('no id was returned by PostgreSQL from RETURNING clause.');
        }

        return is_scalar($id) ? (string) $id : '';
    }

    /**
     * @param array<int|string, mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $types
     */
    private function executeOracleInsert(string $sql, array $parameters, array $types): string
    {
        $tableNameValue = $this->configuration['table_name'] ?? 'messenger_messages';
        $tableName = is_scalar($tableNameValue) ? (string) $tableNameValue : 'messenger_messages';
        $sequenceName = $tableName . self::ORACLE_SEQUENCES_SUFFIX;
        /** @var array<int<0, max>|string, mixed> $params */
        $params = $parameters;
        /** @var array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $paramTypes */
        $paramTypes = $types;
        $this->connection->executeStatement($sql, $params, $paramTypes);
        $result = $this->connection->fetchOne('SELECT ' . $sequenceName . '.CURRVAL FROM DUAL');
        $id = is_numeric($result) ? (int) $result : 0;

        if (0 === $id) {
            throw new TransportException('no id was returned by Oracle from sequence: ' . $sequenceName);
        }

        return (string) $id;
    }

    /**
     * @param array<int|string, mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $types
     */
    private function executeGenericInsert(string $sql, array $parameters, array $types): string
    {
        /** @var array<int<0, max>|string, mixed> $params */
        $params = $parameters;
        /** @var array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $paramTypes */
        $paramTypes = $types;
        $this->connection->executeStatement($sql, $params, $paramTypes);
        $id = $this->connection->lastInsertId();

        if ('' === $id || '0' === $id || 0 === $id) {
            throw new TransportException('lastInsertId() returned an invalid value, no id was returned.');
        }

        return (string) $id;
    }
}
