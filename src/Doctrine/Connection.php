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

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
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
    protected const DEFAULT_OPTIONS = [
        'table_name' => 'messenger_messages',
        'queue_name' => 'default',
        'redeliver_timeout' => 3600,
        'auto_setup' => true,
    ];

    /**
     * @return array<string, mixed>
     */
    private function getDefaultOptions(): array
    {
        return [
            'table_name' => 'messenger_messages',
            'queue_name' => 'default',
            'redeliver_timeout' => 3600,
            'auto_setup' => true,
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public static function getDefaultOptionsStatic(): array
    {
        return [
            'table_name' => 'messenger_messages',
            'queue_name' => 'default',
            'redeliver_timeout' => 3600,
            'auto_setup' => true,
        ];
    }

    protected ?float $queueEmptiedAt = null;

    private bool $autoSetup;

    private bool $doMysqlCleanup = false;

    private readonly SchemaManager $schemaManager;

    private readonly MessageInserter $messageInserter;

    private readonly MessageRetriever $messageRetriever;

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
     *
     * @param array<string, mixed> $configuration Configuration options
     */
    public function __construct(
        protected array $configuration,
        protected DBALConnection $driverConnection,
    ) {
        /** @var array<string, mixed> $mergedConfiguration */
        $mergedConfiguration = array_replace_recursive($this->getDefaultOptions(), $configuration);
        $this->configuration = $mergedConfiguration;
        $this->autoSetup = (bool) $this->configuration['auto_setup'];

        $this->schemaManager = new SchemaManager($this->driverConnection, $this->configuration);
        $this->messageInserter = new MessageInserter($this->driverConnection, $this->configuration, $this->schemaManager, $this->autoSetup);
        $this->messageRetriever = new MessageRetriever($this->driverConnection, $this->configuration, $this->schemaManager, $this->autoSetup);
    }

    /**
     * @param array<string, mixed> $options
     * @return array<string, mixed>
     */
    public static function buildConfiguration(#[\SensitiveParameter] string $dsn, array $options = []): array
    {
        if (false === $params = parse_url($dsn)) {
            return [];
        }

        $query = [];
        if (isset($params['query'])) {
            parse_str($params['query'], $query);
        }

        /** @var array<string, mixed> $configuration */
        $configuration = $query + $options + self::getDefaultOptionsStatic();

        $configuration['auto_setup'] = filter_var($configuration['auto_setup'], \FILTER_VALIDATE_BOOL);

        // check for extra keys in options
        $optionsExtraKeys = array_diff(array_keys($options), array_keys(self::getDefaultOptionsStatic()));
        if (0 < \count($optionsExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found: [%s]. Allowed options are [%s].', implode(', ', $optionsExtraKeys), implode(', ', array_keys(self::getDefaultOptionsStatic()))));
        }

        // check for extra keys in options
        $queryExtraKeys = array_diff(array_keys($query), array_keys(self::getDefaultOptionsStatic()));
        if (0 < \count($queryExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found in DSN: [%s]. Allowed options are [%s].', implode(', ', $queryExtraKeys), implode(', ', array_keys(self::getDefaultOptionsStatic()))));
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
    /**
     * @param array<string, mixed> $headers
     */
    public function send(string $body, array $headers, int $delay = 0): string
    {
        return $this->messageInserter->insertMessage($body, $headers, $delay);
    }

    public function setup(): void
    {
        $this->schemaManager->setup();
        $this->autoSetup = false;
    }

    /**
     * @return array<string, mixed>
     */
    public function getConfiguration(): array
    {
        return $this->configuration;
    }

    /**
     * @return array<string, mixed>|null
     */
    public function get(): ?array
    {
        $this->performMysqlCleanupIfNeeded();

        return $this->getMessageWithRetry();
    }

    /**
     * @return array<string, mixed>|null
     */
    private function getMessageWithRetry(): ?array
    {
        $retryOnTableNotFound = true;

        while ($retryOnTableNotFound) {
            $this->driverConnection->beginTransaction();

            try {
                $doctrineEnvelope = $this->messageRetriever->fetchMessage();

                if (false === $doctrineEnvelope) {
                    $this->driverConnection->commit();
                    $this->queueEmptiedAt = microtime(true) * 1000;

                    return null;
                }

                return $this->processFoundMessage($doctrineEnvelope);
            } catch (\Throwable $e) {
                $this->driverConnection->rollBack();

                if ($this->shouldRetryOnTableNotFound($e, $retryOnTableNotFound)) {
                    $this->setup();
                    $retryOnTableNotFound = false;
                    continue;
                }

                throw $e;
            }
        }

        return null;
    }

    /**
     * @param array<string, mixed> $doctrineEnvelope
     * @return array<string, mixed>
     */
    private function processFoundMessage(array $doctrineEnvelope): array
    {
        $this->queueEmptiedAt = null;
        $doctrineEnvelope = $this->messageRetriever->decodeEnvelopeHeaders($doctrineEnvelope);
        $messageIdValue = $doctrineEnvelope['id'] ?? '';
        $messageId = is_scalar($messageIdValue) ? (string) $messageIdValue : '';
        $this->markMessageAsDelivered($messageId);
        $this->driverConnection->commit();

        return $doctrineEnvelope;
    }

    private function shouldRetryOnTableNotFound(\Throwable $e, bool $retryOnTableNotFound): bool
    {
        return $this->autoSetup && $e instanceof TableNotFoundException && $retryOnTableNotFound;
    }

    private function performMysqlCleanupIfNeeded(): void
    {
        if (!$this->doMysqlCleanup || !$this->driverConnection->getDatabasePlatform() instanceof AbstractMySQLPlatform) {
            return;
        }

        try {
            $tableNameValue = $this->configuration['table_name'] ?? 'messenger_messages';
            $tableName = is_scalar($tableNameValue) ? (string) $tableNameValue : 'messenger_messages';
            $this->driverConnection->delete($tableName, ['delivered_at' => '9999-12-31 23:59:59']);
            $this->doMysqlCleanup = false;
        } catch (DriverException $e) {
            // Ignore the exception
        }
    }

    private function markMessageAsDelivered(string $id): void
    {
        $tableNameValue = $this->configuration['table_name'] ?? 'messenger_messages';
        $tableName = is_scalar($tableNameValue) ? (string) $tableNameValue : 'messenger_messages';
        $queryBuilder = $this->driverConnection->createQueryBuilder()
            ->update($tableName)
            ->set('delivered_at', '?')
            ->where('id = ?')
        ;

        $now = new \DateTimeImmutable();
        $this->executeStatement($queryBuilder->getSQL(), [
            0 => $now,
            1 => $id,
        ], [
            0 => Types::DATETIME_IMMUTABLE,
        ]);
    }

    /**
     * @param array<int|string, mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $types
     */
    protected function executeStatement(string $sql, array $parameters = [], array $types = []): int|string
    {
        try {
            /** @var array<int<0, max>|string, mixed> $params */
            $params = $parameters;
            /** @var array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $paramTypes */
            $paramTypes = $types;

            return $this->driverConnection->executeStatement($sql, $params, $paramTypes);
        } catch (TableNotFoundException $e) {
            if (!$this->autoSetup || $this->driverConnection->isTransactionActive()) {
                throw $e;
            }
        }

        $this->setup();

        /** @var array<int<0, max>|string, mixed> $params */
        $params = $parameters;
        /** @var array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $paramTypes */
        $paramTypes = $types;

        return $this->driverConnection->executeStatement($sql, $params, $paramTypes);
    }

    public function ack(string $id): bool
    {
        try {
            $tableNameValue = $this->configuration['table_name'] ?? 'messenger_messages';
            $tableName = is_scalar($tableNameValue) ? (string) $tableNameValue : 'messenger_messages';
            if ($this->driverConnection->getDatabasePlatform() instanceof AbstractMySQLPlatform) {
                if ($updated = $this->driverConnection->update($tableName, ['delivered_at' => '9999-12-31 23:59:59'], ['id' => $id]) > 0) {
                    $this->doMysqlCleanup = true;
                }

                return $updated;
            }

            return $this->driverConnection->delete($tableName, ['id' => $id]) > 0;
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function reject(string $id): bool
    {
        try {
            $tableNameValue = $this->configuration['table_name'] ?? 'messenger_messages';
            $tableName = is_scalar($tableNameValue) ? (string) $tableNameValue : 'messenger_messages';
            if ($this->driverConnection->getDatabasePlatform() instanceof AbstractMySQLPlatform) {
                if ($updated = $this->driverConnection->update($tableName, ['delivered_at' => '9999-12-31 23:59:59'], ['id' => $id]) > 0) {
                    $this->doMysqlCleanup = true;
                }

                return $updated;
            }

            return $this->driverConnection->delete($tableName, ['id' => $id]) > 0;
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function keepalive(string $id, ?int $seconds = null): void
    {
        // Check if the redeliver timeout is smaller than the keepalive interval
        $redeliverTimeoutValue = $this->configuration['redeliver_timeout'] ?? 3600;
        $redeliverTimeout = is_numeric($redeliverTimeoutValue) ? (int) $redeliverTimeoutValue : 3600;
        if (null !== $seconds && $redeliverTimeout < $seconds) {
            throw new TransportException(\sprintf('Doctrine redeliver_timeout (%ds) cannot be smaller than the keepalive interval (%ds).', $redeliverTimeout, $seconds));
        }

        $this->driverConnection->beginTransaction();
        try {
            $tableNameValue = $this->configuration['table_name'] ?? 'messenger_messages';
            $tableName = is_scalar($tableNameValue) ? (string) $tableNameValue : 'messenger_messages';
            $queryBuilder = $this->driverConnection->createQueryBuilder()
                ->update($tableName)
                ->set('delivered_at', '?')
                ->where('id = ?')
            ;
            $now = new \DateTimeImmutable();
            $this->executeStatement($queryBuilder->getSQL(), [
                0 => $now,
                1 => $id,
            ], [
                0 => Types::DATETIME_IMMUTABLE,
            ]);

            $this->driverConnection->commit();
        } catch (\Throwable $e) {
            $this->driverConnection->rollBack();
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    public function getMessageCount(): int
    {
        return $this->messageRetriever->getMessageCount();
    }

    /**
     * @return \Generator<array<string, mixed>>
     */
    public function findAll(?int $limit = null): \Generator
    {
        return $this->messageRetriever->findAll($limit);
    }

    /**
     * @return array<string, mixed>|null
     */
    public function find(mixed $id): ?array
    {
        return $this->messageRetriever->find($id);
    }

    /**
     * @internal
     */
    public function configureSchema(Schema $schema, DBALConnection $forConnection, \Closure $isSameDatabase): void
    {
        $tableNameValue = $this->configuration['table_name'] ?? 'messenger_messages';
        $tableName = is_scalar($tableNameValue) ? (string) $tableNameValue : 'messenger_messages';
        if ($schema->hasTable($tableName)) {
            return;
        }

        if ($forConnection !== $this->driverConnection && !$isSameDatabase($this->executeStatement(...))) {
            return;
        }

        $this->schemaManager->addTableToSchema($schema);
    }

    /**
     * @internal
     */
    /**
     * @return array<string>
     */
    public function getExtraSetupSqlForTable(Table $createdTable): array
    {
        return [];
    }
}
