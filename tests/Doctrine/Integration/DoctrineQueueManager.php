<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Doctrine\DBAL\Connection as DBALConnection;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;

/**
 * 队列管理器 - Linus风格的数据结构优先设计
 */
final class DoctrineQueueManager
{
    private DBALConnection $dbalConnection;

    private PhpSerializer $serializer;

    private string $tableName;

    /** @var array<string, DoctrineTransport> */
    private array $transports = [];

    public function __construct(DBALConnection $dbalConnection, PhpSerializer $serializer, string $tableName)
    {
        $this->dbalConnection = $dbalConnection;
        $this->serializer = $serializer;
        $this->tableName = $tableName;
    }

    public function createTransport(string $queueName): DoctrineTransport
    {
        if (isset($this->transports[$queueName])) {
            return $this->transports[$queueName];
        }

        $options = [
            'table_name' => $this->tableName,
            'queue_name' => $queueName,
            'redeliver_timeout' => 3600,
            'auto_setup' => false,
        ];

        $connection = new Connection($options, $this->dbalConnection);
        $transport = new DoctrineTransport($connection, $this->serializer);

        $this->transports[$queueName] = $transport;

        return $transport;
    }

    /**
     * @param array<string> $queueNames
     * @return array<string, DoctrineTransport>
     */
    public function createMultipleTransports(array $queueNames): array
    {
        $transports = [];
        foreach ($queueNames as $queueName) {
            $transports[$queueName] = $this->createTransport($queueName);
        }

        return $transports;
    }

    public function sendMessage(string $queueName, \stdClass $message): void
    {
        $transport = $this->createTransport($queueName);
        $transport->send(new Envelope($message, []));
    }

    /**
     * @param array<\stdClass> $messages
     */
    public function sendMessages(string $queueName, array $messages): void
    {
        foreach ($messages as $message) {
            $this->sendMessage($queueName, $message);
        }
    }
}
