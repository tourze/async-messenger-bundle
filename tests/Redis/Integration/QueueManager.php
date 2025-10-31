<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\StampInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;

/**
 * 测试队列管理器 - Linus风格的数据结构优先设计
 *
 * 职责：
 * - 管理Redis传输实例的生命周期
 * - 提供队列配置的标准化接口
 * - 批量操作的简化封装
 */
final class QueueManager
{
    private \Redis $redis;

    private PhpSerializer $serializer;

    /** @var array<string, mixed> */
    private array $connectionOptions;

    /** @var array<string, RedisTransport> */
    private array $transports = [];

    /** @param array<string, mixed> $connectionOptions */
    public function __construct(\Redis $redis, PhpSerializer $serializer, array $connectionOptions)
    {
        $this->redis = $redis;
        $this->serializer = $serializer;
        $this->connectionOptions = $connectionOptions;
    }

    /** @param array<string, mixed> $extraOptions */
    public function createTransport(string $queueName, array $extraOptions = []): RedisTransport
    {
        if (isset($this->transports[$queueName])) {
            return $this->transports[$queueName];
        }

        $options = array_merge($this->connectionOptions, [
            'queue' => $queueName,
            'delayed_queue' => "{$queueName}_delayed",
        ], $extraOptions);

        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $this->transports[$queueName] = $transport;

        return $transport;
    }

    /**
     * @param array<string> $queueNames
     * @param array<string, mixed> $extraOptions
     * @return array<string, RedisTransport>
     */
    public function createMultipleTransports(array $queueNames, array $extraOptions = []): array
    {
        $transports = [];
        foreach ($queueNames as $queueName) {
            $transports[$queueName] = $this->createTransport($queueName, $extraOptions);
        }

        return $transports;
    }

    /** @param array<StampInterface> $stamps */
    public function sendMessage(string $queueName, \stdClass $message, array $stamps = []): void
    {
        $transport = $this->createTransport($queueName);
        $transport->send(new Envelope($message, $stamps));
    }

    /**
     * @param array<\stdClass> $messages
     * @param array<StampInterface> $stamps
     */
    public function sendMessages(string $queueName, array $messages, array $stamps = []): void
    {
        foreach ($messages as $message) {
            $this->sendMessage($queueName, $message, $stamps);
        }
    }
}
