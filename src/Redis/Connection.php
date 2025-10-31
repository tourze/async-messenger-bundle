<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Tourze\AsyncMessengerBundle\Redis;

use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * Redis 连接。
 *
 * @author Alexander Schranz <alexander@sulu.io>
 * @author Antoine Bluchet <soyuka@gmail.com>
 * @author Robin Chalas <robin.chalas@gmail.com>
 *
 * @internal
 *
 * @final 不可继承
 */
class Connection
{
    private const DEFAULT_OPTIONS = [
        'queue' => 'async_messages',
        'delayed_queue' => 'async_messages_delayed',
        'auto_setup' => true,
        'queue_max_entries' => 0, // any value higher than 0 defines an approximate maximum number of queue entries
        'redeliver_timeout' => 3600, // Timeout before redeliver messages still in pending state (seconds)
        'claim_interval' => 60000, // Interval by which pending/abandoned messages should be checked
    ];

    private string $queue;

    private string $delayedQueue;

    private bool $autoSetup;

    private int $maxEntries;

    private int $redeliverTimeout;

    private float $nextClaim = 0.0;

    private float $claimInterval;

    /**
     * @var array<string, array<string, mixed>>
     */
    private array $processingMessages = [];

    /**
     * @param array<string, mixed> $options
     */
    public function __construct(private readonly \Redis $redis, array $options = [])
    {
        $redisVersion = phpversion('redis');
        if (false === $redisVersion || version_compare($redisVersion, '4.3.0', '<')) {
            throw new LogicException('The redis transport requires php-redis 4.3.0 or higher.');
        }

        if (!$redis->isConnected()) {
            throw new InvalidArgumentException('Redis connection must be established before creating the transport.');
        }

        $options += self::DEFAULT_OPTIONS;

        if ('' === $options['queue']) {
            throw new InvalidArgumentException('"queue" should be configured, got an empty string.');
        }

        $queueValue = $options['queue'] ?? 'async_messages';
        $this->queue = is_string($queueValue) ? $queueValue : 'async_messages';

        $delayedQueueValue = $options['delayed_queue'] ?? 'async_messages_delayed';
        $this->delayedQueue = is_string($delayedQueueValue) ? $delayedQueueValue : 'async_messages_delayed';

        $autoSetupValue = $options['auto_setup'] ?? true;
        $this->autoSetup = is_bool($autoSetupValue) ? $autoSetupValue : true;

        $maxEntriesValue = $options['queue_max_entries'] ?? 0;
        $this->maxEntries = is_numeric($maxEntriesValue) ? (int) $maxEntriesValue : 0;

        $redeliverTimeoutValue = $options['redeliver_timeout'] ?? 3600;
        $redeliverTimeout = is_numeric($redeliverTimeoutValue) ? (int) $redeliverTimeoutValue : 3600;
        $this->redeliverTimeout = $redeliverTimeout * 1000;

        $claimIntervalValue = $options['claim_interval'] ?? 60000;
        $claimInterval = is_numeric($claimIntervalValue) ? (float) $claimIntervalValue : 60000;
        $this->claimInterval = $claimInterval / 1000;
    }

    private function getRedis(): \Redis
    {
        return $this->redis;
    }

    /**
     * @return array<string, mixed>|null
     */
    public function get(): ?array
    {
        if ($this->autoSetup) {
            $this->setup();
        }

        $redis = $this->getRedis();
        $now = microtime(true) * 1000;

        $this->processDelayedMessages($redis, $now);
        $this->claimAbandonedMessagesIfNeeded();

        return $this->getMessageFromQueue($redis, $now);
    }

    private function processDelayedMessages(\Redis $redis, float $now): void
    {
        try {
            $delayedMessages = $redis->zRangeByScore($this->delayedQueue, '0', (string) $now, ['limit' => [0, 1]]);
            if (0 === count($delayedMessages)) {
                return;
            }

            $message = $delayedMessages[0];
            if (is_string($message)) {
                $redis->zRem($this->delayedQueue, $message);

                $decodedMessage = json_decode($message, true);
                if (is_array($decodedMessage) && isset($decodedMessage['body'])) {
                    /** @var array<string, mixed> $decodedMessage */
                    $this->addDelayedMessageToQueue($redis, $decodedMessage, $now);
                }
            }
        } catch (\RedisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    /**
     * @param array<string, mixed> $decodedMessage
     */
    private function addDelayedMessageToQueue(\Redis $redis, array $decodedMessage, float $now): void
    {
        $id = $this->generateId();
        $redis->lPush($this->queue, json_encode([
            'id' => $id,
            'body' => $decodedMessage['body'],
            'headers' => $decodedMessage['headers'] ?? [],
            'timestamp' => $now,
        ]));
    }

    private function claimAbandonedMessagesIfNeeded(): void
    {
        if ($this->nextClaim <= microtime(true)) {
            $this->claimOldPendingMessages();
        }
    }

    /**
     * @return array<string, mixed>|null
     */
    private function getMessageFromQueue(\Redis $redis, float $now): ?array
    {
        try {
            $message = $redis->lPop($this->queue);
            if (!$message) {
                return null;
            }

            $decodedMessage = json_decode($message, true);
            if (!is_array($decodedMessage) || !isset($decodedMessage['id'])) {
                return null;
            }

            $messageIdValue = $decodedMessage['id'] ?? '';
            $messageId = is_scalar($messageIdValue) ? (string) $messageIdValue : '';
            $this->trackProcessingMessage($messageId, $message);

            return [
                'id' => $decodedMessage['id'],
                'data' => ['message' => json_encode([
                    'body' => $decodedMessage['body'],
                    'headers' => $decodedMessage['headers'] ?? [],
                ])],
            ];
        } catch (\RedisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    private function trackProcessingMessage(string $id, string $message): void
    {
        $this->processingMessages[$id] = [
            'message' => $message,
            'timestamp' => microtime(true),
        ];
    }

    public function setup(): void
    {
        // No setup needed for list-based implementation
        $this->autoSetup = false;
    }

    private function generateId(): string
    {
        return base64_encode(random_bytes(12));
    }

    /**
     * @param array<string, mixed> $headers
     */
    public function add(string $body, array $headers, int $delayInMs = 0): string
    {
        if ($this->autoSetup) {
            $this->setup();
        }

        $redis = $this->getRedis();
        $id = $this->generateId();

        try {
            $added = $this->addMessageToQueue($redis, $id, $body, $headers, $delayInMs);
        } catch (\RedisException $e) {
            $this->handleRedisException($redis, $e);
        }

        $this->validateMessageAdded($redis, $added);

        return $id;
    }

    /**
     * @param array<string, mixed> $headers
     */
    private function addMessageToQueue(\Redis $redis, string $id, string $body, array $headers, int $delayInMs): int
    {
        if ($delayInMs > 0) {
            return $this->addDelayedMessage($redis, $id, $body, $headers, $delayInMs);
        }

        return $this->addImmediateMessage($redis, $id, $body, $headers);
    }

    /**
     * @param array<string, mixed> $headers
     */
    private function addDelayedMessage(\Redis $redis, string $id, string $body, array $headers, int $delayInMs): int
    {
        $score = (microtime(true) * 1000) + $delayInMs;
        $message = json_encode([
            'id' => $id,
            'body' => $body,
            'headers' => $headers,
            'uniqid' => uniqid('', true),
        ]);

        if (false === $message) {
            throw new TransportException(json_last_error_msg());
        }

        return $redis->zAdd($this->delayedQueue, $score, $message);
    }

    /**
     * @param array<string, mixed> $headers
     */
    private function addImmediateMessage(\Redis $redis, string $id, string $body, array $headers): int
    {
        $message = json_encode([
            'id' => $id,
            'body' => $body,
            'headers' => $headers,
            'timestamp' => microtime(true) * 1000,
        ]);

        if (false === $message) {
            throw new TransportException(json_last_error_msg());
        }

        $added = $redis->rPush($this->queue, $message);
        $this->trimQueueIfNeeded($redis);

        return $added;
    }

    private function trimQueueIfNeeded(\Redis $redis): void
    {
        if ($this->maxEntries > 0) {
            $redis->ltrim($this->queue, -$this->maxEntries, -1);
        }
    }

    private function handleRedisException(\Redis $redis, \RedisException $e): never
    {
        $error = $redis->getLastError();
        if (null !== $error) {
            $redis->clearLastError();
        }
        throw new TransportException(null !== $error ? $error : $e->getMessage(), 0, $e);
    }

    private function validateMessageAdded(\Redis $redis, int $added): void
    {
        if (0 === $added) {
            $error = $redis->getLastError();
            if (null !== $error) {
                $redis->clearLastError();
            }
            throw new TransportException(null !== $error ? $error : 'Could not add a message to the redis queue.');
        }
    }

    private function claimOldPendingMessages(): void
    {
        $now = microtime(true);
        $timeout = $now - ($this->redeliverTimeout / 1000);

        // Check for abandoned messages
        $abandonedMessages = [];
        foreach ($this->processingMessages as $id => $info) {
            if ($info['timestamp'] < $timeout) {
                $abandonedMessages[$id] = $info['message'];
            }
        }

        if (count($abandonedMessages) > 0) {
            $redis = $this->getRedis();
            try {
                foreach ($abandonedMessages as $id => $message) {
                    // Re-add abandoned message to queue (at the front for priority)
                    $redis->lPush($this->queue, $message);
                    unset($this->processingMessages[$id]);
                }
            } catch (\RedisException $e) {
                throw new TransportException($e->getMessage(), 0, $e);
            }
        }

        $this->nextClaim = $now + $this->claimInterval;
    }

    public function ack(string $id): void
    {
        // Remove from processing messages
        unset($this->processingMessages[$id]);
    }

    public function reject(string $id): void
    {
        // Remove from processing messages (do not re-queue)
        unset($this->processingMessages[$id]);
    }

    /**
     * @param int|null $seconds the minimum duration the message should be kept alive
     */
    public function keepalive(string $id, ?int $seconds = null): void
    {
        if (null !== $seconds && $this->redeliverTimeout < $seconds * 1000) {
            throw new TransportException(\sprintf('Redis redeliver_timeout (%ds) cannot be smaller than the keepalive interval (%ds).', $this->redeliverTimeout / 1000, $seconds));
        }

        // Update timestamp to keep message alive
        if (isset($this->processingMessages[$id])) {
            $this->processingMessages[$id]['timestamp'] = microtime(true);
        }
    }

    public function cleanup(): void
    {
        static $unlink = true;
        $redis = $this->getRedis();

        if ($unlink) {
            try {
                $unlinkResult = $redis->unlink($this->queue, $this->delayedQueue);
                $unlink = is_int($unlinkResult) && $unlinkResult > 0;
            } catch (\Throwable) {
                $unlink = false;
            }
        }

        if (false === $unlink) {
            $redis->del($this->queue, $this->delayedQueue);
        }
    }

    public function getMessageCount(): int
    {
        $redis = $this->getRedis();

        try {
            // Get count from normal queue
            $normalCount = (int) $redis->lLen($this->queue);

            // Get count from delayed queue
            $delayedCount = (int) $redis->zCard($this->delayedQueue);

            // Include processing messages
            $processingCount = count($this->processingMessages);

            return $normalCount + $delayedCount + $processingCount;
        } catch (\RedisException $e) {
            return 0;
        }
    }

    public function close(): void
    {
        // Connection is managed externally, just clear processing messages
        $this->processingMessages = [];
    }
}
