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
 * A Redis connection.
 *
 * @author Alexander Schranz <alexander@sulu.io>
 * @author Antoine Bluchet <soyuka@gmail.com>
 * @author Robin Chalas <robin.chalas@gmail.com>
 *
 * @internal
 *
 * @final
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

    private \Redis $redis;
    private string $queue;
    private string $delayedQueue;
    private bool $autoSetup;
    private int $maxEntries;
    private int $redeliverTimeout;
    private float $nextClaim = 0.0;
    private float $claimInterval;
    private array $processingMessages = [];

    public function __construct(\Redis $redis, array $options = [])
    {
        if (version_compare(phpversion('redis'), '4.3.0', '<')) {
            throw new LogicException('The redis transport requires php-redis 4.3.0 or higher.');
        }

        if (!$redis->isConnected()) {
            throw new InvalidArgumentException('Redis connection must be established before creating the transport.');
        }

        $options += self::DEFAULT_OPTIONS;

        if ('' === $options['queue']) {
            throw new InvalidArgumentException('"queue" should be configured, got an empty string.');
        }

        $this->redis = $redis;
        $this->queue = $options['queue'];
        $this->delayedQueue = $options['delayed_queue'];
        $this->autoSetup = $options['auto_setup'];
        $this->maxEntries = $options['queue_max_entries'];
        $this->redeliverTimeout = $options['redeliver_timeout'] * 1000;
        $this->claimInterval = $options['claim_interval'] / 1000;
    }

    private function getRedis(): \Redis
    {
        return $this->redis;
    }


    public function get(): ?array
    {
        if ($this->autoSetup) {
            $this->setup();
        }

        $redis = $this->getRedis();
        $now = microtime(true) * 1000; // current time in milliseconds

        // First check delayed queue for messages that are ready
        try {
            $delayedMessages = $redis->zRangeByScore($this->delayedQueue, '0', (string)$now, ['limit' => [0, 1]]);
            if (!empty($delayedMessages)) {
                $message = $delayedMessages[0];
                // Remove from delayed queue
                $redis->zRem($this->delayedQueue, $message);

                // Parse the message and add to normal queue
                $decodedMessage = json_decode($message, true);
                if (is_array($decodedMessage) && isset($decodedMessage['body'])) {
                    // Add to normal queue for immediate processing (at the front for priority)
                    $id = $this->generateId();
                    $redis->lPush($this->queue, json_encode([
                        'id' => $id,
                        'body' => $decodedMessage['body'],
                        'headers' => $decodedMessage['headers'] ?? [],
                        'timestamp' => $now
                    ]));
                }
            }
        } catch (\RedisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        // Check for redeliver of abandoned messages
        if ($this->nextClaim <= microtime(true)) {
            $this->claimOldPendingMessages();
        }

        // Get message from normal queue (FIFO: get from left)
        try {
            $message = $redis->lPop($this->queue);
            if (!$message) {
                return null;
            }

            $decodedMessage = json_decode($message, true);
            if (!is_array($decodedMessage) || !isset($decodedMessage['id'])) {
                return null;
            }

            // Track processing message
            $this->processingMessages[$decodedMessage['id']] = [
                'message' => $message,
                'timestamp' => microtime(true)
            ];

            return [
                'id' => $decodedMessage['id'],
                'data' => ['message' => json_encode([
                    'body' => $decodedMessage['body'],
                    'headers' => $decodedMessage['headers'] ?? []
                ])]
            ];
        } catch (\RedisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
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

    public function add(string $body, array $headers, int $delayInMs = 0): string
    {
        if ($this->autoSetup) {
            $this->setup();
        }

        $redis = $this->getRedis();
        $id = $this->generateId();

        try {
            if ($delayInMs > 0) {
                // Add to delayed queue with score as timestamp when message should be processed
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

                $added = $redis->zAdd($this->delayedQueue, $score, $message);
            } else {
                // Add to normal queue
                $message = json_encode([
                    'id' => $id,
                    'body' => $body,
                    'headers' => $headers,
                    'timestamp' => microtime(true) * 1000
                ]);

                if (false === $message) {
                    throw new TransportException(json_last_error_msg());
                }

                // Use rPush to add to end of list (FIFO)
                $added = $redis->rPush($this->queue, $message);

                // Trim queue if max entries is set
                if ($this->maxEntries > 0) {
                    // Keep the last N entries (trim from the left)
                    $redis->ltrim($this->queue, -$this->maxEntries, -1);
                }
            }
        } catch (\RedisException $e) {
            $error = $redis->getLastError();
            if ($error !== null) {
                $redis->clearLastError();
            }
            throw new TransportException($error !== null ? $error : $e->getMessage(), 0, $e);
        }

        if (!$added) {
            $error = $redis->getLastError();
            if ($error !== null) {
                $redis->clearLastError();
            }
            throw new TransportException($error !== null ? $error : 'Could not add a message to the redis queue.');
        }

        return $id;
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

        if (!empty($abandonedMessages)) {
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
                $unlink = false !== $redis->unlink($this->queue, $this->delayedQueue);
            } catch (\Throwable) {
                $unlink = false;
            }
        }

        if (!$unlink) {
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
