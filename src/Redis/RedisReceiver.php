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

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Stamp\RedisReceivedStamp;

/**
 * @author Alexander Schranz <alexander@sulu.io>
 * @author Antoine Bluchet <soyuka@gmail.com>
 */
class RedisReceiver implements MessageCountAwareInterface
{
    private SerializerInterface $serializer;

    public function __construct(
        private readonly Connection $connection,
        ?SerializerInterface $serializer = null,
    ) {
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    /**
     * @return iterable<Envelope>
     */
    public function get(): iterable
    {
        $message = $this->connection->get();

        if (null === $message) {
            return [];
        }

        if (!$this->isValidMessage($message)) {
            return $this->get();
        }

        $envelope = $this->decodeMessage($message);

        if (null === $envelope) {
            return [];
        }

        $messageIdValue = $message['id'] ?? '';
        $messageId = is_scalar($messageIdValue) ? (string) $messageIdValue : '';

        return [$envelope
            ->withoutAll(TransportMessageIdStamp::class)
            ->with(
                new RedisReceivedStamp($messageId),
                new TransportMessageIdStamp($messageId)
            )];
    }

    /**
     * @param array<string, mixed> $message
     */
    private function isValidMessage(array $message): bool
    {
        if (null !== $message['data']) {
            return true;
        }

        try {
            $messageIdValue = $message['id'] ?? '';
            $messageId = is_scalar($messageIdValue) ? (string) $messageIdValue : '';
            $this->connection->reject($messageId);
        } catch (TransportException $e) {
            if (null !== $e->getPrevious()) {
                throw $e;
            }
        }

        return false;
    }

    /**
     * @param array<string, mixed> $message
     */
    private function decodeMessage(array $message): ?Envelope
    {
        $messageData = $message['data'] ?? null;
        if (!is_array($messageData) || !isset($messageData['message'])) {
            return null;
        }

        $messageContentValue = $messageData['message'] ?? '';
        $messageContent = is_string($messageContentValue) ? $messageContentValue : '';
        $redisEnvelope = json_decode($messageContent, true);

        if (!is_array($redisEnvelope)) {
            return null;
        }

        try {
            if (\array_key_exists('body', $redisEnvelope) && \array_key_exists('headers', $redisEnvelope)) {
                return $this->serializer->decode([
                    'body' => $redisEnvelope['body'],
                    'headers' => $redisEnvelope['headers'],
                ]);
            }

            return $this->serializer->decode($redisEnvelope);
        } catch (MessageDecodingFailedException $exception) {
            $messageIdValue = $message['id'] ?? '';
            $messageId = is_scalar($messageIdValue) ? (string) $messageIdValue : '';
            $this->connection->reject($messageId);
            throw $exception;
        }
    }

    public function reject(Envelope $envelope): void
    {
        $this->connection->reject($this->findRedisReceivedStamp($envelope)->getId());
    }

    private function findRedisReceivedStamp(Envelope $envelope): RedisReceivedStamp
    {
        $redisReceivedStamp = $envelope->last(RedisReceivedStamp::class);
        assert($redisReceivedStamp instanceof RedisReceivedStamp || null === $redisReceivedStamp);

        if (null === $redisReceivedStamp) {
            throw new LogicException('No RedisReceivedStamp found on the Envelope.');
        }

        return $redisReceivedStamp;
    }

    public function ack(Envelope $envelope): void
    {
        $this->connection->ack($this->findRedisReceivedStamp($envelope)->getId());
    }

    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        $this->connection->keepalive($this->findRedisReceivedStamp($envelope)->getId(), $seconds);
    }

    public function getMessageCount(): int
    {
        return $this->connection->getMessageCount();
    }
}
