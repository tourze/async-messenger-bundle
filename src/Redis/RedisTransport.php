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
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * @author Alexander Schranz <alexander@sulu.io>
 * @author Antoine Bluchet <soyuka@gmail.com>
 */
class RedisTransport implements TransportInterface, SetupableTransportInterface, MessageCountAwareInterface
{
    private SerializerInterface $serializer;
    private RedisReceiver $receiver;
    private RedisSender $sender;

    public function __construct(
        private readonly Connection $connection,
        ?SerializerInterface $serializer = null,
    ) {
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    public function get(): iterable
    {
        return $this->getReceiver()->get();
    }

    private function getReceiver(): RedisReceiver
    {
        return $this->receiver ??= new RedisReceiver($this->connection, $this->serializer);
    }

    public function ack(Envelope $envelope): void
    {
        $this->getReceiver()->ack($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        $this->getReceiver()->reject($envelope);
    }

    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        $this->getReceiver()->keepalive($envelope, $seconds);
    }

    public function send(Envelope $envelope): Envelope
    {
        return $this->getSender()->send($envelope);
    }

    private function getSender(): RedisSender
    {
        return $this->sender ??= new RedisSender($this->connection, $this->serializer);
    }

    public function setup(): void
    {
        $this->connection->setup();
    }

    public function getMessageCount(): int
    {
        return $this->getReceiver()->getMessageCount();
    }

    public function close(): void
    {
        $this->connection->close();
    }
}
