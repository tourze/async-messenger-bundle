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
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * @author Alexander Schranz <alexander@sulu.io>
 * @author Antoine Bluchet <soyuka@gmail.com>
 */
class RedisSender implements SenderInterface
{
    public function __construct(
        private readonly Connection $connection,
        private readonly SerializerInterface $serializer,
    ) {
    }

    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        $delayStamp = $envelope->last(DelayStamp::class);
        assert($delayStamp instanceof DelayStamp || null === $delayStamp);
        $delayInMs = null !== $delayStamp ? $delayStamp->getDelay() : 0;

        $bodyValue = $encodedMessage['body'] ?? '';
        $body = is_string($bodyValue) ? $bodyValue : '';

        $headers = $encodedMessage['headers'] ?? [];

        // 确保headers是string键的数组
        $stringKeyHeaders = [];
        if (is_array($headers)) {
            foreach ($headers as $key => $value) {
                $stringKeyHeaders[(string) $key] = $value;
            }
        }

        $id = $this->connection->add(
            $body,
            $stringKeyHeaders,
            $delayInMs
        );

        return $envelope->with(new TransportMessageIdStamp($id));
    }
}
