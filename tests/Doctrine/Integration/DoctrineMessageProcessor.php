<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Symfony\Component\Messenger\Envelope;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;

/**
 * 消息处理器 - 封装复杂的消息处理逻辑
 */
final class DoctrineMessageProcessor
{
    /** @return array<Envelope> */
    public function consumeAll(DoctrineTransport $transport): array
    {
        $receivedMessages = [];

        while (true) {
            $envelopes = iterator_to_array($transport->get());
            if ([] === $envelopes) {
                break;
            }

            foreach ($envelopes as $envelope) {
                $receivedMessages[] = $envelope;
                $transport->ack($envelope);
            }
        }

        return $receivedMessages;
    }

    /** @return array<Envelope> */
    public function consumeAndValidate(DoctrineTransport $transport, callable $validator): array
    {
        $receivedMessages = [];

        while (true) {
            $envelopes = iterator_to_array($transport->get());
            if ([] === $envelopes) {
                break;
            }

            foreach ($envelopes as $envelope) {
                $validator($envelope);
                $receivedMessages[] = $envelope;
                $transport->ack($envelope);
            }
        }

        return $receivedMessages;
    }

    /** @return array<Envelope> */
    public function consumeCount(DoctrineTransport $transport, int $count): array
    {
        $consumedMessages = [];

        for ($i = 0; $i < $count; ++$i) {
            $envelopes = iterator_to_array($transport->get());
            if ([] === $envelopes) {
                break;
            }

            $consumedMessages[] = $envelopes[0];
            $transport->ack($envelopes[0]);
        }

        return $consumedMessages;
    }
}
