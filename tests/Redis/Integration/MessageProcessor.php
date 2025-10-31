<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use Symfony\Component\Messenger\Envelope;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;

/**
 * 消息处理器 - 封装复杂的消息处理逻辑
 *
 * 职责：
 * - 统一消息消费的模式
 * - 提供批量处理和验证的能力
 * - 隔离消息确认逻辑
 */
final class MessageProcessor
{
    /** @return array<Envelope> */
    public function consumeAll(RedisTransport $transport): array
    {
        $receivedMessages = [];

        while (true) {
            $messages = $transport->get();
            if ([] === $messages) {
                break;
            }

            foreach ($messages as $msg) {
                $receivedMessages[] = $msg;
                $transport->ack($msg);
            }
        }

        return $receivedMessages;
    }

    /** @return array<Envelope> */
    public function consumeAndValidate(RedisTransport $transport, callable $validator): array
    {
        $receivedMessages = [];

        while (true) {
            $messages = $transport->get();
            if ([] === $messages) {
                break;
            }

            foreach ($messages as $msg) {
                $validator($msg);
                $receivedMessages[] = $msg;
                $transport->ack($msg);
            }
        }

        return $receivedMessages;
    }

    /** @return array<Envelope> */
    public function consumeCount(RedisTransport $transport, int $count): array
    {
        $consumedMessages = [];

        for ($i = 0; $i < $count; ++$i) {
            $messages = iterator_to_array($transport->get());
            if ([] === $messages) {
                break;
            }

            $consumedMessages[] = $messages[0];
            $transport->ack($messages[0]);
        }

        return $consumedMessages;
    }
}
