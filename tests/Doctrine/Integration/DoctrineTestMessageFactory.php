<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

/**
 * 测试消息工厂 - 标准化消息创建
 */
final class DoctrineTestMessageFactory
{
    /** @param array<string, mixed> $extraFields */
    public static function create(string $queue, int $index = 0, array $extraFields = []): \stdClass
    {
        $message = new \stdClass();
        $message->queue = $queue;
        $message->content = "{$queue} message {$index}";
        $message->id = "{$queue}-{$index}";

        // 设置已知的额外字段，避免动态属性名称
        if (isset($extraFields['content'])) {
            $message->content = $extraFields['content'];
        }
        if (isset($extraFields['priority'])) {
            $message->priority = $extraFields['priority'];
        }
        if (isset($extraFields['type'])) {
            $message->type = $extraFields['type'];
        }

        return $message;
    }

    /**
     * @param array<string, mixed> $extraFields
     * @return array<\stdClass>
     */
    public static function createBatch(string $queue, int $count, array $extraFields = []): array
    {
        $messages = [];
        for ($i = 0; $i < $count; ++$i) {
            $messages[] = self::create($queue, $i, $extraFields);
        }

        return $messages;
    }
}
