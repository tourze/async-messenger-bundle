<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

/**
 * 测试消息工厂 - 标准化消息创建
 *
 * 职责：
 * - 标准化测试消息的创建模式
 * - 确保消息属性的一致性
 * - 避免动态属性名称，提供已知字段的显式设置
 */
final class TestMessageFactory
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
        if (isset($extraFields['delayed'])) {
            $message->delayed = $extraFields['delayed'];
        }
        if (isset($extraFields['queueName'])) {
            $message->queueName = $extraFields['queueName'];
        }
        if (isset($extraFields['batchId'])) {
            $message->batchId = $extraFields['batchId'];
        }
        if (isset($extraFields['type'])) {
            $message->type = $extraFields['type'];
        }
        if (isset($extraFields['prefix'])) {
            $message->prefix = $extraFields['prefix'];
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
