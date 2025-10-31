<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration\Fixtures;

/**
 * 用于性能测试的大消息测试类。
 */
class LargeTestMessage
{
    public string $id;

    public string $large_content;

    /** @var array<int, string> */
    public array $large_array;

    /** @var array<string, mixed> */
    public array $metadata;
}
