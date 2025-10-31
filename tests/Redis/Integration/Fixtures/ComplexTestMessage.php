<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration\Fixtures;

/**
 * 用于序列化测试的复杂测试消息。
 */
class ComplexTestMessage
{
    public string $id;

    public float $timestamp;

    /** @var array<string, mixed> */
    public array $metadata;

    public string $binary_data;

    public string $unicode_content;
}
