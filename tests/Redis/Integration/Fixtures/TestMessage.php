<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration\Fixtures;

/**
 * 带有内容属性的测试消息对象。
 */
class TestMessage
{
    public string $content;

    public string $id;
}
