<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration\Fixtures;

/**
 * 用于Doctrine集成测试的消息对象
 */
class TestMessage
{
    public string $content;

    public int $index;

    public string $id;

    public function __construct(string $content = '', int $index = 0, string $id = '')
    {
        $this->content = $content;
        $this->index = $index;
        $this->id = '' !== $id ? $id : uniqid('msg_', true);
    }
}
