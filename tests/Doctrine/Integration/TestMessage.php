<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

/**
 * 测试消息类 - 专用于Doctrine集成测试
 *
 * 设计原则：
 * - 纯数据结构，无业务逻辑
 * - 使用公共属性简化测试代码
 * - 提供默认值确保测试稳定性
 */
final class TestMessage
{
    public function __construct(
        public string $content = '',
        public string $type = '',
        public int $index = 0,
    ) {
    }
}
