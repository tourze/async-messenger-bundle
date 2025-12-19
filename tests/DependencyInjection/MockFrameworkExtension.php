<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\DependencyInjection;

use Tourze\SymfonyDependencyServiceLoader\AutoExtension;

/**
 * @internal
 * 仅用于测试目的的模拟类
 */
final class MockFrameworkExtension extends AutoExtension
{
    protected function getConfigDir(): string
    {
        return __DIR__ . '/../../src/Resources/config';
    }

    public function getAlias(): string
    {
        return 'framework';
    }
}
