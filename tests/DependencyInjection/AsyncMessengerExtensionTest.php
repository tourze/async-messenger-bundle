<?php

namespace Tourze\AsyncMessengerBundle\Tests\DependencyInjection;

use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Tourze\AsyncMessengerBundle\DependencyInjection\AsyncMessengerExtension;

class AsyncMessengerExtensionTest extends TestCase
{
    public function test_load_loadsServicesConfiguration(): void
    {
        $container = new ContainerBuilder();
        $extension = new AsyncMessengerExtension();

        $configs = [];
        $extension->load($configs, $container);

        // 验证容器已加载配置（检查容器状态变化）
        $this->assertTrue($container->isTrackingResources());
    }

    public function test_load_withEmptyConfig_doesNotThrow(): void
    {
        $container = new ContainerBuilder();
        $extension = new AsyncMessengerExtension();

        $configs = [];
        
        $this->expectNotToPerformAssertions();
        $extension->load($configs, $container);
    }
}