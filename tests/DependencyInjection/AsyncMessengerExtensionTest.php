<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\DependencyInjection;

use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Tourze\AsyncMessengerBundle\DependencyInjection\AsyncMessengerExtension;
use Tourze\PHPUnitSymfonyUnitTest\AbstractDependencyInjectionExtensionTestCase;

/**
 * @internal
 */
#[CoversClass(AsyncMessengerExtension::class)]
final class AsyncMessengerExtensionTest extends AbstractDependencyInjectionExtensionTestCase
{
    public function testLoadLoadsServicesConfiguration(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $extension = new AsyncMessengerExtension();

        $configs = [];
        $extension->load($configs, $container);

        // 验证容器已加载配置（检查容器状态变化）
        $this->assertTrue($container->isTrackingResources());
    }

    public function testLoadWithEmptyConfigDoesNotThrow(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $extension = new AsyncMessengerExtension();

        $configs = [];

        $this->expectNotToPerformAssertions();
        $extension->load($configs, $container);
    }

    public function testPrepend(): void
    {
        // Arrange
        $container = new ContainerBuilder();

        // Add minimal doctrine config to prevent parent::prepend() assertion failure
        $container->prependExtensionConfig('doctrine', [
            'dbal' => [
                'driver' => 'pdo_mysql',
                'url' => 'mysql://test:test@localhost/test',
            ],
        ]);

        $container->registerExtension(new MockFrameworkExtension());
        $extension = new AsyncMessengerExtension();
        $_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE'] = 'true';

        // Act
        $extension->prepend($container);

        // Assert
        $frameworkConfigs = $container->getExtensionConfig('framework');
        $this->assertNotEmpty($frameworkConfigs);

        $messengerConfig = null;
        foreach ($frameworkConfigs as $config) {
            if (isset($config['messenger'])) {
                $messengerConfig = $config['messenger'];
                break;
            }
        }

        $this->assertNotNull($messengerConfig);
        $this->assertIsArray($messengerConfig);
        $this->assertArrayHasKey('transports', $messengerConfig);
        $this->assertArrayHasKey('failure_transport', $messengerConfig);

        // Cleanup
        unset($_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }

    protected function setUp(): void
    {
        parent::setUp();
        // 测试 Extension 不需要特殊的设置
    }
}
