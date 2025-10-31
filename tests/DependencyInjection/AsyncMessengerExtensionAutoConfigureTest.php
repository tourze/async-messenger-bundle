<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\DependencyInjection;

use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Tourze\AsyncMessengerBundle\AsyncMessageInterface;
use Tourze\AsyncMessengerBundle\DependencyInjection\AsyncMessengerExtension;
use Tourze\PHPUnitSymfonyUnitTest\AbstractDependencyInjectionExtensionTestCase;

/**
 * @internal
 */
#[CoversClass(AsyncMessengerExtension::class)]
final class AsyncMessengerExtensionAutoConfigureTest extends AbstractDependencyInjectionExtensionTestCase
{
    private AsyncMessengerExtension $extension;

    private ContainerBuilder $container;

    public function testPrependWithoutFrameworkExtensionDoesNothing(): void
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

        // Act
        $this->extension->prepend($container);

        // Assert - framework config should still be empty since no framework extension
        $this->assertEmpty($container->getExtensionConfig('framework'));
    }

    public function testPrependWithAutoConfigureEnabledRegistersTransports(): void
    {
        // Arrange
        $_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE'] = 'true';

        // Act
        $this->extension->prepend($this->container);

        // Assert
        $frameworkConfigs = $this->container->getExtensionConfig('framework');
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
        $this->assertIsArray($messengerConfig['transports']);
        $this->assertArrayHasKey('async_doctrine', $messengerConfig['transports']);
        $this->assertArrayHasKey('async_redis', $messengerConfig['transports']);
        $this->assertArrayHasKey('async', $messengerConfig['transports']);
        $this->assertArrayHasKey('sync', $messengerConfig['transports']);

        // Check failure transport
        $this->assertEquals('async_doctrine', $messengerConfig['failure_transport']);

        // Cleanup
        unset($_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }

    public function testPrependWithAutoConfigureDisabledDoesNotRegisterTransports(): void
    {
        // Arrange
        $_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE'] = 'false';

        // Act
        $this->extension->prepend($this->container);

        // Assert
        $frameworkConfigs = $this->container->getExtensionConfig('framework');
        $this->assertEmpty($frameworkConfigs);

        // Cleanup
        unset($_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }

    public function testPrependWithSimpleDsnConfiguration(): void
    {
        // Arrange
        $_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE'] = 'true';

        // Act
        $this->extension->prepend($this->container);

        // Assert
        $frameworkConfigs = $this->container->getExtensionConfig('framework');
        $this->assertNotEmpty($frameworkConfigs);
        /** @var array<int, array<string, mixed>> $frameworkConfigsTyped */
        $frameworkConfigsTyped = $frameworkConfigs;
        $messengerConfig = $this->findMessengerConfig($frameworkConfigsTyped);

        $this->assertNotNull($messengerConfig);
        $this->assertIsArray($messengerConfig['transports'] ?? null);

        // 验证使用简单的 DSN 字符串
        $this->assertEquals('async-doctrine://', $messengerConfig['transports']['async_doctrine']);
        $this->assertEquals('async-redis://', $messengerConfig['transports']['async_redis']);

        // Cleanup
        unset($_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }

    /**
     * @param array<int, array<string, mixed>> $frameworkConfigs
     * @return array<string, mixed>|null
     */
    private function findMessengerConfig(array $frameworkConfigs): ?array
    {
        foreach ($frameworkConfigs as $config) {
            if (isset($config['messenger']) && is_array($config['messenger'])) {
                /** @var array<string, mixed> */
                return $config['messenger'];
            }
        }

        return null;
    }

    public function testPrependWithExistingTransportsOverridesConfiguration(): void
    {
        // Arrange
        $_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE'] = 'true';

        // 先设置已存在的 transport
        $existingConfig = [
            'messenger' => [
                'transports' => [
                    'async_doctrine' => [
                        'dsn' => 'existing_doctrine_dsn',
                        'options' => ['table_name' => 'existing_table'],
                    ],
                    'async_redis' => [
                        'dsn' => 'existing_redis_dsn',
                        'options' => ['stream' => 'existing_stream'],
                    ],
                    'custom_transport' => [
                        'dsn' => 'custom_dsn',
                    ],
                ],
            ],
        ];
        $this->container->prependExtensionConfig('framework', $existingConfig);

        // Act
        $this->extension->prepend($this->container);

        // Assert
        $frameworkConfigs = $this->container->getExtensionConfig('framework');
        $this->assertNotEmpty($frameworkConfigs);
        /** @var array<int, array<string, mixed>> $frameworkConfigsTyped */
        $frameworkConfigsTyped = $frameworkConfigs;
        $messengerConfig = $this->findMessengerConfig($frameworkConfigsTyped);

        $this->assertNotNull($messengerConfig);
        $this->assertIsArray($messengerConfig['transports']);

        // 验证配置被覆盖为新的配置
        $this->assertEquals('async-doctrine://', $messengerConfig['transports']['async_doctrine']);
        $this->assertEquals('async-redis://', $messengerConfig['transports']['async_redis']);

        // 验证 async transport 被添加
        $this->assertArrayHasKey('async', $messengerConfig['transports']);
        $this->assertIsArray($messengerConfig['transports']['async']);
        $this->assertEquals('failover://async_doctrine,async_redis', $messengerConfig['transports']['async']['dsn']);

        // 验证其他 transport 仍然存在
        $this->assertArrayHasKey('custom_transport', $messengerConfig['transports']);

        // Cleanup
        unset($_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }

    public function testPrependWithAutoConfigureDisabledViaEnvDoesNotRegisterTransports(): void
    {
        // Arrange
        $_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE'] = '0';

        // Act
        $this->extension->prepend($this->container);

        // Assert
        $frameworkConfigs = $this->container->getExtensionConfig('framework');

        // 查找 messenger 配置
        $messengerConfig = null;
        foreach ($frameworkConfigs as $config) {
            if (isset($config['messenger'])) {
                $messengerConfig = $config['messenger'];
                break;
            }
        }

        // 当 auto configure 被禁用时，不应该有 messenger 配置
        $this->assertNull($messengerConfig);

        // Cleanup
        unset($_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }

    protected function setUp(): void
    {
        $this->extension = new AsyncMessengerExtension();
        $this->container = new ContainerBuilder();

        // Add minimal doctrine config to prevent parent::prepend() assertion failure
        $this->container->prependExtensionConfig('doctrine', [
            'dbal' => [
                'driver' => 'pdo_mysql',
                'url' => 'mysql://test:test@localhost/test',
            ],
        ]);

        // 模拟 framework extension
        $this->container->registerExtension(new MockFrameworkExtension());
    }
}
