<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\DependencyInjection;

use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Tourze\AsyncMessengerBundle\DependencyInjection\AsyncMessengerExtension;

class AsyncMessengerExtensionAutoConfigureTest extends TestCase
{
    private AsyncMessengerExtension $extension;
    private ContainerBuilder $container;

    public function test_prepend_withoutFrameworkExtension_doesNothing(): void
    {
        // Arrange
        $container = new ContainerBuilder();

        // Act
        $this->extension->prepend($container);

        // Assert
        $this->assertEmpty($container->getExtensionConfig('framework'));
    }

    public function test_prepend_withAutoConfigureEnabled_registersTransports(): void
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
        $this->assertArrayHasKey('transports', $messengerConfig);
        $this->assertArrayHasKey('async_doctrine', $messengerConfig['transports']);
        $this->assertArrayHasKey('async_redis', $messengerConfig['transports']);

        // Cleanup
        unset($_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }

    public function test_prepend_withAutoConfigureDisabled_doesNotRegisterTransports(): void
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

    public function test_prepend_withSimpleDsnConfiguration(): void
    {
        // Arrange
        $_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE'] = 'true';

        // Act
        $this->extension->prepend($this->container);

        // Assert
        $frameworkConfigs = $this->container->getExtensionConfig('framework');
        $messengerConfig = $this->findMessengerConfig($frameworkConfigs);

        $this->assertNotNull($messengerConfig);

        // 验证使用简单的 DSN 字符串
        $this->assertEquals('async-doctrine://', $messengerConfig['transports']['async_doctrine']);
        $this->assertEquals('async-redis://', $messengerConfig['transports']['async_redis']);

        // Cleanup
        unset($_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }

    private function findMessengerConfig(array $frameworkConfigs): ?array
    {
        foreach ($frameworkConfigs as $config) {
            if (isset($config['messenger'])) {
                return $config['messenger'];
            }
        }
        return null;
    }

    public function test_prepend_withExistingTransports_doesNotOverride(): void
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
        $messengerConfig = $this->findMessengerConfig($frameworkConfigs);
        
        $this->assertNotNull($messengerConfig);
        
        // 验证已存在的配置没有被覆盖
        $this->assertEquals('existing_doctrine_dsn', $messengerConfig['transports']['async_doctrine']['dsn']);
        $this->assertEquals('existing_table', $messengerConfig['transports']['async_doctrine']['options']['table_name']);
        $this->assertEquals('existing_redis_dsn', $messengerConfig['transports']['async_redis']['dsn']);
        $this->assertEquals('existing_stream', $messengerConfig['transports']['async_redis']['options']['stream']);
        
        // 验证其他 transport 仍然存在
        $this->assertArrayHasKey('custom_transport', $messengerConfig['transports']);
        
        // Cleanup
        unset($_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }


    public function test_prepend_withAutoConfigureDisabledViaServer_doesNotRegisterTransports(): void
    {
        // Arrange
        $_SERVER['ASYNC_MESSENGER_AUTO_CONFIGURE'] = '0';
        
        // Act
        $this->extension->prepend($this->container);
        
        // Assert
        $frameworkConfigs = $this->container->getExtensionConfig('framework');
        $this->assertEmpty($frameworkConfigs);
        
        // Cleanup
        unset($_SERVER['ASYNC_MESSENGER_AUTO_CONFIGURE']);
    }

    protected function setUp(): void
    {
        $this->extension = new AsyncMessengerExtension();
        $this->container = new ContainerBuilder();

        // 模拟 framework extension
        $this->container->registerExtension(new MockFrameworkExtension());
    }
}

class MockFrameworkExtension extends \Symfony\Component\DependencyInjection\Extension\Extension
{
    public function load(array $configs, ContainerBuilder $container): void
    {
        // Mock implementation
    }
    
    public function getAlias(): string
    {
        return 'framework';
    }
}