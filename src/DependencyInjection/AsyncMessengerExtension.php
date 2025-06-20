<?php

namespace Tourze\AsyncMessengerBundle\DependencyInjection;

use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\Extension;
use Symfony\Component\DependencyInjection\Extension\PrependExtensionInterface;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;

class AsyncMessengerExtension extends Extension implements PrependExtensionInterface
{
    public function load(array $configs, ContainerBuilder $container): void
    {
        $loader = new YamlFileLoader(
            $container,
            new FileLocator(__DIR__ . '/../Resources/config')
        );
        $loader->load('services.yaml');
    }

    public function prepend(ContainerBuilder $container): void
    {
        // 只有在 framework bundle 存在时才配置
        if (!$container->hasExtension('framework')) {
            return;
        }

        // 检查是否应该自动配置（通过环境变量）
        $autoConfigureEnv = $_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE'] ?? $_SERVER['ASYNC_MESSENGER_AUTO_CONFIGURE'] ?? 'true';
        if ($autoConfigureEnv === 'false' || $autoConfigureEnv === '0') {
            return;
        }

        // 获取现有的 messenger 配置
        $frameworkConfigs = $container->getExtensionConfig('framework');
        $messengerConfig = [];
        
        // 查找现有的 messenger 配置
        foreach ($frameworkConfigs as $frameworkConfig) {
            if (isset($frameworkConfig['messenger'])) {
                $messengerConfig = $frameworkConfig['messenger'];
                break;
            }
        }

        // 准备我们的 transport 配置 - 非常简单，只需要 DSN
        // TransportFactory 会处理所有的配置细节
        $transports = [
            'async_doctrine' => 'async-doctrine://',
            'async_redis' => 'async-redis://',
        ];

        // 合并 transport 配置，不覆盖已存在的
        if (!isset($messengerConfig['transports'])) {
            $messengerConfig['transports'] = [];
        }
        
        foreach ($transports as $name => $config) {
            if (!isset($messengerConfig['transports'][$name])) {
                $messengerConfig['transports'][$name] = $config;
            }
        }
        
        // 设置默认的 failure_transport
        if (!isset($messengerConfig['failure_transport'])) {
            $messengerConfig['failure_transport'] = 'async_doctrine';
        }

        // 将配置添加到 framework
        $container->prependExtensionConfig('framework', [
            'messenger' => $messengerConfig
        ]);
    }

}
