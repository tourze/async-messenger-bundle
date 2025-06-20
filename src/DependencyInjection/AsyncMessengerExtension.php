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

        // 获取现有的 messenger 配置
        $configs = $container->getExtensionConfig('framework');
        $messengerConfig = [];
        
        // 查找现有的 messenger 配置
        foreach ($configs as $config) {
            if (isset($config['messenger'])) {
                $messengerConfig = $config['messenger'];
                break;
            }
        }

        // 准备我们的 transport 配置
        $transports = [
            'async_doctrine' => [
                'dsn' => '%env(ASYNC_DOCTRINE_DSN)%',
                'options' => [
                    'table_name' => 'messenger_messages',
                    'queue_name' => 'default',
                    'redeliver_timeout' => 3600,
                    'auto_setup' => true,
                ],
            ],
            'async_redis' => [
                'dsn' => '%env(ASYNC_REDIS_DSN)%',
                'options' => [
                    'stream' => 'messages',
                    'group' => 'symfony',
                    'consumer' => 'consumer',
                    'auto_setup' => true,
                    'stream_max_entries' => 0, // 0 = unlimited
                    'dbindex' => 0,
                ],
            ],
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

        // 设置默认的环境变量（如果未设置）
        $this->setDefaultEnvVars($container);

        // 将配置添加到 framework
        $container->prependExtensionConfig('framework', [
            'messenger' => $messengerConfig
        ]);
    }

    private function setDefaultEnvVars(ContainerBuilder $container): void
    {
        // 设置默认的 DSN 环境变量
        $defaultEnvVars = [
            'ASYNC_DOCTRINE_DSN' => 'async-doctrine://default',
            'ASYNC_REDIS_DSN' => 'async-redis://localhost:6379',
        ];

        foreach ($defaultEnvVars as $key => $value) {
            if (!$container->hasParameter("env($key)")) {
                $container->setParameter("env($key)", $value);
            }
        }
    }
}
