<?php

namespace Tourze\AsyncMessengerBundle\DependencyInjection;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Tourze\SymfonyDependencyServiceLoader\AppendDoctrineConnectionExtension;

final class AsyncMessengerExtension extends AppendDoctrineConnectionExtension
{
    protected function getConfigDir(): string
    {
        return __DIR__ . '/../Resources/config';
    }

    public function prepend(ContainerBuilder $container): void
    {
        parent::prepend($container);

        // 只有在 framework bundle 存在时才配置
        if (!$container->hasExtension('framework')) {
            return;
        }

        // 检查是否应该自动配置（通过环境变量）
        $autoConfigureEnv = $_ENV['ASYNC_MESSENGER_AUTO_CONFIGURE'] ?? 'true';
        if ('false' === $autoConfigureEnv || '0' === $autoConfigureEnv) {
            return;
        }

        // 获取现有的 messenger 配置
        /** @var array<int, array<string, mixed>> $frameworkConfigs */
        $frameworkConfigs = $container->getExtensionConfig('framework');
        /** @var array<string, mixed> $messengerConfig */
        $messengerConfig = [];

        // 查找现有的 messenger 配置
        foreach ($frameworkConfigs as $frameworkConfig) {
            if (isset($frameworkConfig['messenger']) && is_array($frameworkConfig['messenger'])) {
                /** @var array<string, mixed> $existingMessengerConfig */
                $existingMessengerConfig = $frameworkConfig['messenger'];
                $messengerConfig = $existingMessengerConfig;
                break;
            }
        }

        // 准备我们的 transport 配置 - 非常简单，只需要 DSN
        // TransportFactory 会处理所有的配置细节
        /** @var array<string, mixed> $transports */
        $transports = [
            'async_doctrine' => 'async-doctrine://',
            'async_redis' => 'async-redis://',
            'async' => [
                'dsn' => 'failover://async_doctrine,async_redis',
                'options' => [
                    'circuit_breaker' => [
                        'failure_threshold' => 5,
                        'success_threshold' => 2,
                        'timeout' => 30,
                    ],
                    'consumption_strategy' => 'adaptive_priority',
                    'try_unhealthy_on_failure' => true,
                ],
            ],
            'sync' => 'sync://',
        ];

        // 合并 transport 配置，不覆盖已存在的
        if (!array_key_exists('transports', $messengerConfig) || !is_array($messengerConfig['transports'])) {
            $messengerConfig['transports'] = [];
        }

        /** @var array<string, mixed> $existingTransports */
        $existingTransports = $messengerConfig['transports'];
        foreach ($transports as $name => $config) {
            // 任何情况下，都是直接覆盖
            $existingTransports[$name] = $config;
        }
        $messengerConfig['transports'] = $existingTransports;

        // 设置默认的 failure_transport
        $messengerConfig['failure_transport'] = 'async_doctrine';

        // 将配置添加到 framework
        $container->prependExtensionConfig('framework', [
            'messenger' => $messengerConfig,
        ]);
    }

    protected function getDoctrineConnectionName(): string
    {
        return 'async_messenger';
    }
}
