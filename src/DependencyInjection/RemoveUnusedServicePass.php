<?php

namespace Tourze\AsyncMessengerBundle\DependencyInjection;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Tourze\AsyncContracts\AsyncMessageInterface;

/**
 * 减少一些不必要的服务注册
 */
class RemoveUnusedServicePass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        foreach ($container->getServiceIds() as $serviceId) {
            $definition = $container->findDefinition($serviceId);
            if (empty($definition->getClass())) {
                continue;
            }

            try {
                if (!class_exists($definition->getClass())) {
                    continue;
                }
            } catch (\Throwable) {
                continue;
            }

            // 请求不需要注册
            if (is_subclass_of($definition->getClass(), AsyncMessageInterface::class)) {
                $container->removeDefinition($serviceId);
                continue;
            }
        }
    }
}
