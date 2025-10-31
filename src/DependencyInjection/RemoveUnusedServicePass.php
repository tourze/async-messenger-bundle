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
            if (!$container->hasDefinition($serviceId)) {
                continue;
            }

            try {
                $definition = $container->findDefinition($serviceId);
                if (null === $definition->getClass() || '' === $definition->getClass()) {
                    continue;
                }

                if (!class_exists($definition->getClass())) {
                    continue;
                }

                // 请求不需要注册
                if (is_subclass_of($definition->getClass(), AsyncMessageInterface::class)) {
                    $container->removeDefinition($serviceId);
                }
            } catch (\Throwable) {
                continue;
            }
        }
    }
}
