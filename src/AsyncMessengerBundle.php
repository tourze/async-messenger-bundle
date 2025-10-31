<?php

namespace Tourze\AsyncMessengerBundle;

use Doctrine\Bundle\DoctrineBundle\DoctrineBundle;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;
use Tourze\AsyncMessengerBundle\DependencyInjection\RemoveUnusedServicePass;
use Tourze\BundleDependency\BundleDependencyInterface;
use Tourze\RedisDedicatedConnectionBundle\RedisDedicatedConnectionBundle;

class AsyncMessengerBundle extends Bundle implements BundleDependencyInterface
{
    public function build(ContainerBuilder $container): void
    {
        parent::build($container);
        $container->addCompilerPass(new RemoveUnusedServicePass());
    }

    /**
     * @return array<class-string<Bundle>, array<string, bool>>
     */
    public static function getBundleDependencies(): array
    {
        return [
            DoctrineBundle::class => ['all' => true],
            RedisDedicatedConnectionBundle::class => ['all' => true],
        ];
    }
}
