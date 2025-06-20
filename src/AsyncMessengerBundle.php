<?php

namespace Tourze\AsyncMessengerBundle;

use Doctrine\Bundle\DoctrineBundle\DoctrineBundle;
use Snc\RedisBundle\SncRedisBundle;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;
use Tourze\AsyncMessengerBundle\DependencyInjection\RemoveUnusedServicePass;
use Tourze\BundleDependency\BundleDependencyInterface;
use Tourze\DoctrineDedicatedConnectionBundle\DoctrineDedicatedConnectionBundle;
use Tourze\RedisDedicatedConnectionBundle\RedisDedicatedConnectionBundle;

class AsyncMessengerBundle extends Bundle implements BundleDependencyInterface
{
    public function build(ContainerBuilder $container): void
    {
        parent::build($container);
        $container->addCompilerPass(new RemoveUnusedServicePass());
    }

    public static function getBundleDependencies(): array
    {
        return [
            DoctrineBundle::class => ['all' => true],
            SncRedisBundle::class => ['all' => true],
            DoctrineDedicatedConnectionBundle::class => ['all' => true],
            RedisDedicatedConnectionBundle::class => ['all' => true],
        ];
    }
}
