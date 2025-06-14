<?php

namespace Tourze\AsyncMessengerBundle;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;
use Tourze\AsyncMessengerBundle\DependencyInjection\RemoveUnusedServicePass;

class AsyncMessengerBundle extends Bundle
{
    public function build(ContainerBuilder $container): void
    {
        parent::build($container);
        $container->addCompilerPass(new RemoveUnusedServicePass());
    }
}
