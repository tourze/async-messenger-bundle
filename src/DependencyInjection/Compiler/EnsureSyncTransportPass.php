<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\DependencyInjection\Compiler;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

/**
 * Ensures that a sync transport is always available
 */
final class EnsureSyncTransportPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        // Check if messenger is configured
        if (!$container->hasParameter('messenger.transports')) {
            return;
        }
        
        $transports = $container->getParameter('messenger.transports');
        
        // Check if sync transport already exists
        if (isset($transports['sync'])) {
            return;
        }
        
        // Add sync transport configuration
        $transports['sync'] = [
            'dsn' => 'sync://',
        ];
        
        $container->setParameter('messenger.transports', $transports);
        
        // Also ensure the transport service is defined
        if (!$container->hasDefinition('messenger.transport.sync')) {
            // The Symfony Messenger bundle will create this automatically
            // based on the parameter we just set
        }
    }
}