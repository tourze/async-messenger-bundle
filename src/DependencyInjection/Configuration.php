<?php

namespace Tourze\AsyncMessengerBundle\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class Configuration implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('async_messenger');
        $rootNode = $treeBuilder->getRootNode();

        $rootNode
            ->children()
                ->booleanNode('auto_configure_transports')
                    ->defaultTrue()
                    ->info('Whether to automatically configure async_doctrine and async_redis transports')
                ->end()
                ->arrayNode('doctrine')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->scalarNode('dsn')
                            ->defaultValue('%env(ASYNC_DOCTRINE_DSN)%')
                        ->end()
                        ->scalarNode('table_name')
                            ->defaultValue('messenger_messages')
                        ->end()
                        ->scalarNode('queue_name')
                            ->defaultValue('default')
                        ->end()
                        ->integerNode('redeliver_timeout')
                            ->defaultValue(3600)
                        ->end()
                        ->booleanNode('auto_setup')
                            ->defaultTrue()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('redis')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->scalarNode('dsn')
                            ->defaultValue('%env(ASYNC_REDIS_DSN)%')
                        ->end()
                        ->scalarNode('stream')
                            ->defaultValue('messages')
                        ->end()
                        ->scalarNode('group')
                            ->defaultValue('symfony')
                        ->end()
                        ->scalarNode('consumer')
                            ->defaultValue('consumer')
                        ->end()
                        ->booleanNode('auto_setup')
                            ->defaultTrue()
                        ->end()
                        ->integerNode('stream_max_entries')
                            ->defaultValue(0)
                        ->end()
                        ->integerNode('dbindex')
                            ->defaultValue(0)
                        ->end()
                    ->end()
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }
}