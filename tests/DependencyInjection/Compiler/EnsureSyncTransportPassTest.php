<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\DependencyInjection\Compiler;

use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Tourze\AsyncMessengerBundle\DependencyInjection\Compiler\EnsureSyncTransportPass;

class EnsureSyncTransportPassTest extends TestCase
{
    private EnsureSyncTransportPass $pass;
    private ContainerBuilder $container;
    
    protected function setUp(): void
    {
        $this->pass = new EnsureSyncTransportPass();
        $this->container = new ContainerBuilder();
    }
    
    public function test_processDoesNothingWhenMessengerNotConfigured(): void
    {
        // Act
        $this->pass->process($this->container);
        
        // Assert
        $this->assertFalse($this->container->hasParameter('messenger.transports'));
    }
    
    public function test_processDoesNothingWhenSyncTransportExists(): void
    {
        // Arrange
        $transports = [
            'async' => ['dsn' => 'redis://'],
            'sync' => ['dsn' => 'sync://'],
        ];
        $this->container->setParameter('messenger.transports', $transports);
        
        // Act
        $this->pass->process($this->container);
        
        // Assert
        $this->assertEquals($transports, $this->container->getParameter('messenger.transports'));
    }
    
    public function test_processAddsSyncTransportWhenMissing(): void
    {
        // Arrange
        $transports = [
            'async' => ['dsn' => 'redis://'],
            'failed' => ['dsn' => 'doctrine://'],
        ];
        $this->container->setParameter('messenger.transports', $transports);
        
        // Act
        $this->pass->process($this->container);
        
        // Assert
        $updatedTransports = $this->container->getParameter('messenger.transports');
        $this->assertArrayHasKey('sync', $updatedTransports);
        $this->assertEquals(['dsn' => 'sync://'], $updatedTransports['sync']);
        
        // Original transports should still exist
        $this->assertArrayHasKey('async', $updatedTransports);
        $this->assertArrayHasKey('failed', $updatedTransports);
    }
    
    public function test_processAddsSyncTransportToEmptyTransports(): void
    {
        // Arrange
        $this->container->setParameter('messenger.transports', []);
        
        // Act
        $this->pass->process($this->container);
        
        // Assert
        $updatedTransports = $this->container->getParameter('messenger.transports');
        $this->assertCount(1, $updatedTransports);
        $this->assertArrayHasKey('sync', $updatedTransports);
        $this->assertEquals(['dsn' => 'sync://'], $updatedTransports['sync']);
    }
}