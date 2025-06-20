<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Transport;

use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Transport\FallbackTransport;
use Tourze\AsyncMessengerBundle\Transport\FallbackTransportFactory;

class FallbackTransportFactoryTest extends TestCase
{
    private ContainerInterface $container;
    private SerializerInterface $serializer;
    private FallbackTransportFactory $factory;
    
    protected function setUp(): void
    {
        $this->container = $this->createMock(ContainerInterface::class);
        $this->serializer = $this->createMock(SerializerInterface::class);
        $this->factory = new FallbackTransportFactory($this->container);
    }
    
    public function test_supportsCorrectDsn(): void
    {
        $this->assertTrue($this->factory->supports('fallback://', []));
        $this->assertTrue($this->factory->supports('fallback://host', []));
        $this->assertTrue($this->factory->supports('fallback://host:port', []));
    }
    
    public function test_doesNotSupportIncorrectDsn(): void
    {
        $this->assertFalse($this->factory->supports('redis://', []));
        $this->assertFalse($this->factory->supports('doctrine://', []));
        $this->assertFalse($this->factory->supports('sync://', []));
        $this->assertFalse($this->factory->supports('', []));
    }
    
    public function test_createTransportWithDefaultTransports(): void
    {
        // Arrange
        $redisTransport = $this->createMock(TransportInterface::class);
        $doctrineTransport = $this->createMock(TransportInterface::class);
        $syncTransport = $this->createMock(TransportInterface::class);
        
        $this->container->expects($this->exactly(3))
            ->method('has')
            ->willReturnMap([
                ['messenger.transport.async_redis', true],
                ['messenger.transport.async_doctrine', true],
                ['messenger.transport.sync', true],
            ]);
        
        $this->container->expects($this->exactly(3))
            ->method('get')
            ->willReturnMap([
                ['messenger.transport.async_redis', $redisTransport],
                ['messenger.transport.async_doctrine', $doctrineTransport],
                ['messenger.transport.sync', $syncTransport],
            ]);
        
        // Act
        $transport = $this->factory->createTransport('fallback://', [], $this->serializer);
        
        // Assert
        $this->assertInstanceOf(FallbackTransport::class, $transport);
    }
    
    public function test_createTransportWithCustomTransports(): void
    {
        // Arrange
        $transport1 = $this->createMock(TransportInterface::class);
        $transport2 = $this->createMock(TransportInterface::class);
        
        $options = [
            'transports' => ['custom1', 'custom2'],
        ];
        
        $this->container->expects($this->exactly(2))
            ->method('has')
            ->willReturnMap([
                ['messenger.transport.custom1', true],
                ['messenger.transport.custom2', true],
            ]);
        
        $this->container->expects($this->exactly(2))
            ->method('get')
            ->willReturnMap([
                ['messenger.transport.custom1', $transport1],
                ['messenger.transport.custom2', $transport2],
            ]);
        
        // Act
        $transport = $this->factory->createTransport('fallback://host', $options, $this->serializer);
        
        // Assert
        $this->assertInstanceOf(FallbackTransport::class, $transport);
    }
    
    public function test_createTransportThrowsExceptionForInvalidDsn(): void
    {
        // Arrange & Act & Assert
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid DSN for fallback transport');
        
        $this->factory->createTransport('fallback:', [], $this->serializer);
    }
    
    public function test_createTransportThrowsExceptionForMissingTransport(): void
    {
        // Arrange
        $this->container->expects($this->once())
            ->method('has')
            ->with('messenger.transport.async_redis')
            ->willReturn(false);
        
        // Act & Assert
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Transport "async_redis" not found in container');
        
        $this->factory->createTransport('fallback://host', [], $this->serializer);
    }
}