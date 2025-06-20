<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\FailoverTransport;
use Tourze\AsyncMessengerBundle\Failover\FailoverTransportFactory;

class FailoverTransportFactoryTest extends TestCase
{
    public function testSupports(): void
    {
        $factory = new FailoverTransportFactory([]);
        
        $this->assertTrue($factory->supports('failover://async_doctrine,async_redis', []));
        $this->assertFalse($factory->supports('redis://localhost', []));
    }
    
    public function testCreateTransportWithTransportNames(): void
    {
        $doctrineTransport = $this->createMock(TransportInterface::class);
        $redisTransport = $this->createMock(TransportInterface::class);
        
        $doctrineFactory = $this->createMock(TransportFactoryInterface::class);
        $doctrineFactory->method('supports')->willReturnCallback(
            fn($dsn) => str_starts_with($dsn, 'async-doctrine://')
        );
        $doctrineFactory->method('createTransport')->willReturn($doctrineTransport);
        
        $redisFactory = $this->createMock(TransportFactoryInterface::class);
        $redisFactory->method('supports')->willReturnCallback(
            fn($dsn) => str_starts_with($dsn, 'async-redis://')
        );
        $redisFactory->method('createTransport')->willReturn($redisTransport);
        
        $factory = new FailoverTransportFactory([$doctrineFactory, $redisFactory]);
        
        $transport = $factory->createTransport(
            'failover://async_doctrine,async_redis',
            [],
            new PhpSerializer()
        );
        
        $this->assertInstanceOf(FailoverTransport::class, $transport);
    }
    
    public function testCreateTransportRequiresAtLeastTwoTransports(): void
    {
        $factory = new FailoverTransportFactory([]);
        
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Failover transport requires at least 2 transport DSNs');
        
        $factory->createTransport('failover://async_doctrine', [], new PhpSerializer());
    }
}