<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreaker;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\RoundRobinStrategy;
use Tourze\AsyncMessengerBundle\Failover\FailoverTransport;

class FailoverTransportTest extends TestCase
{
    public function testFailoverTransportRequiresAtLeastTwoTransports(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Failover transport requires at least 2 transports');
        
        $transport = $this->createMock(TransportInterface::class);
        
        new FailoverTransport(
            ['primary' => $transport],
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );
    }
    
    public function testSendFailsOverToPrimaryWhenSecondaryFails(): void
    {
        $envelope = new Envelope(new \stdClass());
        $sentEnvelope = new Envelope(new \stdClass());
        
        $primaryTransport = $this->createMock(TransportInterface::class);
        $primaryTransport->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willThrowException(new \RuntimeException('Primary failed'));
            
        $secondaryTransport = $this->createMock(TransportInterface::class);
        $secondaryTransport->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willReturn($sentEnvelope);
        
        $failoverTransport = new FailoverTransport(
            [
                'primary' => $primaryTransport,
                'secondary' => $secondaryTransport
            ],
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );
        
        $result = $failoverTransport->send($envelope);
        
        $this->assertNotSame($envelope, $result);
    }
    
    public function testAllTransportsFailThrowsException(): void
    {
        $envelope = new Envelope(new \stdClass());
        
        $primaryTransport = $this->createMock(TransportInterface::class);
        $primaryTransport->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willThrowException(new \RuntimeException('Primary failed'));
            
        $secondaryTransport = $this->createMock(TransportInterface::class);
        $secondaryTransport->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willThrowException(new \RuntimeException('Secondary failed'));
        
        $failoverTransport = new FailoverTransport(
            [
                'primary' => $primaryTransport,
                'secondary' => $secondaryTransport
            ],
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );
        
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('All transports failed');
        
        $failoverTransport->send($envelope);
    }
}