<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\Integration;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreaker;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\AdaptivePriorityStrategy;
use Tourze\AsyncMessengerBundle\Failover\FailoverTransport;

class FailoverTransportIntegrationTest extends TestCase
{
    private TransportInterface $primaryTransport;
    private TransportInterface $secondaryTransport;
    private FailoverTransport $failoverTransport;
    
    public function testFailoverOnPrimarySendFailure(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message);

        // Primary fails
        $this->primaryTransport
            ->expects($this->once())
            ->method('send')
            ->willThrowException(new \RuntimeException('Primary connection failed'));

        // Secondary succeeds
        $this->secondaryTransport
            ->expects($this->once())
            ->method('send')
            ->willReturn($envelope);

        // Should not throw exception
        $result = $this->failoverTransport->send($envelope);

        $this->assertInstanceOf(Envelope::class, $result);
    }
    
    public function testCircuitBreakerOpensAfterMultipleFailures(): void
    {
        $message = new \stdClass();

        // First two calls to primary fail (to open circuit breaker)
        $callCount = 0;
        $this->primaryTransport
            ->method('send')
            ->willReturnCallback(function () use (&$callCount) {
                $callCount++;
                if ($callCount <= 2) {
                    throw new \Tourze\AsyncMessengerBundle\Tests\Exception\TestTransportException('Primary failed');
                }
                return new Envelope(new \stdClass());
            });

        // Configure secondary to succeed
        $this->secondaryTransport
            ->method('send')
            ->willReturnCallback(fn($e) => $e);

        // First message - primary fails, fallback to secondary
        $this->failoverTransport->send(new Envelope($message));

        // Second message - primary fails again, fallback to secondary
        $this->failoverTransport->send(new Envelope($message));

        // At this point, circuit breaker should be open for primary
        // Third message should go directly to secondary without trying primary
        $this->primaryTransport
            ->expects($this->never())
            ->method('send');

        $this->secondaryTransport
            ->expects($this->once())
            ->method('send')
            ->willReturnCallback(fn($e) => $e);

        $this->failoverTransport->send(new Envelope($message));
    }
    
    
    protected function setUp(): void
    {
        $this->primaryTransport = $this->createMock(TransportInterface::class);
        $this->secondaryTransport = $this->createMock(TransportInterface::class);

        $this->failoverTransport = new FailoverTransport(
            [
                'primary' => $this->primaryTransport,
                'secondary' => $this->secondaryTransport,
            ],
            new CircuitBreaker(
                failureThreshold: 2,
                successThreshold: 1,
                timeout: 1
            ),
            new AdaptivePriorityStrategy()
        );
    }
}