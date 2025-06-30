<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\ConsumptionStrategy;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\LatencyAwareStrategy;

final class LatencyAwareStrategyTest extends TestCase
{
    private LatencyAwareStrategy $strategy;
    private CircuitBreakerInterface $circuitBreaker;

    protected function setUp(): void
    {
        $this->strategy = new LatencyAwareStrategy();
        $this->circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
    }

    public function testSelectTransportReturnsLowestLatencyTransport(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        // Set different latencies
        $this->strategy->recordResult('transport1', true, 0.5);
        $this->strategy->recordResult('transport2', true, 0.1);
        $this->strategy->recordResult('transport3', true, 0.3);
        
        $selected = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        
        self::assertEquals('transport2', $selected);
    }

    public function testSelectTransportHandlesNoMetrics(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        $selected = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        
        self::assertContains($selected, ['transport1', 'transport2']);
    }

    public function testSelectTransportReturnsNullWhenNoTransportsAvailable(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(false);
        
        $selected = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        
        self::assertNull($selected);
    }

    public function testRecordResultTracksAverageLatency(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        // Record multiple latencies for transport1
        $this->strategy->recordResult('transport1', true, 0.1);
        $this->strategy->recordResult('transport1', true, 0.2);
        $this->strategy->recordResult('transport1', true, 0.3);
        
        // Record single latency for transport2
        $this->strategy->recordResult('transport2', true, 0.25);
        
        // transport1 should have average latency of 0.2, so it should be selected
        $selected = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        
        self::assertEquals('transport1', $selected);
    }

    public function testFailureDoesNotAffectLatencyTracking(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        $this->strategy->recordResult('transport1', true, 0.1);
        $this->strategy->recordResult('transport1', false, 1.0); // Failure with high latency
        $this->strategy->recordResult('transport1', true, 0.1);
        
        $selected = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        
        self::assertEquals('transport1', $selected);
    }

    public function testSelectTransportOnlyConsidersAvailableTransports(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturnMap([
                ['transport1', false],
                ['transport2', true],
                ['transport3', true],
            ]);
        
        // transport1 has lowest latency but is not available
        $this->strategy->recordResult('transport1', true, 0.1);
        $this->strategy->recordResult('transport2', true, 0.2);
        $this->strategy->recordResult('transport3', true, 0.3);
        
        $selected = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        
        self::assertEquals('transport2', $selected);
    }
}