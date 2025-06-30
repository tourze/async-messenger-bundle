<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\ConsumptionStrategy;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\RoundRobinStrategy;

final class RoundRobinStrategyTest extends TestCase
{
    private RoundRobinStrategy $strategy;
    private CircuitBreakerInterface $circuitBreaker;

    protected function setUp(): void
    {
        $this->strategy = new RoundRobinStrategy();
        $this->circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
    }

    public function testSelectTransportRotatesThroughAvailableTransports(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        $selections = [];
        for ($i = 0; $i < 6; $i++) {
            $selections[] = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        }
        
        // Should rotate through all transports twice
        self::assertEquals('transport1', $selections[0]);
        self::assertEquals('transport2', $selections[1]);
        self::assertEquals('transport3', $selections[2]);
        self::assertEquals('transport1', $selections[3]);
        self::assertEquals('transport2', $selections[4]);
        self::assertEquals('transport3', $selections[5]);
    }

    public function testSelectTransportHandlesDynamicTransportList(): void
    {
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        // Start with 2 transports
        $transports1 = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];
        
        $selections = [];
        $selections[] = $this->strategy->selectTransport($transports1, $this->circuitBreaker);
        $selections[] = $this->strategy->selectTransport($transports1, $this->circuitBreaker);
        
        // Add a third transport
        $transports2 = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        $selections[] = $this->strategy->selectTransport($transports2, $this->circuitBreaker);
        
        // Remove the second transport
        $transports3 = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        $selections[] = $this->strategy->selectTransport($transports3, $this->circuitBreaker);
        
        self::assertEquals('transport1', $selections[0]);
        self::assertEquals('transport2', $selections[1]);
        self::assertEquals('transport3', $selections[2]);
        self::assertEquals('transport1', $selections[3]);
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

    public function testSelectTransportSkipsUnavailableTransports(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturnMap([
                ['transport1', true],
                ['transport2', false],
                ['transport3', true],
            ]);
        
        $selections = [];
        for ($i = 0; $i < 4; $i++) {
            $selections[] = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        }
        
        // Should only rotate between transport1 and transport3
        self::assertEquals('transport1', $selections[0]);
        self::assertEquals('transport3', $selections[1]);
        self::assertEquals('transport1', $selections[2]);
        self::assertEquals('transport3', $selections[3]);
    }

    public function testRecordResultDoesNotAffectSelection(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        // Record result should not affect round-robin behavior
        $this->strategy->recordResult('transport1', false, 1.0);
        $this->strategy->recordResult('transport2', true, 0.1);
        
        $selections = [];
        for ($i = 0; $i < 4; $i++) {
            $selections[] = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        }
        
        // Should still rotate evenly
        self::assertEquals('transport1', $selections[0]);
        self::assertEquals('transport2', $selections[1]);
        self::assertEquals('transport1', $selections[2]);
        self::assertEquals('transport2', $selections[3]);
    }
}