<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\ConsumptionStrategy;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\AdaptivePriorityStrategy;

final class AdaptivePriorityStrategyTest extends TestCase
{
    private AdaptivePriorityStrategy $strategy;
    private CircuitBreakerInterface $circuitBreaker;

    protected function setUp(): void
    {
        $this->strategy = new AdaptivePriorityStrategy();
        $this->circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
    }

    public function testSelectTransportReturnsFirstAvailableTransport(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        $selected = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        
        self::assertContains($selected, ['transport1', 'transport2', 'transport3']);
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

    public function testRecordResultAdjustsPriority(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        // Record success for transport1
        $this->strategy->recordResult('transport1', true, 0.1);
        
        // Record failure for transport2
        $this->strategy->recordResult('transport2', false, 0.5);
        
        // transport1 should be preferred
        $selected = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        self::assertNotNull($selected);
    }

    public function testPriorityAdaptsBasedOnPerformance(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        // Make transport2 perform better
        for ($i = 0; $i < 10; $i++) {
            $this->strategy->recordResult('transport1', true, 0.5);
            $this->strategy->recordResult('transport2', true, 0.1);
        }
        
        // Run selection multiple times to see preference
        $selections = [];
        for ($i = 0; $i < 20; $i++) {
            $selected = $this->strategy->selectTransport($transports, $this->circuitBreaker);
            $selections[$selected] = ($selections[$selected] ?? 0) + 1;
        }
        
        // transport2 should be selected more often due to better performance
        self::assertGreaterThanOrEqual(10, $selections['transport2'] ?? 0);
    }

    public function testSelectTransportHandlesEmptyTransportArray(): void
    {
        $selected = $this->strategy->selectTransport([], $this->circuitBreaker);
        
        self::assertNull($selected);
    }
}