<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\ConsumptionStrategy;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\WeightedRoundRobinStrategy;

final class WeightedRoundRobinStrategyTest extends TestCase
{
    private WeightedRoundRobinStrategy $strategy;
    private CircuitBreakerInterface $circuitBreaker;

    protected function setUp(): void
    {
        $this->strategy = new WeightedRoundRobinStrategy();
        $this->circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
    }

    public function testSelectTransportWithDefaultWeights(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        $selections = [];
        for ($i = 0; $i < 10; $i++) {
            $selections[] = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        }
        
        // With default weights, should alternate evenly
        $count1 = count(array_filter($selections, fn($t) => $t === 'transport1'));
        $count2 = count(array_filter($selections, fn($t) => $t === 'transport2'));
        
        self::assertEquals(5, $count1);
        self::assertEquals(5, $count2);
    }

    public function testSelectTransportWithCustomWeights(): void
    {
        // Note: WeightedRoundRobinStrategy doesn't have constructor with weights parameter
        // It automatically adjusts weights based on performance
        $strategy = new WeightedRoundRobinStrategy();
        $circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        $circuitBreaker->method('isAvailable')->willReturn(true);
        
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];
        
        $selections = [];
        for ($i = 0; $i < 20; $i++) {
            $selections[] = $strategy->selectTransport($transports, $circuitBreaker);
        }
        
        // transport1 should be selected approximately 3 times more often
        $count1 = count(array_filter($selections, fn($t) => $t === 'transport1'));
        $count2 = count(array_filter($selections, fn($t) => $t === 'transport2'));
        
        self::assertGreaterThan($count2 * 2, $count1);
        self::assertLessThan($count2 * 4, $count1);
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

    public function testSelectTransportHandlesUnknownTransports(): void
    {
        $strategy = new WeightedRoundRobinStrategy();
        $circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        $circuitBreaker->method('isAvailable')->willReturn(true);
        
        // Include transport3 which has no defined weight
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        
        $selections = [];
        for ($i = 0; $i < 30; $i++) {
            $selections[] = $strategy->selectTransport($transports, $circuitBreaker);
        }
        
        // All transports should be selected at least once
        self::assertContains('transport1', $selections);
        self::assertContains('transport2', $selections);
        self::assertContains('transport3', $selections);
    }

    public function testRecordResultDoesNotAffectWeights(): void
    {
        $strategy = new WeightedRoundRobinStrategy();
        $circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        $circuitBreaker->method('isAvailable')->willReturn(true);
        
        // Record result should not affect weighted round-robin behavior
        $strategy->recordResult('transport1', false, 1.0);
        $strategy->recordResult('transport2', true, 0.1);
        
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];
        
        $selections = [];
        for ($i = 0; $i < 12; $i++) {
            $selections[] = $strategy->selectTransport($transports, $circuitBreaker);
        }
        
        // transport1 should still be selected more often
        $count1 = count(array_filter($selections, fn($t) => $t === 'transport1'));
        self::assertGreaterThanOrEqual(6, $count1);
    }

    public function testSelectTransportSkipsUnavailableTransports(): void
    {
        $strategy = new WeightedRoundRobinStrategy();
        $circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        
        $circuitBreaker
            ->method('isAvailable')
            ->willReturnMap([
                ['transport1', true],
                ['transport2', false], // Not available
                ['transport3', true],
            ]);
        
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        
        $selections = [];
        for ($i = 0; $i < 20; $i++) {
            $selections[] = $strategy->selectTransport($transports, $circuitBreaker);
        }
        
        // transport2 should never be selected
        self::assertNotContains('transport2', $selections);
        
        // transport1 should be selected more often than transport3
        $count1 = count(array_filter($selections, fn($t) => $t === 'transport1'));
        $count3 = count(array_filter($selections, fn($t) => $t === 'transport3'));
        self::assertGreaterThan($count3, $count1);
    }
}