<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\ConsumptionStrategy;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\WeightedRoundRobinStrategy;

/**
 * @internal
 */
#[CoversClass(WeightedRoundRobinStrategy::class)]
final class WeightedRoundRobinStrategyTest extends TestCase
{
    private WeightedRoundRobinStrategy $strategy;

    private CircuitBreakerInterface $circuitBreaker;

    protected function setUp(): void
    {
        parent::setUp();
        $this->strategy = new WeightedRoundRobinStrategy();
        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        $this->circuitBreaker = $circuitBreaker;
    }

    public function testSelectTransportWithDefaultWeights(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturn(true)
        ;

        $selections = [];
        for ($i = 0; $i < 10; ++$i) {
            $selections[] = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        }

        // With default weights, should alternate evenly
        $count1 = count(array_filter($selections, fn ($t) => 'transport1' === $t));
        $count2 = count(array_filter($selections, fn ($t) => 'transport2' === $t));

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

        // First, record some results to establish different weights
        // Make transport1 perform better than transport2
        for ($i = 0; $i < 10; ++$i) {
            $strategy->recordResult('transport1', true, 0.1); // 100% success rate
            $strategy->recordResult('transport2', 0 === $i % 2, 0.5); // 50% success rate
        }

        $selections = [];
        for ($i = 0; $i < 100; ++$i) {
            $selections[] = $strategy->selectTransport($transports, $circuitBreaker);
        }

        // transport1 should be selected more often due to better performance
        $count1 = count(array_filter($selections, fn ($t) => 'transport1' === $t));
        $count2 = count(array_filter($selections, fn ($t) => 'transport2' === $t));

        self::assertGreaterThan($count2, $count1);
        self::assertGreaterThan(0, $count1);
        self::assertGreaterThan(0, $count2);
    }

    public function testSelectTransportReturnsNullWhenNoTransportsAvailable(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
        ];

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturn(false)
        ;

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
        for ($i = 0; $i < 30; ++$i) {
            $selections[] = $strategy->selectTransport($transports, $circuitBreaker);
        }

        // All transports should be selected at least once
        self::assertContains('transport1', $selections);
        self::assertContains('transport2', $selections);
        self::assertContains('transport3', $selections);
    }

    public function testRecordResultAffectsWeights(): void
    {
        $strategy = new WeightedRoundRobinStrategy();
        $circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        $circuitBreaker->method('isAvailable')->willReturn(true);

        // Record results to establish different weights
        // Make transport2 perform better than transport1
        for ($i = 0; $i < 10; ++$i) {
            $strategy->recordResult('transport1', false, 1.0); // 0% success rate
            $strategy->recordResult('transport2', true, 0.1); // 100% success rate
        }

        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
        ];

        $selections = [];
        for ($i = 0; $i < 100; ++$i) {
            $selections[] = $strategy->selectTransport($transports, $circuitBreaker);
        }

        // transport2 should be selected more often due to better performance
        $count1 = count(array_filter($selections, fn ($t) => 'transport1' === $t));
        $count2 = count(array_filter($selections, fn ($t) => 'transport2' === $t));

        self::assertGreaterThan($count1, $count2);
        self::assertGreaterThan(0, $count1);
        self::assertGreaterThan(0, $count2);
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
            ])
        ;

        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];

        $selections = [];
        for ($i = 0; $i < 20; ++$i) {
            $selections[] = $strategy->selectTransport($transports, $circuitBreaker);
        }

        // transport2 should never be selected
        self::assertNotContains('transport2', $selections);

        // transport1 and transport3 should both be selected
        $count1 = count(array_filter($selections, fn ($t) => 'transport1' === $t));
        $count3 = count(array_filter($selections, fn ($t) => 'transport3' === $t));
        self::assertGreaterThan(0, $count1);
        self::assertGreaterThan(0, $count3);
        self::assertEquals(20, $count1 + $count3);
    }
}
