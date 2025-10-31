<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\ConsumptionStrategy;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\RoundRobinStrategy;

/**
 * @internal
 */
#[CoversClass(RoundRobinStrategy::class)]
final class RoundRobinStrategyTest extends TestCase
{
    private RoundRobinStrategy $strategy;

    private CircuitBreakerInterface $circuitBreaker;

    protected function setUp(): void
    {
        parent::setUp();
        $this->strategy = new RoundRobinStrategy();
        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        $this->circuitBreaker = $circuitBreaker;
    }

    public function testSelectTransportRotatesThroughAvailableTransports(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturn(true)
        ;

        $selections = [];
        for ($i = 0; $i < 6; ++$i) {
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
        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturn(true)
        ;

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

        // Test that it selects from the first two transports in order
        self::assertEquals('transport1', $selections[0]);
        self::assertEquals('transport2', $selections[1]);

        // For the third selection, it should pick from the available transports
        self::assertContains($selections[2], ['transport1', 'transport2', 'transport3']);

        // For the fourth selection, it should pick from the available transports
        self::assertContains($selections[3], ['transport1', 'transport3']);
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

    public function testSelectTransportSkipsUnavailableTransports(): void
    {
        $transports = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturnMap([
                ['transport1', true],
                ['transport2', false],
                ['transport3', true],
            ])
        ;

        $selections = [];
        for ($i = 0; $i < 4; ++$i) {
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

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturn(true)
        ;

        // Record result should not affect round-robin behavior
        $this->strategy->recordResult('transport1', false, 1.0);
        $this->strategy->recordResult('transport2', true, 0.1);

        $selections = [];
        for ($i = 0; $i < 4; ++$i) {
            $selections[] = $this->strategy->selectTransport($transports, $this->circuitBreaker);
        }

        // Should still rotate evenly
        self::assertEquals('transport1', $selections[0]);
        self::assertEquals('transport2', $selections[1]);
        self::assertEquals('transport1', $selections[2]);
        self::assertEquals('transport2', $selections[3]);
    }
}
