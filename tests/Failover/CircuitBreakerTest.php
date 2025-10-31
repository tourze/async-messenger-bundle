<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreaker;

/**
 * @internal
 */
#[CoversClass(CircuitBreaker::class)]
final class CircuitBreakerTest extends TestCase
{
    private CircuitBreaker $circuitBreaker;

    protected function setUp(): void
    {
        parent::setUp();
        $this->circuitBreaker = new CircuitBreaker();
    }

    public function testInitialStateIsClosed(): void
    {
        self::assertTrue($this->circuitBreaker->isAvailable('transport1'));
    }

    public function testRecordSuccess(): void
    {
        $this->circuitBreaker->recordSuccess('transport1');
        self::assertTrue($this->circuitBreaker->isAvailable('transport1'));
    }

    public function testRecordFailureOpensCircuitAfterThreshold(): void
    {
        $exception = new \RuntimeException('Test exception');

        // Record failures up to threshold
        for ($i = 0; $i < 5; ++$i) {
            $this->circuitBreaker->recordFailure('transport1', $exception);
        }

        self::assertFalse($this->circuitBreaker->isAvailable('transport1'));
    }

    public function testCircuitBreakerResetsAfterTimeout(): void
    {
        $exception = new \RuntimeException('Test exception');

        // Open the circuit
        for ($i = 0; $i < 5; ++$i) {
            $this->circuitBreaker->recordFailure('transport1', $exception);
        }

        self::assertFalse($this->circuitBreaker->isAvailable('transport1'));

        // Simulate passage of time
        $reflection = new \ReflectionClass($this->circuitBreaker);
        if ($reflection->hasProperty('stateChangedTimes')) {
            $property = $reflection->getProperty('stateChangedTimes');
            $property->setAccessible(true);
            $property->setValue($this->circuitBreaker, ['transport1' => time() - 31]);
        }

        // Should be available again after timeout (half-open state)
        self::assertTrue($this->circuitBreaker->isAvailable('transport1'));
    }

    public function testMultipleTransportsAreTrackedIndependently(): void
    {
        $exception = new \RuntimeException('Test exception');

        // Open circuit for transport1
        for ($i = 0; $i < 5; ++$i) {
            $this->circuitBreaker->recordFailure('transport1', $exception);
        }

        // transport2 should still be available
        self::assertFalse($this->circuitBreaker->isAvailable('transport1'));
        self::assertTrue($this->circuitBreaker->isAvailable('transport2'));
    }
}
