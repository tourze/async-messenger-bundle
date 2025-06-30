<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategyInterface;
use Tourze\AsyncMessengerBundle\Failover\FailoverReceiver;
use Tourze\AsyncMessengerBundle\Failover\Stamp\FailoverSourceStamp;

final class FailoverReceiverTest extends TestCase
{
    private FailoverReceiver $receiver;
    private array $innerReceivers;
    private ConsumptionStrategyInterface $strategy;
    private CircuitBreakerInterface $circuitBreaker;

    protected function setUp(): void
    {
        $this->innerReceivers = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        
        $this->strategy = $this->createMock(ConsumptionStrategyInterface::class);
        $this->circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        
        $this->receiver = new FailoverReceiver(
            $this->innerReceivers,
            $this->circuitBreaker,
            $this->strategy
        );
    }

    public function testGetSuccessfullyReceivesFromAvailableTransport(): void
    {
        $envelope = new Envelope(new \stdClass());
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturnMap([
                ['transport1', false],
                ['transport2', true],
                ['transport3', true],
            ]);
        
        $this->strategy
            ->method('selectTransport')
            ->with(['transport2', 'transport3'])
            ->willReturn('transport2');
        
        $this->innerReceivers['transport2']
            ->expects(self::once())
            ->method('get')
            ->willReturn([$envelope]);
        
        $this->circuitBreaker
            ->expects(self::once())
            ->method('recordSuccess')
            ->with('transport2');
        
        $result = $this->receiver->get();
        $envelopes = iterator_to_array($result);
        
        self::assertCount(1, $envelopes);
        self::assertSame($envelope, $envelopes[0]);
    }

    public function testGetHandlesFailureAndTriesNextTransport(): void
    {
        $envelope = new Envelope(new \stdClass());
        $exception = new \RuntimeException('Transport failed');
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        $this->strategy
            ->method('selectTransport')
            ->willReturnOnConsecutiveCalls('transport1', 'transport2');
        
        $this->innerReceivers['transport1']
            ->expects(self::once())
            ->method('get')
            ->willThrowException($exception);
        
        $this->innerReceivers['transport2']
            ->expects(self::once())
            ->method('get')
            ->willReturn([$envelope]);
        
        $this->circuitBreaker
            ->expects(self::once())
            ->method('recordFailure')
            ->with('transport1', $exception);
        
        $this->circuitBreaker
            ->expects(self::once())
            ->method('recordSuccess')
            ->with('transport2');
        
        $result = $this->receiver->get();
        $envelopes = iterator_to_array($result);
        
        self::assertCount(1, $envelopes);
        self::assertSame($envelope, $envelopes[0]);
    }

    public function testGetReturnsEmptyWhenNoTransportsAvailable(): void
    {
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(false);
        
        $result = $this->receiver->get();
        $envelopes = iterator_to_array($result);
        
        self::assertEmpty($envelopes);
    }

    public function testAckDelegatesToCorrectTransport(): void
    {
        $envelope = new Envelope(new \stdClass());
        $envelope = $envelope->with(new FailoverSourceStamp('transport1'));
        
        $this->innerReceivers['transport1']
            ->expects(self::once())
            ->method('ack')
            ->with($envelope);
        
        $this->receiver->ack($envelope);
    }

    public function testRejectDelegatesToCorrectTransport(): void
    {
        $envelope = new Envelope(new \stdClass());
        $envelope = $envelope->with(new FailoverSourceStamp('transport2'));
        
        $this->innerReceivers['transport2']
            ->expects(self::once())
            ->method('reject')
            ->with($envelope);
        
        $this->receiver->reject($envelope);
    }
}