<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\FailoverSender;

final class FailoverSenderTest extends TestCase
{
    private FailoverSender $sender;
    private array $innerSenders;
    private CircuitBreakerInterface $circuitBreaker;

    protected function setUp(): void
    {
        $this->innerSenders = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];
        
        $this->circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        
        $this->sender = new FailoverSender(
            $this->innerSenders,
            $this->circuitBreaker
        );
    }

    public function testSendSuccessfullyToFirstAvailableTransport(): void
    {
        $envelope = new Envelope(new \stdClass());
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->with('transport1')
            ->willReturn(true);
        
        $this->innerSenders['transport1']
            ->expects(self::once())
            ->method('send')
            ->with($envelope)
            ->willReturn($envelope);
        
        $this->circuitBreaker
            ->expects(self::once())
            ->method('recordSuccess')
            ->with('transport1');
        
        $result = $this->sender->send($envelope);
        
        self::assertSame($envelope, $result);
    }

    public function testSendFailsOverToNextTransportOnFailure(): void
    {
        $envelope = new Envelope(new \stdClass());
        $exception = new \RuntimeException('Transport failed');
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturnMap([
                ['transport1', true],
                ['transport2', true],
            ]);
        
        $this->innerSenders['transport1']
            ->expects(self::once())
            ->method('send')
            ->with($envelope)
            ->willThrowException($exception);
        
        $this->innerSenders['transport2']
            ->expects(self::once())
            ->method('send')
            ->with($envelope)
            ->willReturn($envelope);
        
        $this->circuitBreaker
            ->expects(self::once())
            ->method('recordFailure')
            ->with('transport1', $exception);
        
        $this->circuitBreaker
            ->expects(self::once())
            ->method('recordSuccess')
            ->with('transport2');
        
        $result = $this->sender->send($envelope);
        
        self::assertSame($envelope, $result);
    }

    public function testSendSkipsUnavailableTransports(): void
    {
        $envelope = new Envelope(new \stdClass());
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturnMap([
                ['transport1', false],
                ['transport2', false],
                ['transport3', true],
            ]);
        
        $this->innerSenders['transport1']
            ->expects(self::never())
            ->method('send');
        
        $this->innerSenders['transport2']
            ->expects(self::never())
            ->method('send');
        
        $this->innerSenders['transport3']
            ->expects(self::once())
            ->method('send')
            ->with($envelope)
            ->willReturn($envelope);
        
        $result = $this->sender->send($envelope);
        
        self::assertSame($envelope, $result);
    }

    public function testSendThrowsExceptionWhenAllTransportsFail(): void
    {
        $envelope = new Envelope(new \stdClass());
        $exception1 = new \RuntimeException('Transport 1 failed');
        $exception2 = new \RuntimeException('Transport 2 failed');
        $exception3 = new \RuntimeException('Transport 3 failed');
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(true);
        
        $this->innerSenders['transport1']
            ->expects(self::once())
            ->method('send')
            ->willThrowException($exception1);
        
        $this->innerSenders['transport2']
            ->expects(self::once())
            ->method('send')
            ->willThrowException($exception2);
        
        $this->innerSenders['transport3']
            ->expects(self::once())
            ->method('send')
            ->willThrowException($exception3);
        
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('All transports failed');
        
        $this->sender->send($envelope);
    }

    public function testSendThrowsExceptionWhenNoTransportsAvailable(): void
    {
        $envelope = new Envelope(new \stdClass());
        
        $this->circuitBreaker
            ->method('isAvailable')
            ->willReturn(false);
        
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('No available transports');
        
        $this->sender->send($envelope);
    }
}