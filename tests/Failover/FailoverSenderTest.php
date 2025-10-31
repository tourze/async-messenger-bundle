<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerInterface;
use Tourze\AsyncMessengerBundle\Failover\FailoverSender;
use Tourze\AsyncMessengerBundle\Failover\Stamp;

/**
 * @internal
 */
#[CoversClass(FailoverSender::class)]
final class FailoverSenderTest extends TestCase
{
    private FailoverSender $sender;

    /** @var array<string, TransportInterface> */
    private array $innerSenders;

    private CircuitBreakerInterface $circuitBreaker;

    protected function setUp(): void
    {
        parent::setUp();
        $this->innerSenders = [
            'transport1' => $this->createMock(TransportInterface::class),
            'transport2' => $this->createMock(TransportInterface::class),
            'transport3' => $this->createMock(TransportInterface::class),
        ];

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->createMock(CircuitBreakerInterface::class);
        $this->circuitBreaker = $circuitBreaker;

        $this->sender = new FailoverSender(
            $this->innerSenders,
            $this->circuitBreaker
        );
    }

    public function testSendSuccessfullyToFirstAvailableTransport(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->with('transport1')
            ->willReturn(true)
        ;

        /** @var TransportInterface&MockObject $transport1 */
        $transport1 = $this->innerSenders['transport1'];
        $transport1
            ->expects(self::once())
            ->method('send')
            ->with($envelope)
            ->willReturn($envelope)
        ;

        $circuitBreaker
            ->expects(self::once())
            ->method('recordSuccess')
            ->with('transport1')
        ;

        $result = $this->sender->send($envelope);

        // Should return a new envelope with the original message and FailoverSourceStamp
        self::assertSame($envelope->getMessage(), $result->getMessage());
        self::assertCount(1, $result->all(Stamp\FailoverSourceStamp::class));

        $failoverStamp = $result->all(Stamp\FailoverSourceStamp::class)[0];
        self::assertEquals('transport1', $failoverStamp->getTransportName());
    }

    public function testSendFailsOverToNextTransportOnFailure(): void
    {
        $envelope = new Envelope(new \stdClass(), []);
        $exception = new \RuntimeException('Transport failed');

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturnMap([
                ['transport1', true],
                ['transport2', true],
            ])
        ;

        /** @var TransportInterface&MockObject $transport1 */
        $transport1 = $this->innerSenders['transport1'];
        $transport1
            ->expects(self::once())
            ->method('send')
            ->with($envelope)
            ->willThrowException($exception)
        ;

        /** @var TransportInterface&MockObject $transport2 */
        $transport2 = $this->innerSenders['transport2'];
        $transport2
            ->expects(self::once())
            ->method('send')
            ->with($envelope)
            ->willReturn($envelope)
        ;

        $circuitBreaker
            ->expects(self::once())
            ->method('recordFailure')
            ->with('transport1', $exception)
        ;

        $circuitBreaker
            ->expects(self::once())
            ->method('recordSuccess')
            ->with('transport2')
        ;

        $result = $this->sender->send($envelope);

        // Should return a new envelope with FailoverSourceStamp from transport2
        self::assertSame($envelope->getMessage(), $result->getMessage());
        self::assertCount(1, $result->all(Stamp\FailoverSourceStamp::class));

        $failoverStamp = $result->all(Stamp\FailoverSourceStamp::class)[0];
        self::assertEquals('transport2', $failoverStamp->getTransportName());
    }

    public function testSendSkipsUnavailableTransports(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturnMap([
                ['transport1', false],
                ['transport2', false],
                ['transport3', true],
            ])
        ;

        /** @var TransportInterface&MockObject $transport1 */
        $transport1 = $this->innerSenders['transport1'];
        $transport1
            ->expects(self::never())
            ->method('send')
        ;

        /** @var TransportInterface&MockObject $transport2 */
        $transport2 = $this->innerSenders['transport2'];
        $transport2
            ->expects(self::never())
            ->method('send')
        ;

        /** @var TransportInterface&MockObject $transport3 */
        $transport3 = $this->innerSenders['transport3'];
        $transport3
            ->expects(self::once())
            ->method('send')
            ->with($envelope)
            ->willReturn($envelope)
        ;

        $result = $this->sender->send($envelope);

        // Should return a new envelope with FailoverSourceStamp from transport3
        self::assertSame($envelope->getMessage(), $result->getMessage());
        self::assertCount(1, $result->all(Stamp\FailoverSourceStamp::class));

        $failoverStamp = $result->all(Stamp\FailoverSourceStamp::class)[0];
        self::assertEquals('transport3', $failoverStamp->getTransportName());
    }

    public function testSendThrowsExceptionWhenAllTransportsFail(): void
    {
        $envelope = new Envelope(new \stdClass(), []);
        $exception1 = new \RuntimeException('Transport 1 failed');
        $exception2 = new \RuntimeException('Transport 2 failed');
        $exception3 = new \RuntimeException('Transport 3 failed');

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturn(true)
        ;

        /** @var TransportInterface&MockObject $transport1 */
        $transport1 = $this->innerSenders['transport1'];
        $transport1
            ->expects(self::once())
            ->method('send')
            ->willThrowException($exception1)
        ;

        /** @var TransportInterface&MockObject $transport2 */
        $transport2 = $this->innerSenders['transport2'];
        $transport2
            ->expects(self::once())
            ->method('send')
            ->willThrowException($exception2)
        ;

        /** @var TransportInterface&MockObject $transport3 */
        $transport3 = $this->innerSenders['transport3'];
        $transport3
            ->expects(self::once())
            ->method('send')
            ->willThrowException($exception3)
        ;

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('All transports failed');

        $this->sender->send($envelope);
    }

    public function testSendThrowsExceptionWhenNoTransportsAvailable(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        /** @var CircuitBreakerInterface&MockObject $circuitBreaker */
        $circuitBreaker = $this->circuitBreaker;
        $circuitBreaker
            ->method('isAvailable')
            ->willReturn(false)
        ;

        // When all transports are unhealthy, it will still try them (try_unhealthy_on_failure=true by default)
        // and all will fail, leading to "All transports failed" message
        /** @var TransportInterface&MockObject $transport1 */
        $transport1 = $this->innerSenders['transport1'];
        $transport1
            ->expects(self::once())
            ->method('send')
            ->willThrowException(new \RuntimeException('Transport 1 failed'))
        ;

        /** @var TransportInterface&MockObject $transport2 */
        $transport2 = $this->innerSenders['transport2'];
        $transport2
            ->expects(self::once())
            ->method('send')
            ->willThrowException(new \RuntimeException('Transport 2 failed'))
        ;

        /** @var TransportInterface&MockObject $transport3 */
        $transport3 = $this->innerSenders['transport3'];
        $transport3
            ->expects(self::once())
            ->method('send')
            ->willThrowException(new \RuntimeException('Transport 3 failed'))
        ;

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('All transports failed');

        $this->sender->send($envelope);
    }
}
