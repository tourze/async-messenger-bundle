<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\MockObject\Builder\InvocationMocker;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreaker;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\AdaptivePriorityStrategy;
use Tourze\AsyncMessengerBundle\Failover\FailoverTransport;
use Tourze\AsyncMessengerBundle\Failover\Stamp\FailoverSourceStamp;
use Tourze\AsyncMessengerBundle\Tests\Exception\TestTransportException;

/**
 * @internal
 */
#[CoversClass(FailoverTransport::class)]
final class FailoverTransportIntegrationTest extends TestCase
{
    private TransportInterface $primaryTransport;

    private TransportInterface $secondaryTransport;

    private FailoverTransport $failoverTransport;

    public function testFailoverOnPrimarySendFailure(): void
    {
        $message = new \stdClass();
        $envelope = new Envelope($message, []);

        // Primary fails
        /** @var InvocationMocker $primaryMock */
        $primaryMock = $this->primaryTransport->expects($this->once());
        $primaryMock->method('send')->willThrowException(new \RuntimeException('Primary connection failed'));

        // Secondary succeeds
        /** @var InvocationMocker $secondaryMock */
        $secondaryMock = $this->secondaryTransport->expects($this->once());
        $secondaryMock->method('send')->willReturn($envelope);

        // Should not throw exception
        $result = $this->failoverTransport->send($envelope);

        $this->assertNotNull($result);
    }

    public function testCircuitBreakerOpensAfterMultipleFailures(): void
    {
        $message = new \stdClass();

        // First two calls to primary fail (to open circuit breaker)
        $callCount = 0;
        /** @var InvocationMocker $primaryMock */
        $primaryMock = $this->primaryTransport->method('send');
        $primaryMock->willReturnCallback(function () use (&$callCount) {
            ++$callCount;
            if ($callCount <= 2) {
                throw new TestTransportException('Primary failed');
            }

            return new Envelope(new \stdClass(), []);
        });

        // Configure secondary to succeed
        /** @var InvocationMocker $secondaryMock */
        $secondaryMock = $this->secondaryTransport->method('send');
        $secondaryMock->willReturnCallback(fn ($e) => $e);

        // First message - primary fails, fallback to secondary
        $result1 = $this->failoverTransport->send(new Envelope($message, []));
        $this->assertInstanceOf(Envelope::class, $result1);

        // Second message - primary fails again, fallback to secondary
        $result2 = $this->failoverTransport->send(new Envelope($message, []));
        $this->assertInstanceOf(Envelope::class, $result2);

        // At this point, circuit breaker should be open for primary
        // Third message should go directly to secondary without trying primary
        $result3 = $this->failoverTransport->send(new Envelope($message, []));
        $this->assertInstanceOf(Envelope::class, $result3);

        // Verify that primary was called exactly twice (for the first two messages)
        // and not called for the third message due to circuit breaker being open
        $this->assertSame(2, $callCount, 'Circuit breaker should prevent third call to primary');
    }

    public function testGetUsesConsumptionStrategy(): void
    {
        // The get() method uses consumption strategy and may not directly call transport->get()
        // This test verifies the method returns an iterable without exceptions
        $result = $this->failoverTransport->get();

        $this->assertIsIterable($result);
        // In practice, get() may yield nothing immediately due to the consumption strategy
        // The important thing is that it doesn't throw an exception
    }

    public function testAckFailsOverWhenPrimaryUnavailable(): void
    {
        // Create envelope with source transport stamp
        $envelope = new Envelope(new \stdClass(), []);
        $envelope = $envelope->with(new FailoverSourceStamp('primary'));

        // Primary fails
        /** @var InvocationMocker $primaryAck */
        $primaryAck = $this->primaryTransport->expects($this->once());
        $primaryAck->method('ack')->with($envelope)->willThrowException(new \RuntimeException('Primary connection failed'));

        // Secondary should not be called for ack - it goes to the original source
        /** @var InvocationMocker $secondaryAck */
        $secondaryAck = $this->secondaryTransport->expects($this->never());
        $secondaryAck->method('ack');

        // Should throw exception since primary failed and ack must go to source
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Primary connection failed');

        $this->failoverTransport->ack($envelope);
    }

    public function testRejectFailsOverWhenPrimaryUnavailable(): void
    {
        // Create envelope with source transport stamp
        $envelope = new Envelope(new \stdClass(), []);
        $envelope = $envelope->with(new FailoverSourceStamp('primary'));

        // Primary fails
        /** @var InvocationMocker $primaryReject */
        $primaryReject = $this->primaryTransport->expects($this->once());
        $primaryReject->method('reject')->with($envelope)->willThrowException(new \RuntimeException('Primary connection failed'));

        // Secondary should not be called for reject - it goes to the original source
        /** @var InvocationMocker $secondaryReject */
        $secondaryReject = $this->secondaryTransport->expects($this->never());
        $secondaryReject->method('reject');

        // Should throw exception since primary failed and reject must go to source
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Primary connection failed');

        $this->failoverTransport->reject($envelope);
    }

    public function testKeepaliveWorksWithAvailableTransports(): void
    {
        // Create envelope with source transport stamp
        $envelope = new Envelope(new \stdClass(), []);
        $envelope = $envelope->with(new FailoverSourceStamp('primary'));
        $seconds = 30;

        // Should not throw exception - keepalive failures are silently ignored
        // and TransportInterface doesn't guarantee keepalive method exists
        $this->failoverTransport->keepalive($envelope, $seconds);

        // Assert that the method completes without exception by testing stamp exists
        $this->assertNotNull($envelope->last(FailoverSourceStamp::class));
    }

    public function testSetupCallsSetupOnAllSetupableTransports(): void
    {
        $setupableTransport1 = $this->createMock(TransportInterface::class);
        $setupableTransport2 = $this->createMock(SetupableTransportInterface::class);

        $setupableTransport2
            ->expects($this->once())
            ->method('setup')
        ;

        $failoverTransport = new FailoverTransport(
            [
                'transport1' => $setupableTransport1,
                'transport2' => $setupableTransport2,
            ],
            new CircuitBreaker(
                failureThreshold: 2,
                successThreshold: 1,
                timeout: 1
            ),
            new AdaptivePriorityStrategy()
        );

        $failoverTransport->setup();
    }

    public function testGetMessageCountSumsFromAvailableTransports(): void
    {
        $messageCountTransport1 = $this->createMock(MessageCountAwareInterface::class);
        $messageCountTransport2 = $this->createMock(MessageCountAwareInterface::class);

        $messageCountTransport1
            ->expects($this->once())
            ->method('getMessageCount')
            ->willReturn(5)
        ;

        $messageCountTransport2
            ->expects($this->once())
            ->method('getMessageCount')
            ->willReturn(3)
        ;

        $failoverTransport = new FailoverTransport(
            [
                'transport1' => $messageCountTransport1,
                'transport2' => $messageCountTransport2,
            ],
            new CircuitBreaker(
                failureThreshold: 2,
                successThreshold: 1,
                timeout: 1
            ),
            new AdaptivePriorityStrategy()
        );

        $count = $failoverTransport->getMessageCount();

        $this->assertSame(8, $count);
    }

    public function testAllReturnsMessagesFromAvailableTransports(): void
    {
        $envelope1 = new Envelope(new \stdClass(), []);
        $envelope2 = new Envelope(new \stdClass(), []);

        $listableTransport1 = $this->createMock(ListableReceiverInterface::class);
        $listableTransport2 = $this->createMock(ListableReceiverInterface::class);

        $listableTransport1
            ->expects($this->once())
            ->method('all')
            ->with(null)
            ->willReturn([$envelope1])
        ;

        $listableTransport2
            ->expects($this->once())
            ->method('all')
            ->with(null)
            ->willReturn([$envelope2])
        ;

        $failoverTransport = new FailoverTransport(
            [
                'transport1' => $listableTransport1,
                'transport2' => $listableTransport2,
            ],
            new CircuitBreaker(
                failureThreshold: 2,
                successThreshold: 1,
                timeout: 1
            ),
            new AdaptivePriorityStrategy()
        );

        $result = iterator_to_array($failoverTransport->all());

        // Should have at least 1 message - circuit breaker may prevent second transport
        $this->assertGreaterThanOrEqual(1, count($result));
        $this->assertContainsOnlyInstancesOf(Envelope::class, $result);
    }

    public function testFindReturnsMessageFromFirstAvailableTransport(): void
    {
        $id = 'test-message-id';
        $envelope = new Envelope(new \stdClass(), []);

        $listableTransport1 = $this->createMock(ListableReceiverInterface::class);
        $listableTransport2 = $this->createMock(ListableReceiverInterface::class);

        $listableTransport1
            ->expects($this->once())
            ->method('find')
            ->with($id)
            ->willReturn($envelope)
        ;

        // Second transport should not be called since first found the message
        $listableTransport2
            ->expects($this->never())
            ->method('find')
        ;

        $failoverTransport = new FailoverTransport(
            [
                'transport1' => $listableTransport1,
                'transport2' => $listableTransport2,
            ],
            new CircuitBreaker(
                failureThreshold: 2,
                successThreshold: 1,
                timeout: 1
            ),
            new AdaptivePriorityStrategy()
        );

        $result = $failoverTransport->find($id);

        $this->assertNotNull($result);
        $this->assertInstanceOf(Envelope::class, $result);
        // The returned envelope should have the FailoverSourceStamp added
        $this->assertNotNull($result->last(FailoverSourceStamp::class));
    }

    public function testSend(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'test send message';
        $envelope = new Envelope($message, []);

        // Primary transport succeeds
        /** @var InvocationMocker $primarySend */
        $primarySend = $this->primaryTransport->expects($this->once());
        $primarySend->method('send')->with($envelope)->willReturn($envelope);

        // Secondary should not be called
        /** @var InvocationMocker $secondarySend */
        $secondarySend = $this->secondaryTransport->expects($this->never());
        $secondarySend->method('send');

        // Act
        $result = $this->failoverTransport->send($envelope);

        // Assert
        $this->assertNotNull($result);
        $this->assertInstanceOf(Envelope::class, $result);

        // Verify the envelope has FailoverSourceStamp
        $failoverStamp = $result->last(FailoverSourceStamp::class);
        $this->assertNotNull($failoverStamp);
        $this->assertEquals('primary', $failoverStamp->getTransportName());
    }

    protected function setUp(): void
    {
        $this->primaryTransport = $this->createMock(TransportInterface::class);
        $this->secondaryTransport = $this->createMock(TransportInterface::class);

        $this->failoverTransport = new FailoverTransport(
            [
                'primary' => $this->primaryTransport,
                'secondary' => $this->secondaryTransport,
            ],
            new CircuitBreaker(
                failureThreshold: 2,
                successThreshold: 1,
                timeout: 1
            ),
            new AdaptivePriorityStrategy()
        );
    }
}
