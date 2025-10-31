<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreaker;
use Tourze\AsyncMessengerBundle\Failover\ConsumptionStrategy\RoundRobinStrategy;
use Tourze\AsyncMessengerBundle\Failover\FailoverReceiver;
use Tourze\AsyncMessengerBundle\Failover\FailoverTransport;

/**
 * @internal
 */
#[CoversClass(FailoverTransport::class)]
final class FailoverTransportTest extends TestCase
{
    public function testFailoverTransportRequiresAtLeastTwoTransports(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Failover transport requires at least 2 transports');

        $transport = $this->createMock(TransportInterface::class);

        new FailoverTransport(
            ['primary' => $transport],
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );
    }

    public function testSendFailsOverToPrimaryWhenSecondaryFails(): void
    {
        $envelope = new Envelope(new \stdClass(), []);
        $sentEnvelope = new Envelope(new \stdClass(), []);

        $primaryTransport = $this->createMock(TransportInterface::class);
        $primaryTransport->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willThrowException(new \RuntimeException('Primary failed'))
        ;

        $secondaryTransport = $this->createMock(TransportInterface::class);
        $secondaryTransport->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willReturn($sentEnvelope)
        ;

        $failoverTransport = new FailoverTransport(
            [
                'primary' => $primaryTransport,
                'secondary' => $secondaryTransport,
            ],
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );

        $result = $failoverTransport->send($envelope);

        $this->assertNotSame($envelope, $result);
    }

    public function testAllTransportsFailThrowsException(): void
    {
        $envelope = new Envelope(new \stdClass(), []);

        $primaryTransport = $this->createMock(TransportInterface::class);
        $primaryTransport->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willThrowException(new \RuntimeException('Primary failed'))
        ;

        $secondaryTransport = $this->createMock(TransportInterface::class);
        $secondaryTransport->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willThrowException(new \RuntimeException('Secondary failed'))
        ;

        $failoverTransport = new FailoverTransport(
            [
                'primary' => $primaryTransport,
                'secondary' => $secondaryTransport,
            ],
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('All transports failed');

        $failoverTransport->send($envelope);
    }

    public function testAck(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass(), []);
        /*
         * 1) FailoverReceiver 是具体类但不是简单的数据类，它包含复杂的故障转移逻辑
         * 2) 测试 FailoverTransport 的 ack 方法时，需要验证与 receiver 的交互，mock 具体类更准确
         * 3) 虽然实现了接口，但使用具体类可以确保所有必需的方法签名都匹配
         */
        $receiver = $this->createMock(FailoverReceiver::class);
        $receiver->expects($this->once())
            ->method('ack')
            ->with($envelope)
        ;

        $transports = [
            'primary' => $this->createMock(TransportInterface::class),
            'secondary' => $this->createMock(TransportInterface::class),
        ];

        $transport = new FailoverTransport(
            $transports,
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );

        // Use reflection to inject the mock receiver
        $reflection = new \ReflectionClass($transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($transport, $receiver);

        // Act
        $transport->ack($envelope);

        // Assert - expectations are verified automatically
    }

    public function testFind(): void
    {
        // Arrange
        $id = 'test-id';
        $envelope = new Envelope(new \stdClass(), []);
        /*
         * 1) FailoverReceiver 是具体类，测试需要验证与具体实现的交互
         * 2) find 方法是 FailoverReceiver 的特有逻辑，需要确保方法签名匹配
         * 3) 使用具体类 mock 可以避免接口变更时的测试失效
         */
        $receiver = $this->createMock(FailoverReceiver::class);
        $receiver->expects($this->once())
            ->method('find')
            ->with($id)
            ->willReturn($envelope)
        ;

        $transports = [
            'primary' => $this->createMock(TransportInterface::class),
            'secondary' => $this->createMock(TransportInterface::class),
        ];

        $transport = new FailoverTransport(
            $transports,
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );

        // Use reflection to inject the mock receiver
        $reflection = new \ReflectionClass($transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($transport, $receiver);

        // Act
        $result = $transport->find($id);

        // Assert
        $this->assertSame($envelope, $result);
    }

    public function testGet(): void
    {
        // Arrange
        $envelopes = [new Envelope(new \stdClass(), [])];
        /*
         * 1) FailoverReceiver 具体类包含复杂的消息获取逻辑（故障转移、熔断器等）
         * 2) 测试 get 方法需要验证与 FailoverReceiver 的正确集成
         * 3) Mock 具体类可以确保返回类型和方法行为一致
         */
        $receiver = $this->createMock(FailoverReceiver::class);
        $receiver->expects($this->once())
            ->method('get')
            ->willReturn($envelopes)
        ;

        $transports = [
            'primary' => $this->createMock(TransportInterface::class),
            'secondary' => $this->createMock(TransportInterface::class),
        ];

        $transport = new FailoverTransport(
            $transports,
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );

        // Use reflection to inject the mock receiver
        $reflection = new \ReflectionClass($transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($transport, $receiver);

        // Act
        $result = $transport->get();

        // Assert
        $this->assertSame($envelopes, $result);
    }

    public function testKeepalive(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass(), []);
        $seconds = 30;
        /*
         * 1) FailoverReceiver keepalive 方法具有特定的参数签名和行为
         * 2) 测试需要验证参数传递的正确性，使用具体类更准确
         * 3) keepalive 是 FailoverReceiver 的核心功能，需要与真实实现保持一致
         */
        $receiver = $this->createMock(FailoverReceiver::class);
        $receiver->expects($this->once())
            ->method('keepalive')
            ->with($envelope, $seconds)
        ;

        $transports = [
            'primary' => $this->createMock(TransportInterface::class),
            'secondary' => $this->createMock(TransportInterface::class),
        ];

        $transport = new FailoverTransport(
            $transports,
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );

        // Use reflection to inject the mock receiver
        $reflection = new \ReflectionClass($transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($transport, $receiver);

        // Act
        $transport->keepalive($envelope, $seconds);

        // Assert - expectations are verified automatically
    }

    public function testReject(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass(), []);
        /*
         * 1) FailoverReceiver 的 reject 方法涉及复杂的故障处理逻辑
         * 2) 测试需要验证 FailoverTransport 正确地委托给 receiver
         * 3) 使用具体类 mock 可以确保方法签名和行为匹配
         */
        $receiver = $this->createMock(FailoverReceiver::class);
        $receiver->expects($this->once())
            ->method('reject')
            ->with($envelope)
        ;

        $transports = [
            'primary' => $this->createMock(TransportInterface::class),
            'secondary' => $this->createMock(TransportInterface::class),
        ];

        $transport = new FailoverTransport(
            $transports,
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );

        // Use reflection to inject the mock receiver
        $reflection = new \ReflectionClass($transport);
        $receiverProperty = $reflection->getProperty('receiver');
        $receiverProperty->setAccessible(true);
        $receiverProperty->setValue($transport, $receiver);

        // Act
        $transport->reject($envelope);

        // Assert - expectations are verified automatically
    }

    public function testSetup(): void
    {
        // Arrange
        $primaryTransport = $this->createMock(TransportInterface::class);
        $secondaryTransport = $this->createMock(TransportInterface::class);

        // Only transports that implement SetupableTransportInterface should have setup() called
        $setupableTransport = $this->createMock(SetupableTransportInterface::class);
        $setupableTransport->expects($this->once())
            ->method('setup')
        ;

        $transports = [
            'primary' => $primaryTransport,
            'secondary' => $secondaryTransport,
            'setupable' => $setupableTransport,
        ];

        $transport = new FailoverTransport(
            $transports,
            new CircuitBreaker(),
            new RoundRobinStrategy()
        );

        // Act
        $transport->setup();

        // Assert - expectations are verified automatically
    }
}
