<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Transport;

use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Transport\FallbackStamp;
use Tourze\AsyncMessengerBundle\Transport\FallbackTransport;

class FallbackTransportTest extends TestCase
{
    public function test_constructorThrowsExceptionWithEmptyTransports(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one transport must be provided');
        
        new FallbackTransport([]);
    }
    
    public function test_sendUsesFirstHealthyTransport(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass());
        $sentEnvelope = new Envelope(new \stdClass());
        
        $transport1 = $this->createMock(TransportInterface::class);
        $transport1->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willReturn($sentEnvelope);
        
        $transport2 = $this->createMock(TransportInterface::class);
        $transport2->expects($this->never())
            ->method('send');
        
        $fallback = new FallbackTransport([
            'redis' => $transport1,
            'doctrine' => $transport2,
        ]);
        
        // Act
        $result = $fallback->send($envelope);
        
        // Assert
        $this->assertInstanceOf(Envelope::class, $result);
        $stamp = $result->last(FallbackStamp::class);
        $this->assertNotNull($stamp);
        $this->assertEquals('redis', $stamp->getTransportName());
    }
    
    public function test_sendFallsBackWhenTransportFails(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass());
        $sentEnvelope = new Envelope(new \stdClass());
        
        $transport1 = $this->createMock(TransportInterface::class);
        $transport1->expects($this->once())
            ->method('send')
            ->willThrowException(new \RuntimeException('Redis connection failed'));
        
        $transport2 = $this->createMock(TransportInterface::class);
        $transport2->expects($this->once())
            ->method('send')
            ->with($envelope)
            ->willReturn($sentEnvelope);
        
        $transport3 = $this->createMock(TransportInterface::class);
        $transport3->expects($this->never())
            ->method('send');
        
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('warning')
            ->with(
                'Transport {transport} failed during {operation}: {error}',
                $this->callback(function ($context) {
                    return $context['transport'] === 'redis'
                        && $context['operation'] === 'send'
                        && $context['error'] === 'Redis connection failed';
                })
            );
        
        $fallback = new FallbackTransport([
            'redis' => $transport1,
            'doctrine' => $transport2,
            'sync' => $transport3,
        ], $logger);
        
        // Act
        $result = $fallback->send($envelope);
        
        // Assert
        $stamp = $result->last(FallbackStamp::class);
        $this->assertNotNull($stamp);
        $this->assertEquals('doctrine', $stamp->getTransportName());
        
        $health = $fallback->getTransportHealth();
        $this->assertFalse($health['redis']);
        $this->assertTrue($health['doctrine']);
        $this->assertTrue($health['sync']);
    }
    
    public function test_sendThrowsExceptionWhenAllTransportsFail(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass());
        
        $transport1 = $this->createMock(TransportInterface::class);
        $transport1->expects($this->once())
            ->method('send')
            ->willThrowException(new \RuntimeException('Redis failed'));
        
        $transport2 = $this->createMock(TransportInterface::class);
        $transport2->expects($this->once())
            ->method('send')
            ->willThrowException(new \RuntimeException('Doctrine failed'));
        
        $fallback = new FallbackTransport([
            'redis' => $transport1,
            'doctrine' => $transport2,
        ]);
        
        // Act & Assert
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('All transports failed for send operation');
        
        $fallback->send($envelope);
    }
    
    public function test_getReturnsEnvelopesFromFirstHealthyTransport(): void
    {
        // Arrange
        $envelope1 = new Envelope(new \stdClass());
        $envelope2 = new Envelope(new \stdClass());
        
        $transport1 = $this->createMock(TransportInterface::class);
        $transport1->expects($this->once())
            ->method('get')
            ->willReturn([$envelope1, $envelope2]);
        
        $transport2 = $this->createMock(TransportInterface::class);
        $transport2->expects($this->never())
            ->method('get');
        
        $fallback = new FallbackTransport([
            'redis' => $transport1,
            'doctrine' => $transport2,
        ]);
        
        // Act
        $result = iterator_to_array($fallback->get());
        
        // Assert
        $this->assertCount(2, $result);
        
        $stamp1 = $result[0]->last(FallbackStamp::class);
        $this->assertNotNull($stamp1);
        $this->assertEquals('redis', $stamp1->getTransportName());
        
        $stamp2 = $result[1]->last(FallbackStamp::class);
        $this->assertNotNull($stamp2);
        $this->assertEquals('redis', $stamp2->getTransportName());
    }
    
    public function test_ackUsesTransportFromStamp(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass());
        $envelope = $envelope->with(new FallbackStamp('doctrine'));
        
        $transport1 = $this->createMock(TransportInterface::class);
        $transport1->expects($this->never())
            ->method('ack');
        
        $transport2 = $this->createMock(TransportInterface::class);
        $transport2->expects($this->once())
            ->method('ack')
            ->with($envelope);
        
        $fallback = new FallbackTransport([
            'redis' => $transport1,
            'doctrine' => $transport2,
        ]);
        
        // Act
        $fallback->ack($envelope);
    }
    
    public function test_ackFallsBackWhenTransportFails(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass());
        $envelope = $envelope->with(new FallbackStamp('redis'));
        
        $transport1 = $this->createMock(TransportInterface::class);
        $transport1->expects($this->once())
            ->method('ack')
            ->willThrowException(new \RuntimeException('Redis failed'));
        
        $transport2 = $this->createMock(TransportInterface::class);
        $transport2->expects($this->once())
            ->method('ack')
            ->with($envelope);
        
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->atLeastOnce())
            ->method('warning');
        
        $fallback = new FallbackTransport([
            'redis' => $transport1,
            'doctrine' => $transport2,
        ], $logger);
        
        // Act
        $fallback->ack($envelope);
    }
    
    public function test_rejectUsesTransportFromStamp(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass());
        $envelope = $envelope->with(new FallbackStamp('doctrine'));
        
        $transport1 = $this->createMock(TransportInterface::class);
        $transport1->expects($this->never())
            ->method('reject');
        
        $transport2 = $this->createMock(TransportInterface::class);
        $transport2->expects($this->once())
            ->method('reject')
            ->with($envelope);
        
        $fallback = new FallbackTransport([
            'redis' => $transport1,
            'doctrine' => $transport2,
        ]);
        
        // Act
        $fallback->reject($envelope);
    }
    
    public function test_resetTransportHealth(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass());
        
        $transport1 = $this->createMock(TransportInterface::class);
        $transport1->expects($this->exactly(2))
            ->method('send')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new \RuntimeException('Failed')),
                new Envelope(new \stdClass())
            );
        
        $fallback = new FallbackTransport([
            'redis' => $transport1,
        ]);
        
        // Act - First send fails
        try {
            $fallback->send($envelope);
        } catch (\RuntimeException $e) {
            // Expected
        }
        
        $health = $fallback->getTransportHealth();
        $this->assertFalse($health['redis']);
        
        // Reset health
        $fallback->resetTransportHealth('redis');
        
        $health = $fallback->getTransportHealth();
        $this->assertTrue($health['redis']);
        
        // Second send should succeed
        $result = $fallback->send($envelope);
        $this->assertInstanceOf(Envelope::class, $result);
    }
    
    public function test_allTransportsUnhealthyResetsHealth(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass());
        
        $transport = $this->createMock(TransportInterface::class);
        $transport->expects($this->exactly(2))
            ->method('send')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new \RuntimeException('Failed')),
                new Envelope(new \stdClass())
            );
        
        $warnings = [];
        
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->exactly(2))
            ->method('warning')
            ->willReturnCallback(function ($message) use (&$warnings) {
                $warnings[] = $message;
            });
        
        // After test we'll check the warnings
        
        $fallback = new FallbackTransport([
            'redis' => $transport,
        ], $logger);
        
        // First send fails, marking transport as unhealthy
        try {
            $fallback->send($envelope);
        } catch (\RuntimeException $e) {
            // Expected
        }
        
        // Second send should reset health and succeed
        $result = $fallback->send($envelope);
        $this->assertInstanceOf(Envelope::class, $result);
        
        // Check warnings
        $this->assertCount(2, $warnings);
        $this->assertStringContainsString('Transport {transport} failed', $warnings[0]);
        $this->assertEquals('No healthy transports available, resetting all to healthy', $warnings[1]);
    }
    
    public function test_getLastUsedTransport(): void
    {
        // Arrange
        $envelope = new Envelope(new \stdClass());
        
        $transport1 = $this->createMock(TransportInterface::class);
        $transport1->method('send')
            ->willReturn(new Envelope(new \stdClass()));
        
        $transport2 = $this->createMock(TransportInterface::class);
        $transport2->method('get')
            ->willReturn([new Envelope(new \stdClass())]);
        
        $fallback = new FallbackTransport([
            'redis' => $transport1,
            'doctrine' => $transport2,
        ]);
        
        // Act
        $this->assertNull($fallback->getLastUsedTransport());
        
        $fallback->send($envelope);
        $this->assertEquals('redis', $fallback->getLastUsedTransport());
        
        iterator_to_array($fallback->get());
        $this->assertEquals('redis', $fallback->getLastUsedTransport());
    }
}