<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Unit\Exception;

use LogicException;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Exception\TransportException;

class TransportExceptionTest extends TestCase
{
    public function testExceptionInheritance(): void
    {
        $exception = new TransportException('Test message');
        
        $this->assertInstanceOf(LogicException::class, $exception);
        $this->assertSame('Test message', $exception->getMessage());
    }

    public function testExceptionWithCodeAndPrevious(): void
    {
        $previous = new \Exception('Previous exception');
        $exception = new TransportException('Test message', 123, $previous);
        
        $this->assertSame('Test message', $exception->getMessage());
        $this->assertSame(123, $exception->getCode());
        $this->assertSame($previous, $exception->getPrevious());
    }
}