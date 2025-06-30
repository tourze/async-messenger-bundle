<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Unit\Exception;

use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tourze\AsyncMessengerBundle\Exception\FailoverException;

class FailoverExceptionTest extends TestCase
{
    public function testExceptionInheritance(): void
    {
        $exception = new FailoverException('Test message');
        
        $this->assertInstanceOf(RuntimeException::class, $exception);
        $this->assertSame('Test message', $exception->getMessage());
    }

    public function testExceptionWithCodeAndPrevious(): void
    {
        $previous = new \Exception('Previous exception');
        $exception = new FailoverException('Test message', 123, $previous);
        
        $this->assertSame('Test message', $exception->getMessage());
        $this->assertSame(123, $exception->getCode());
        $this->assertSame($previous, $exception->getPrevious());
    }
}