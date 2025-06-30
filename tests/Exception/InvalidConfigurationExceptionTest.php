<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Unit\Exception;

use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Exception\InvalidConfigurationException;

class InvalidConfigurationExceptionTest extends TestCase
{
    public function testExceptionInheritance(): void
    {
        $exception = new InvalidConfigurationException('Test message');
        
        $this->assertInstanceOf(InvalidArgumentException::class, $exception);
        $this->assertSame('Test message', $exception->getMessage());
    }

    public function testExceptionWithCodeAndPrevious(): void
    {
        $previous = new \Exception('Previous exception');
        $exception = new InvalidConfigurationException('Test message', 123, $previous);
        
        $this->assertSame('Test message', $exception->getMessage());
        $this->assertSame(123, $exception->getCode());
        $this->assertSame($previous, $exception->getPrevious());
    }
}