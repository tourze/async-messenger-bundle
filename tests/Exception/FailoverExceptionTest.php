<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Exception;

use PHPUnit\Framework\Attributes\CoversClass;
use Tourze\AsyncMessengerBundle\Exception\FailoverException;
use Tourze\PHPUnitBase\AbstractExceptionTestCase;

/**
 * @internal
 */
#[CoversClass(FailoverException::class)]
final class FailoverExceptionTest extends AbstractExceptionTestCase
{
    public function testExceptionWithCodeAndPrevious(): void
    {
        $previous = new \Exception('Previous exception');
        $exception = new FailoverException('Test message', 123, $previous);

        $this->assertSame('Test message', $exception->getMessage());
        $this->assertSame(123, $exception->getCode());
        $this->assertSame($previous, $exception->getPrevious());
    }
}
