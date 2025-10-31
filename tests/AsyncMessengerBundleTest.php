<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RunTestsInSeparateProcesses;
use Tourze\AsyncMessengerBundle\AsyncMessengerBundle;
use Tourze\PHPUnitSymfonyKernelTest\AbstractBundleTestCase;

/**
 * @internal
 */
#[CoversClass(AsyncMessengerBundle::class)]
#[RunTestsInSeparateProcesses]
final class AsyncMessengerBundleTest extends AbstractBundleTestCase
{
}
