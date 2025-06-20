<?php

namespace Tourze\AsyncMessengerBundle\Tests;

use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Tourze\AsyncMessengerBundle\AsyncMessengerBundle;
use Tourze\AsyncMessengerBundle\DependencyInjection\RemoveUnusedServicePass;

class AsyncMessengerBundleTest extends TestCase
{
    public function test_getPath_returnsCorrectPath(): void
    {
        $bundle = new AsyncMessengerBundle();
        // Bundle getPath() 返回包含 Bundle 类的目录，即 src 目录
        $expectedPath = dirname((new \ReflectionClass($bundle))->getFileName());

        $this->assertEquals($expectedPath, $bundle->getPath());
    }

    public function test_build_addsCompilerPass(): void
    {
        $container = $this->createMock(ContainerBuilder::class);
        $container->expects($this->once())
            ->method('addCompilerPass')
            ->with($this->isInstanceOf(RemoveUnusedServicePass::class))
            ->willReturn($container);

        $bundle = new AsyncMessengerBundle();
        $bundle->build($container);
    }
}