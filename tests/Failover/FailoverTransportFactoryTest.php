<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RunTestsInSeparateProcesses;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Failover\FailoverTransport;
use Tourze\AsyncMessengerBundle\Failover\FailoverTransportFactory;
use Tourze\PHPUnitSymfonyKernelTest\AbstractIntegrationTestCase;

/**
 * 集成测试：测试 FailoverTransportFactory 服务的依赖注入和工厂逻辑
 *
 * 设计决策说明：
 * - FailoverTransportFactory 是一个使用依赖注入的Symfony服务
 * - 使用 AutowireIterator 自动注入所有 messenger.transport_factory 标签的工厂
 * - 集成测试通过容器获取服务实例，验证真实的依赖注入行为
 * - 这是测试服务类的标准方式，与其他TransportFactory测试保持一致
 *
 * @internal
 */
#[CoversClass(FailoverTransportFactory::class)]
#[RunTestsInSeparateProcesses]
final class FailoverTransportFactoryTest extends AbstractIntegrationTestCase
{
    protected function onSetUp(): void
    {
        // AbstractIntegrationTestCase 要求实现这个方法
    }

    private function getFactory(): FailoverTransportFactory
    {
        // 从容器中获取服务实例，测试真实的依赖注入行为
        return self::getService(FailoverTransportFactory::class);
    }

    public function testSupports(): void
    {
        $factory = $this->getFactory();

        self::assertTrue($factory->supports('failover://async_doctrine,async_redis', []));
        self::assertFalse($factory->supports('redis://localhost', []));
    }

    public function testCreateTransportWithTransportNames(): void
    {
        $factory = $this->getFactory();

        // 使用真实的依赖注入工厂，验证容器中的async_doctrine和async_redis工厂被正确注入
        $transport = $factory->createTransport(
            'failover://async_doctrine,async_redis',
            [],
            new PhpSerializer()
        );

        self::assertInstanceOf(FailoverTransport::class, $transport);
    }

    public function testCreateTransportRequiresAtLeastTwoTransports(): void
    {
        $factory = $this->getFactory();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('No transport DSNs provided in DSN');

        // 测试空的failover DSN
        $factory->createTransport(
            'failover://',
            [],
            new PhpSerializer()
        );
    }
}
