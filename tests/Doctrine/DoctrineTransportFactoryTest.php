<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\RunTestsInSeparateProcesses;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransportFactory;
use Tourze\PHPUnitSymfonyKernelTest\AbstractIntegrationTestCase;

/**
 * @internal
 */
#[CoversClass(DoctrineTransportFactory::class)]
#[RunTestsInSeparateProcesses]
final class DoctrineTransportFactoryTest extends AbstractIntegrationTestCase
{
    protected function onSetUp(): void
    {
        // AbstractIntegrationTestCase 要求实现这个方法
    }

    private function getFactory(): DoctrineTransportFactory
    {
        // 从容器中获取服务而不是直接实例化
        return self::getService(DoctrineTransportFactory::class);
    }

    public function testSupports(): void
    {
        $factory = $this->getFactory();
        self::assertTrue($factory->supports('async-doctrine://', []));
        self::assertTrue($factory->supports('async-doctrine://default', []));
        self::assertFalse($factory->supports('doctrine://', []));
        self::assertFalse($factory->supports('redis://', []));
        self::assertFalse($factory->supports('amqp://', []));
    }

    public function testCreateTransport(): void
    {
        $factory = $this->getFactory();
        $serializer = $this->createMock(SerializerInterface::class);

        $transport = $factory->createTransport('async-doctrine://', [], $serializer);

        self::assertInstanceOf(DoctrineTransport::class, $transport);
    }

    public function testCreateTransportWithTableName(): void
    {
        $factory = $this->getFactory();
        $serializer = $this->createMock(SerializerInterface::class);

        $transport = $factory->createTransport('async-doctrine://', ['table_name' => 'custom_table'], $serializer);

        self::assertInstanceOf(DoctrineTransport::class, $transport);
    }
}
