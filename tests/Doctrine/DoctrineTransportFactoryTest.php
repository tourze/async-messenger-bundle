<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransportFactory;

final class DoctrineTransportFactoryTest extends TestCase
{
    private DoctrineTransportFactory $factory;
    private Connection $connection;

    protected function setUp(): void
    {
        $this->connection = $this->createMock(Connection::class);
        $this->factory = new DoctrineTransportFactory($this->connection);
    }

    public function testSupports(): void
    {
        self::assertTrue($this->factory->supports('doctrine://', []));
        self::assertTrue($this->factory->supports('doctrine://default', []));
        self::assertFalse($this->factory->supports('redis://', []));
        self::assertFalse($this->factory->supports('amqp://', []));
    }

    public function testCreateTransport(): void
    {
        $serializer = $this->createMock(SerializerInterface::class);
        
        $transport = $this->factory->createTransport('doctrine://', [], $serializer);
        
        self::assertInstanceOf(DoctrineTransport::class, $transport);
    }

    public function testCreateTransportWithTableName(): void
    {
        $serializer = $this->createMock(SerializerInterface::class);
        
        $transport = $this->factory->createTransport('doctrine://', ['table_name' => 'custom_table'], $serializer);
        
        self::assertInstanceOf(DoctrineTransport::class, $transport);
    }
}