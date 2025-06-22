<?php

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;
use Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp;

class DoctrineReceivedStampTest extends TestCase
{
    public function test_implements_non_sendable_stamp_interface(): void
    {
        $stamp = new DoctrineReceivedStamp('test-id');
        
        $this->assertInstanceOf(NonSendableStampInterface::class, $stamp);
    }

    public function test_constructor_setsId(): void
    {
        $id = 'message-123';
        $stamp = new DoctrineReceivedStamp($id);
        
        $this->assertEquals($id, $stamp->getId());
    }

    public function test_getId_returnsCorrectId(): void
    {
        $id = 'unique-message-id-456';
        $stamp = new DoctrineReceivedStamp($id);
        
        $result = $stamp->getId();
        
        $this->assertEquals($id, $result);
    }

    public function test_constructor_acceptsEmptyString(): void
    {
        $stamp = new DoctrineReceivedStamp('');
        
        $this->assertEquals('', $stamp->getId());
    }

    public function test_constructor_acceptsNumericString(): void
    {
        $id = '12345';
        $stamp = new DoctrineReceivedStamp($id);
        
        $this->assertEquals($id, $stamp->getId());
    }

    public function test_constructor_acceptsUuidString(): void
    {
        $id = '550e8400-e29b-41d4-a716-446655440000';
        $stamp = new DoctrineReceivedStamp($id);
        
        $this->assertEquals($id, $stamp->getId());
    }
}