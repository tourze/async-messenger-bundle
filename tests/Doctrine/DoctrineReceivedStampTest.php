<?php

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp;

/**
 * @internal
 */
#[CoversClass(DoctrineReceivedStamp::class)]
final class DoctrineReceivedStampTest extends TestCase
{
    public function testConstructorSetsId(): void
    {
        $id = 'message-123';
        $stamp = new DoctrineReceivedStamp($id);

        $this->assertEquals($id, $stamp->getId());
    }

    public function testGetIdReturnsCorrectId(): void
    {
        $id = 'unique-message-id-456';
        $stamp = new DoctrineReceivedStamp($id);

        $result = $stamp->getId();

        $this->assertEquals($id, $result);
    }

    public function testConstructorAcceptsEmptyString(): void
    {
        $stamp = new DoctrineReceivedStamp('');

        $this->assertEquals('', $stamp->getId());
    }

    public function testConstructorAcceptsNumericString(): void
    {
        $id = '12345';
        $stamp = new DoctrineReceivedStamp($id);

        $this->assertEquals($id, $stamp->getId());
    }

    public function testConstructorAcceptsUuidString(): void
    {
        $id = '550e8400-e29b-41d4-a716-446655440000';
        $stamp = new DoctrineReceivedStamp($id);

        $this->assertEquals($id, $stamp->getId());
    }
}
