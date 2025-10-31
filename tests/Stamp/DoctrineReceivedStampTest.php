<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Stamp;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp;

/**
 * @internal
 */
#[CoversClass(DoctrineReceivedStamp::class)]
final class DoctrineReceivedStampTest extends TestCase
{
    public function testGetId(): void
    {
        $stamp = new DoctrineReceivedStamp('12345');

        self::assertEquals('12345', $stamp->getId());
    }

    public function testGetIdWithNumericValue(): void
    {
        $stamp = new DoctrineReceivedStamp('99999');

        self::assertEquals('99999', $stamp->getId());
    }

    public function testGetIdWithUuid(): void
    {
        $uuid = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';
        $stamp = new DoctrineReceivedStamp($uuid);

        self::assertEquals($uuid, $stamp->getId());
    }

    public function testGetIdWithEmptyString(): void
    {
        $stamp = new DoctrineReceivedStamp('');

        self::assertEquals('', $stamp->getId());
    }

    public function testImmutability(): void
    {
        $id = 'test-id';
        $stamp1 = new DoctrineReceivedStamp($id);
        $stamp2 = new DoctrineReceivedStamp($id);

        self::assertNotSame($stamp1, $stamp2);
        self::assertEquals($stamp1->getId(), $stamp2->getId());
    }
}
