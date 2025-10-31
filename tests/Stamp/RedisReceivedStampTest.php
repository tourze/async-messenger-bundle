<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Stamp;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Stamp\RedisReceivedStamp;

/**
 * @internal
 */
#[CoversClass(RedisReceivedStamp::class)]
final class RedisReceivedStampTest extends TestCase
{
    public function testGetId(): void
    {
        $stamp = new RedisReceivedStamp('redis-message-123');

        self::assertEquals('redis-message-123', $stamp->getId());
    }

    public function testGetIdWithNumericValue(): void
    {
        $stamp = new RedisReceivedStamp('456789');

        self::assertEquals('456789', $stamp->getId());
    }

    public function testGetIdWithComplexId(): void
    {
        $id = 'queue:message:2024-01-01:12345';
        $stamp = new RedisReceivedStamp($id);

        self::assertEquals($id, $stamp->getId());
    }

    public function testGetIdWithEmptyString(): void
    {
        $stamp = new RedisReceivedStamp('');

        self::assertEquals('', $stamp->getId());
    }

    public function testGetIdWithSpecialCharacters(): void
    {
        $id = 'message_id:@#$%^&*()_+-=[]{}|;\':",.<>?/~`';
        $stamp = new RedisReceivedStamp($id);

        self::assertEquals($id, $stamp->getId());
    }

    public function testImmutability(): void
    {
        $id = 'test-redis-id';
        $stamp1 = new RedisReceivedStamp($id);
        $stamp2 = new RedisReceivedStamp($id);

        self::assertNotSame($stamp1, $stamp2);
        self::assertEquals($stamp1->getId(), $stamp2->getId());
    }

    public function testDifferentIdsProduceDifferentStamps(): void
    {
        $stamp1 = new RedisReceivedStamp('id1');
        $stamp2 = new RedisReceivedStamp('id2');

        self::assertNotEquals($stamp1->getId(), $stamp2->getId());
    }
}
