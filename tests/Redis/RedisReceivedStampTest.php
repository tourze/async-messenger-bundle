<?php

namespace Tourze\AsyncMessengerBundle\Tests\Redis;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Stamp\RedisReceivedStamp;

/**
 * @internal
 */
#[CoversClass(RedisReceivedStamp::class)]
final class RedisReceivedStampTest extends TestCase
{
    public function testConstructorSetsId(): void
    {
        $id = 'redis-message-123';
        $stamp = new RedisReceivedStamp($id);

        $this->assertEquals($id, $stamp->getId());
    }

    public function testGetIdReturnsCorrectId(): void
    {
        $id = 'unique-redis-message-id-456';
        $stamp = new RedisReceivedStamp($id);

        $result = $stamp->getId();

        $this->assertEquals($id, $result);
    }

    public function testConstructorAcceptsEmptyString(): void
    {
        $stamp = new RedisReceivedStamp('');

        $this->assertEquals('', $stamp->getId());
    }

    public function testConstructorAcceptsRedisStreamId(): void
    {
        $id = '1609459200000-0'; // Redis stream ID format
        $stamp = new RedisReceivedStamp($id);

        $this->assertEquals($id, $stamp->getId());
    }

    public function testConstructorAcceptsComplexRedisId(): void
    {
        $id = '1609459200000-123456789';
        $stamp = new RedisReceivedStamp($id);

        $this->assertEquals($id, $stamp->getId());
    }

    public function testConstructorAcceptsAlphanumericId(): void
    {
        $id = 'abc123-def456';
        $stamp = new RedisReceivedStamp($id);

        $this->assertEquals($id, $stamp->getId());
    }

    protected function setUp(): void
    {
        parent::setUp();
        // 测试 Stamp 不需要特殊的设置
    }
}
