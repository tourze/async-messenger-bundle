<?php

namespace Tourze\AsyncMessengerBundle\Tests\Redis;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;
use Tourze\AsyncMessengerBundle\Redis\RedisReceivedStamp;

class RedisReceivedStampTest extends TestCase
{
    public function test_implements_non_sendable_stamp_interface(): void
    {
        $stamp = new RedisReceivedStamp('test-id');
        
        $this->assertInstanceOf(NonSendableStampInterface::class, $stamp);
    }

    public function test_constructor_setsId(): void
    {
        $id = 'redis-message-123';
        $stamp = new RedisReceivedStamp($id);
        
        $this->assertEquals($id, $stamp->getId());
    }

    public function test_getId_returnsCorrectId(): void
    {
        $id = 'unique-redis-message-id-456';
        $stamp = new RedisReceivedStamp($id);
        
        $result = $stamp->getId();
        
        $this->assertEquals($id, $result);
    }

    public function test_constructor_acceptsEmptyString(): void
    {
        $stamp = new RedisReceivedStamp('');
        
        $this->assertEquals('', $stamp->getId());
    }

    public function test_constructor_acceptsRedisStreamId(): void
    {
        $id = '1609459200000-0'; // Redis stream ID format
        $stamp = new RedisReceivedStamp($id);
        
        $this->assertEquals($id, $stamp->getId());
    }

    public function test_constructor_acceptsComplexRedisId(): void
    {
        $id = '1609459200000-123456789';
        $stamp = new RedisReceivedStamp($id);
        
        $this->assertEquals($id, $stamp->getId());
    }

    public function test_constructor_acceptsAlphanumericId(): void
    {
        $id = 'abc123-def456';
        $stamp = new RedisReceivedStamp($id);
        
        $this->assertEquals($id, $stamp->getId());
    }
}