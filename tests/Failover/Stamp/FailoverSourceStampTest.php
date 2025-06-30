<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\Stamp;

use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Failover\Stamp\FailoverSourceStamp;

final class FailoverSourceStampTest extends TestCase
{
    public function testGetTransportName(): void
    {
        $stamp = new FailoverSourceStamp('primary_transport');
        
        self::assertEquals('primary_transport', $stamp->getTransportName());
    }

    public function testGetTransportNameWithEmptyString(): void
    {
        $stamp = new FailoverSourceStamp('');
        
        self::assertEquals('', $stamp->getTransportName());
    }

    public function testGetTransportNameWithSpecialCharacters(): void
    {
        $transportName = 'transport_@#$%_123';
        $stamp = new FailoverSourceStamp($transportName);
        
        self::assertEquals($transportName, $stamp->getTransportName());
    }

    public function testImmutability(): void
    {
        $transportName = 'test_transport';
        $stamp1 = new FailoverSourceStamp($transportName);
        $stamp2 = new FailoverSourceStamp($transportName);
        
        self::assertNotSame($stamp1, $stamp2);
        self::assertEquals($stamp1->getTransportName(), $stamp2->getTransportName());
    }

    public function testDifferentTransportNames(): void
    {
        $stamp1 = new FailoverSourceStamp('transport1');
        $stamp2 = new FailoverSourceStamp('transport2');
        
        self::assertNotEquals($stamp1->getTransportName(), $stamp2->getTransportName());
    }
}