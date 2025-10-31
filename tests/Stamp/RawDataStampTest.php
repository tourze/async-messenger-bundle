<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Stamp;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Stamp\RawDataStamp;

/**
 * @internal
 */
#[CoversClass(RawDataStamp::class)]
final class RawDataStampTest extends TestCase
{
    public function testGetRawData(): void
    {
        $data = ['key' => 'value', 'number' => 42];
        $stamp = new RawDataStamp($data);

        self::assertEquals($data, $stamp->getRawData());
    }

    public function testGetRawDataWithEmptyArray(): void
    {
        $stamp = new RawDataStamp([]);

        self::assertEquals([], $stamp->getRawData());
    }

    public function testGetRawDataWithNestedArray(): void
    {
        $data = [
            'level1' => [
                'level2' => [
                    'key' => 'deep value',
                ],
            ],
            'array' => [1, 2, 3],
        ];
        $stamp = new RawDataStamp($data);

        self::assertEquals($data, $stamp->getRawData());
    }

    public function testGetRawDataWithMixedTypes(): void
    {
        $data = [
            'string' => 'text',
            'int' => 123,
            'float' => 3.14,
            'bool' => true,
            'null' => null,
            'array' => ['a', 'b', 'c'],
        ];
        $stamp = new RawDataStamp($data);

        self::assertEquals($data, $stamp->getRawData());
    }

    public function testImmutability(): void
    {
        $data = ['key' => 'value'];
        $stamp = new RawDataStamp($data);

        // Modify original data
        $data['key'] = 'modified';

        // Stamp data should not be affected
        self::assertEquals(['key' => 'value'], $stamp->getRawData());
    }

    public function testMultipleStampsWithSameData(): void
    {
        $data = ['shared' => 'data'];
        $stamp1 = new RawDataStamp($data);
        $stamp2 = new RawDataStamp($data);

        self::assertNotSame($stamp1, $stamp2);
        self::assertEquals($stamp1->getRawData(), $stamp2->getRawData());
    }
}
