<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use PHPUnit\Framework\Attributes\CoversClass;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerState;
use Tourze\PHPUnitEnum\AbstractEnumTestCase;

/**
 * @internal
 */
#[CoversClass(CircuitBreakerState::class)]
final class CircuitBreakerStateTest extends AbstractEnumTestCase
{
    public function testEnumValues(): void
    {
        $this->assertSame('closed', CircuitBreakerState::CLOSED->value);
        $this->assertSame('open', CircuitBreakerState::OPEN->value);
        $this->assertSame('half_open', CircuitBreakerState::HALF_OPEN->value);
    }

    public function testGetLabel(): void
    {
        $this->assertSame('Closed (Normal)', CircuitBreakerState::CLOSED->getLabel());
        $this->assertSame('Open (Failing)', CircuitBreakerState::OPEN->getLabel());
        $this->assertSame('Half-Open (Testing)', CircuitBreakerState::HALF_OPEN->getLabel());
    }

    public function testAllCasesHaveLabels(): void
    {
        foreach (CircuitBreakerState::cases() as $state) {
            $this->assertNotEmpty($state->getLabel());
        }
    }

    public function testSelectTrait(): void
    {
        $options = CircuitBreakerState::genOptions();
        $this->assertCount(3, $options);

        $expectedOptions = [
            [
                'label' => 'Closed (Normal)',
                'text' => 'Closed (Normal)',
                'value' => 'closed',
                'name' => 'Closed (Normal)',
            ],
            [
                'label' => 'Open (Failing)',
                'text' => 'Open (Failing)',
                'value' => 'open',
                'name' => 'Open (Failing)',
            ],
            [
                'label' => 'Half-Open (Testing)',
                'text' => 'Half-Open (Testing)',
                'value' => 'half_open',
                'name' => 'Half-Open (Testing)',
            ],
        ];

        $this->assertEquals($expectedOptions, $options);
    }

    public function testItemTrait(): void
    {
        $state = CircuitBreakerState::CLOSED;
        $item = $state->toArray();
        $this->assertCount(2, $item);
        $this->assertArrayHasKey('value', $item);
        $this->assertArrayHasKey('label', $item);
        $this->assertSame('closed', $item['value']);
        $this->assertSame('Closed (Normal)', $item['label']);
    }

    public function testToArray(): void
    {
        // Test all enum cases
        foreach (CircuitBreakerState::cases() as $state) {
            $array = $state->toArray();

            // Assert structure
            $this->assertIsArray($array);
            $this->assertArrayHasKey('value', $array);
            $this->assertArrayHasKey('label', $array);

            // Assert values
            $this->assertEquals($state->value, $array['value']);
            $this->assertEquals($state->getLabel(), $array['label']);
        }

        // Test specific cases
        $closed = CircuitBreakerState::CLOSED->toArray();
        $this->assertEquals(['value' => 'closed', 'label' => 'Closed (Normal)'], $closed);

        $open = CircuitBreakerState::OPEN->toArray();
        $this->assertEquals(['value' => 'open', 'label' => 'Open (Failing)'], $open);

        $halfOpen = CircuitBreakerState::HALF_OPEN->toArray();
        $this->assertEquals(['value' => 'half_open', 'label' => 'Half-Open (Testing)'], $halfOpen);
    }

    protected function onSetUp(): void
    {
        // 这个测试类不需要特殊的设置
    }
}
