<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Unit\Failover;

use PHPUnit\Framework\TestCase;
use Tourze\AsyncMessengerBundle\Failover\CircuitBreakerState;

class CircuitBreakerStateTest extends TestCase
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

    public function testToSelectItem(): void
    {
        $state = CircuitBreakerState::OPEN;
        $item = $state->toSelectItem();
        $this->assertArrayHasKey('value', $item);
        $this->assertArrayHasKey('label', $item);
        $this->assertArrayHasKey('text', $item);
        $this->assertArrayHasKey('name', $item);
        $this->assertSame('open', $item['value']);
        $this->assertSame('Open (Failing)', $item['label']);
    }
}