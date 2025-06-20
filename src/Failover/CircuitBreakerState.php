<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Tourze\EnumExtra\Itemable;
use Tourze\EnumExtra\ItemTrait;
use Tourze\EnumExtra\Labelable;
use Tourze\EnumExtra\Selectable;
use Tourze\EnumExtra\SelectTrait;

enum CircuitBreakerState: string implements Itemable, Labelable, Selectable
{
    use ItemTrait;
    use SelectTrait;

    case CLOSED = 'closed';
    case OPEN = 'open';
    case HALF_OPEN = 'half_open';
    
    public function getLabel(): string
    {
        return match ($this) {
            self::CLOSED => 'Closed (Normal)',
            self::OPEN => 'Open (Failing)',
            self::HALF_OPEN => 'Half-Open (Testing)',
        };
    }
}