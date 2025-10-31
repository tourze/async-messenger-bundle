<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\Integration;

interface MessageCountAwareInterface
{
    public function getMessageCount(): int;
}
