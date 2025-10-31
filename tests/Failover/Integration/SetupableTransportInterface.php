<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\Integration;

interface SetupableTransportInterface
{
    public function setup(): void;
}
