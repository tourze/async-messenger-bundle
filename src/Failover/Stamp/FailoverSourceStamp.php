<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

class FailoverSourceStamp implements StampInterface
{
    public function __construct(
        private readonly string $transportName
    ) {
    }

    public function getTransportName(): string
    {
        return $this->transportName;
    }
}
