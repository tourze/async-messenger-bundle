<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Transport;

use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * Stamp to track which transport was used in the FallbackTransport
 */
final class FallbackStamp implements StampInterface
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