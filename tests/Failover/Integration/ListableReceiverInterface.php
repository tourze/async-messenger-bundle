<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover\Integration;

use Symfony\Component\Messenger\Envelope;

interface ListableReceiverInterface
{
    /**
     * @return iterable<Envelope>
     */
    public function all(?string $queueName = null): iterable;

    public function find(string $id): ?Envelope;
}
