<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Exception\InvalidConfigurationException;

class FailoverTransport implements TransportInterface, SetupableTransportInterface, MessageCountAwareInterface, ListableReceiverInterface
{
    private FailoverReceiver $receiver;
    private FailoverSender $sender;

    /**
     * @param TransportInterface[] $transports
     */
    public function __construct(
        private readonly array $transports,
        private readonly CircuitBreakerInterface $circuitBreaker,
        private readonly ConsumptionStrategyInterface $consumptionStrategy,
        private readonly array $options = []
    ) {
        if (count($this->transports) < 2) {
            throw new InvalidConfigurationException('Failover transport requires at least 2 transports');
        }
    }

    public function get(): iterable
    {
        return $this->getReceiver()->get();
    }

    private function getReceiver(): FailoverReceiver
    {
        return $this->receiver ??= new FailoverReceiver(
            $this->transports,
            $this->circuitBreaker,
            $this->consumptionStrategy,
            $this->options
        );
    }

    public function ack(Envelope $envelope): void
    {
        $this->getReceiver()->ack($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        $this->getReceiver()->reject($envelope);
    }

    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        $this->getReceiver()->keepalive($envelope, $seconds);
    }

    public function send(Envelope $envelope): Envelope
    {
        return $this->getSender()->send($envelope);
    }

    private function getSender(): FailoverSender
    {
        return $this->sender ??= new FailoverSender(
            $this->transports,
            $this->circuitBreaker,
            $this->options
        );
    }

    public function setup(): void
    {
        foreach ($this->transports as $transport) {
            if ($transport instanceof SetupableTransportInterface) {
                $transport->setup();
            }
        }
    }

    public function getMessageCount(): int
    {
        $count = 0;
        foreach ($this->transports as $name => $transport) {
            if ($this->circuitBreaker->isAvailable($name) && $transport instanceof MessageCountAwareInterface) {
                try {
                    $count += $transport->getMessageCount();
                } catch (\Throwable $e) {
                    $this->circuitBreaker->recordFailure($name, $e);
                }
            }
        }
        return $count;
    }

    public function all(?int $limit = null): iterable
    {
        return $this->getReceiver()->all($limit);
    }

    public function find(mixed $id): ?Envelope
    {
        return $this->getReceiver()->find($id);
    }
}