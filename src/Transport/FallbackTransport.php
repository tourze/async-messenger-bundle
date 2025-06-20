<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Transport;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * FallbackTransport provides automatic failover between multiple transports.
 * 
 * It tries transports in order: Redis -> Doctrine -> Sync
 * If a transport fails, it automatically falls back to the next one.
 */
final class FallbackTransport implements TransportInterface
{
    private LoggerInterface $logger;
    
    /**
     * @var array<string, TransportInterface>
     */
    private array $transports;
    
    /**
     * @var array<string, bool>
     */
    private array $transportHealth = [];
    
    private ?string $lastUsedTransport = null;

    /**
     * @param array<string, TransportInterface> $transports Ordered list of transports to try
     */
    public function __construct(
        array $transports,
        ?LoggerInterface $logger = null
    ) {
        if (empty($transports)) {
            throw new \InvalidArgumentException('At least one transport must be provided');
        }
        
        $this->transports = $transports;
        $this->logger = $logger ?? new NullLogger();
        
        // Initialize all transports as healthy
        foreach (array_keys($transports) as $name) {
            $this->transportHealth[$name] = true;
        }
    }

    public function get(): iterable
    {
        foreach ($this->getHealthyTransports() as $name => $transport) {
            try {
                $envelopes = $transport->get();
                $this->lastUsedTransport = $name;
                
                // Wrap envelopes with FallbackStamp to track which transport was used
                foreach ($envelopes as $envelope) {
                    yield $envelope->with(new FallbackStamp($name));
                }
                
                return;
            } catch (\Throwable $e) {
                $this->handleTransportFailure($name, 'get', $e);
            }
        }
        
        // All transports failed
        $this->logger->error('All transports failed for get operation');
        return [];
    }

    public function ack(Envelope $envelope): void
    {
        $stamp = $envelope->last(FallbackStamp::class);
        $transportName = $stamp ? $stamp->getTransportName() : $this->lastUsedTransport;
        
        if ($transportName && isset($this->transports[$transportName])) {
            try {
                $this->transports[$transportName]->ack($envelope);
                return;
            } catch (\Throwable $e) {
                $this->handleTransportFailure($transportName, 'ack', $e);
            }
        }
        
        // Fallback: try all healthy transports
        foreach ($this->getHealthyTransports() as $name => $transport) {
            try {
                $transport->ack($envelope);
                return;
            } catch (\Throwable $e) {
                $this->handleTransportFailure($name, 'ack', $e);
            }
        }
        
        $this->logger->error('All transports failed for ack operation');
    }

    public function reject(Envelope $envelope): void
    {
        $stamp = $envelope->last(FallbackStamp::class);
        $transportName = $stamp ? $stamp->getTransportName() : $this->lastUsedTransport;
        
        if ($transportName && isset($this->transports[$transportName])) {
            try {
                $this->transports[$transportName]->reject($envelope);
                return;
            } catch (\Throwable $e) {
                $this->handleTransportFailure($transportName, 'reject', $e);
            }
        }
        
        // Fallback: try all healthy transports
        foreach ($this->getHealthyTransports() as $name => $transport) {
            try {
                $transport->reject($envelope);
                return;
            } catch (\Throwable $e) {
                $this->handleTransportFailure($name, 'reject', $e);
            }
        }
        
        $this->logger->error('All transports failed for reject operation');
    }

    public function send(Envelope $envelope): Envelope
    {
        foreach ($this->getHealthyTransports() as $name => $transport) {
            try {
                $sentEnvelope = $transport->send($envelope);
                $this->lastUsedTransport = $name;
                
                // Mark transport as healthy on success
                $this->transportHealth[$name] = true;
                
                // Add stamp to track which transport was used
                return $sentEnvelope->with(new FallbackStamp($name));
            } catch (\Throwable $e) {
                $this->handleTransportFailure($name, 'send', $e);
            }
        }
        
        // All transports failed - throw exception
        throw new \RuntimeException('All transports failed for send operation');
    }
    
    /**
     * @return array<string, TransportInterface>
     */
    private function getHealthyTransports(): array
    {
        $healthy = [];
        
        foreach ($this->transports as $name => $transport) {
            if ($this->transportHealth[$name]) {
                $healthy[$name] = $transport;
            }
        }
        
        // If no healthy transports, reset all to healthy and try again
        if (empty($healthy)) {
            $this->logger->warning('No healthy transports available, resetting all to healthy');
            foreach (array_keys($this->transports) as $name) {
                $this->transportHealth[$name] = true;
            }
            return $this->transports;
        }
        
        return $healthy;
    }
    
    private function handleTransportFailure(string $name, string $operation, \Throwable $e): void
    {
        $this->transportHealth[$name] = false;
        
        $this->logger->warning(
            'Transport {transport} failed during {operation}: {error}',
            [
                'transport' => $name,
                'operation' => $operation,
                'error' => $e->getMessage(),
                'exception' => $e,
            ]
        );
    }
    
    /**
     * Get the current health status of all transports
     * 
     * @return array<string, bool>
     */
    public function getTransportHealth(): array
    {
        return $this->transportHealth;
    }
    
    /**
     * Reset a transport to healthy status
     */
    public function resetTransportHealth(string $name): void
    {
        if (isset($this->transports[$name])) {
            $this->transportHealth[$name] = true;
        }
    }
    
    /**
     * Get the last successfully used transport name
     */
    public function getLastUsedTransport(): ?string
    {
        return $this->lastUsedTransport;
    }
}