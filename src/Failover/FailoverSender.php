<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Failover;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Tourze\AsyncMessengerBundle\Exception\FailoverException;

class FailoverSender implements SenderInterface
{
    /**
     * @param TransportInterface[] $transports
     */
    public function __construct(
        private readonly array $transports,
        private readonly CircuitBreakerInterface $circuitBreaker,
        private readonly array $options = []
    ) {
    }

    public function send(Envelope $envelope): Envelope
    {
        $lastException = null;
        $attemptedTransports = [];
        
        // First, try to send to healthy transports in order
        foreach ($this->transports as $transportName => $transport) {
            if (!$this->circuitBreaker->isAvailable($transportName)) {
                continue;
            }
            
            $attemptedTransports[] = $transportName;
            
            try {
                $startTime = microtime(true);
                $sentEnvelope = $transport->send($envelope);
                $latency = (microtime(true) - $startTime) * 1000;
                
                $this->circuitBreaker->recordSuccess($transportName);
                
                // Log successful send if logger is available
                if (isset($this->options['logger'])) {
                    $this->options['logger']->debug('Message sent successfully', [
                        'transport' => $transportName,
                        'latency_ms' => $latency,
                    ]);
                }
                
                return $sentEnvelope->with(new Stamp\FailoverSourceStamp($transportName));
                
            } catch (\Throwable $e) {
                $this->circuitBreaker->recordFailure($transportName, $e);
                $lastException = $e;
                
                // Log the failure if logger is available
                if (isset($this->options['logger'])) {
                    $this->options['logger']->warning('Failed to send message to transport', [
                        'transport' => $transportName,
                        'exception' => $e,
                    ]);
                }
            }
        }
        
        // If all healthy transports failed, try unhealthy ones as last resort
        if ($this->options['try_unhealthy_on_failure'] ?? true) {
            foreach ($this->transports as $transportName => $transport) {
                if (in_array($transportName, $attemptedTransports, true)) {
                    continue;
                }
                
                try {
                    $sentEnvelope = $transport->send($envelope);
                    
                    // It worked! Maybe the transport is recovering
                    $this->circuitBreaker->recordSuccess($transportName);
                    
                    if (isset($this->options['logger'])) {
                        $this->options['logger']->info('Message sent to previously unhealthy transport', [
                            'transport' => $transportName,
                        ]);
                    }
                    
                    return $sentEnvelope->with(new Stamp\FailoverSourceStamp($transportName));
                    
                } catch (\Throwable $e) {
                    $this->circuitBreaker->recordFailure($transportName, $e);
                    $lastException = $e;
                }
            }
        }
        
        // All transports failed
        $errorMessage = sprintf(
            'All transports failed. Attempted: %s',
            implode(', ', array_keys($this->transports))
        );
        
        if (isset($this->options['logger'])) {
            $this->options['logger']->error($errorMessage, [
                'last_exception' => $lastException,
            ]);
        }
        
        throw new FailoverException($errorMessage, 0, $lastException);
    }
}