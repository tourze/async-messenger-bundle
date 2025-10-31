<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Tourze\AsyncMessengerBundle\Doctrine;

use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\Exception\RetryableException;
use LogicException as BaseLogicException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp;

/**
 * @author Vincent Touzet <vincent.touzet@gmail.com>
 */
class DoctrineReceiver implements ListableReceiverInterface, MessageCountAwareInterface
{
    private const MAX_RETRIES = 3;

    private int $retryingSafetyCounter = 0;

    private SerializerInterface $serializer;

    public function __construct(
        private Connection $connection,
        ?SerializerInterface $serializer = null,
    ) {
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    public function get(): iterable
    {
        try {
            $doctrineEnvelope = $this->connection->get();
            $this->retryingSafetyCounter = 0; // reset counter
        } catch (RetryableException $exception) {
            // Do nothing when RetryableException occurs less than "MAX_RETRIES"
            // as it will likely be resolved on the next call to get()
            // Problem with concurrent consumers and database deadlocks
            if (++$this->retryingSafetyCounter >= self::MAX_RETRIES) {
                $this->retryingSafetyCounter = 0; // reset counter
                throw new TransportException($exception->getMessage(), 0, $exception);
            }

            return [];
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        } catch (\Exception $exception) {
            if ($exception instanceof LogicException || $exception instanceof BaseLogicException) {
                throw $exception;
            }
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        if (null === $doctrineEnvelope) {
            return [];
        }

        return [$this->createEnvelopeFromData($doctrineEnvelope)];
    }

    /**
     * @param array<string, mixed> $data
     */
    private function createEnvelopeFromData(array $data): Envelope
    {
        try {
            $envelope = $this->serializer->decode([
                'body' => $data['body'],
                'headers' => $data['headers'],
            ]);
        } catch (MessageDecodingFailedException $exception) {
            $messageIdValue = $data['id'] ?? '';
            $messageId = is_scalar($messageIdValue) ? (string) $messageIdValue : '';
            $this->connection->reject($messageId);

            throw $exception;
        }

        $messageIdValue = $data['id'] ?? '';
        $messageId = is_scalar($messageIdValue) ? (string) $messageIdValue : '';

        return $envelope
            ->withoutAll(TransportMessageIdStamp::class)
            ->with(
                new DoctrineReceivedStamp($messageId),
                new TransportMessageIdStamp($messageId)
            )
        ;
    }

    public function reject(Envelope $envelope): void
    {
        $this->withRetryableExceptionRetry(function () use ($envelope): void {
            $this->connection->reject($this->findDoctrineReceivedStamp($envelope)->getId());
        });
    }

    private function withRetryableExceptionRetry(callable $callable): void
    {
        $retryState = $this->initializeRetryState();

        while (true) {
            try {
                $callable();

                return; // 成功执行，退出
            } catch (RetryableException $exception) {
                $retryState = $this->handleRetryableException($exception, $retryState);
            } catch (DBALException $exception) {
                throw new TransportException($exception->getMessage(), 0, $exception);
            } catch (\Exception $exception) {
                $this->handleGeneralException($exception);
            }
        }
    }

    /**
     * @return array{delay: int, multiplier: int, jitter: float, retries: int, maxRetries: int}
     */
    private function initializeRetryState(): array
    {
        return [
            'delay' => 100,
            'multiplier' => 2,
            'jitter' => 0.1,
            'retries' => 0,
            'maxRetries' => self::MAX_RETRIES,
        ];
    }

    /**
     * @param array{delay: int, multiplier: int, jitter: float, retries: int, maxRetries: int} $retryState
     * @return array{delay: int, multiplier: int, jitter: float, retries: int, maxRetries: int}
     */
    private function handleRetryableException(RetryableException $exception, array $retryState): array
    {
        if (++$retryState['retries'] >= $retryState['maxRetries']) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        $retryState['delay'] *= $retryState['multiplier'];

        $randomness = (int) ($retryState['delay'] * $retryState['jitter']);
        $retryState['delay'] += random_int(-$randomness, +$randomness);

        usleep($retryState['delay'] * 1000);

        return $retryState;
    }

    private function handleGeneralException(\Exception $exception): void
    {
        if ($exception instanceof LogicException || $exception instanceof BaseLogicException) {
            throw $exception;
        }
        throw new TransportException($exception->getMessage(), 0, $exception);
    }

    private function findDoctrineReceivedStamp(Envelope $envelope): DoctrineReceivedStamp
    {
        $doctrineReceivedStamp = $envelope->last(DoctrineReceivedStamp::class);
        assert($doctrineReceivedStamp instanceof DoctrineReceivedStamp || null === $doctrineReceivedStamp);

        if (null === $doctrineReceivedStamp) {
            throw new LogicException('No DoctrineReceivedStamp found on the Envelope.');
        }

        return $doctrineReceivedStamp;
    }

    public function ack(Envelope $envelope): void
    {
        $this->withRetryableExceptionRetry(function () use ($envelope): void {
            $this->connection->ack($this->findDoctrineReceivedStamp($envelope)->getId());
        });
    }

    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        $this->connection->keepalive($this->findDoctrineReceivedStamp($envelope)->getId(), $seconds);
    }

    public function getMessageCount(): int
    {
        try {
            return $this->connection->getMessageCount();
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        } catch (\Exception $exception) {
            if ($exception instanceof LogicException || $exception instanceof BaseLogicException) {
                throw $exception;
            }
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function all(?int $limit = null): iterable
    {
        try {
            $doctrineEnvelopes = $this->connection->findAll($limit);
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        } catch (\Exception $exception) {
            if ($exception instanceof LogicException || $exception instanceof BaseLogicException) {
                throw $exception;
            }
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        foreach ($doctrineEnvelopes as $doctrineEnvelope) {
            yield $this->createEnvelopeFromData($doctrineEnvelope);
        }
    }

    public function find(mixed $id): ?Envelope
    {
        try {
            $doctrineEnvelope = $this->connection->find($id);
        } catch (DBALException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        } catch (\Exception $exception) {
            if ($exception instanceof LogicException || $exception instanceof BaseLogicException) {
                throw $exception;
            }
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        if (null === $doctrineEnvelope) {
            return null;
        }

        return $this->createEnvelopeFromData($doctrineEnvelope);
    }
}
