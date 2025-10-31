# AsyncMessengerBundle

[English](README.md) | [中文](README.zh-CN.md)

[![PHP](https://img.shields.io/badge/php-%5E8.1-blue)](https://www.php.net/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Symfony](https://img.shields.io/badge/symfony-%5E6.4-green)](https://symfony.com/)

[![CI](https://github.com/tourze/php-monorepo/workflows/CI/badge.svg)](https://github.com/tourze/php-monorepo/actions)
[![codecov](https://codecov.io/gh/tourze/php-monorepo/graph/badge.svg?flag=async-messenger-bundle)](https://codecov.io/gh/tourze/php-monorepo)

A Symfony bundle providing Redis and Doctrine transports for Symfony Messenger 
with support for delayed and scheduled messages.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
  - [Automatic Transport Registration](#automatic-transport-registration)
  - [Environment Variables](#environment-variables)
- [Using the Transports](#using-the-transports)
  - [Basic Configuration](#basic-configuration)
  - [Customizing Transport Configuration](#customizing-transport-configuration)
- [Usage](#usage)
  - [Basic Usage](#basic-usage)
  - [Failover Transport](#failover-transport)
  - [Failover Features](#failover-features)
- [Advanced Usage](#advanced-usage)
  - [Performance Tuning](#performance-tuning)
  - [Monitoring and Debugging](#monitoring-and-debugging)
- [Requirements](#requirements)
- [License](#license)

## Features

- **Redis Transport**: Uses Redis lists and sorted sets for message queuing
- **Doctrine Transport**: Database-backed message queue
- **Delayed Messages**: Support for message delays
- **Scheduled Messages**: Support for scheduled message delivery
- **Queue Management**: Automatic queue size limiting and message reclaim
- **Failover Transport**: Automatic failover between multiple transports with circuit breaker pattern

## Installation

```bash
composer require tourze/async-messenger-bundle
```

## Configuration

### Automatic Transport Registration

The bundle automatically registers the following transports:
- `async_doctrine`: Doctrine-based transport for database message queuing
- `async_redis`: Redis-based transport for high-performance message queuing
- `async`: Failover transport that automatically switches between `async_doctrine` and `async_redis`
- `sync`: Synchronous transport for immediate processing

These transports are automatically configured with sensible defaults when the bundle is installed.

The default failure transport is set to `async_doctrine` for reliable message recovery.

The `async` transport uses our advanced failover mechanism with:
- Circuit breaker pattern to prevent cascading failures
- Adaptive priority consumption strategy
- Automatic failover from Doctrine to Redis and vice versa
- Self-healing with automatic recovery attempts

**Important**: The failover transport creates its own instances of the underlying 
transports (async_doctrine and async_redis). This means it doesn't share connections 
or state with separately configured async_doctrine or async_redis transports.

### Environment Variables

The bundle automatically registers two transports with minimal configuration:

```bash
# Enable/disable automatic transport registration (default: true)
ASYNC_MESSENGER_AUTO_CONFIGURE=true
```

The transports are registered with these simple DSNs:
- `async_doctrine`: `async-doctrine://`
- `async_redis`: `async-redis://`
- `async`: `failover://async_doctrine,async_redis`
- `sync`: `sync://`

All detailed configuration (table names, queue names, timeouts, etc.) is handled 
internally by the transport factories with sensible defaults.

## Using the Transports

### Basic Configuration

Once installed, the transports are automatically available in your messenger configuration:

```yaml
# config/packages/messenger.yaml
framework:
    messenger:
        routing:
            # Use the failover transport for critical messages
            'App\Message\CriticalMessage': async
            
            # Or use specific transports directly
            'App\Message\EmailMessage': async_doctrine
            'App\Message\NotificationMessage': async_redis
```

### Customizing Transport Configuration

If you need custom configuration, you can override the transport in your `messenger.yaml`:

```yaml
framework:
    messenger:
        transports:
            # Override the default async_doctrine transport
            async_doctrine:
                dsn: 'async-doctrine://'
                options:
                    table_name: 'my_custom_messages'
                    queue_name: 'priority'
                    redeliver_timeout: 7200
```

## Usage

### Basic Usage

```php
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\DelayStamp;

class MyController
{
    public function __construct(
        private MessageBusInterface $bus
    ) {}

    public function myAction()
    {
        // Send message immediately
        $this->bus->dispatch(new MyMessage());
        
        // Send message with 5 second delay
        $this->bus->dispatch(
            new MyMessage(),
            [new DelayStamp(5000)]
        );
    }
}
```

### Failover Transport

The failover transport provides automatic failover between multiple transports:

```yaml
# config/packages/messenger.yaml
framework:
    messenger:
        transports:
            # Define a failover transport that uses async_doctrine as primary
            # and async_redis as secondary
            async_failover:
                dsn: 'failover://async_doctrine,async_redis'
                options:
                    circuit_breaker:
                        failure_threshold: 5
                        success_threshold: 2
                        timeout: 30
                    consumption_strategy: 'adaptive_priority'
        
        routing:
            'App\Message\CriticalMessage': async_failover
```

### Failover Features

1. **Circuit Breaker Pattern**: Prevents cascading failures by temporarily disabling failed transports
2. **Multiple Consumption Strategies**:
    - `round_robin`: Simple round-robin between healthy transports
    - `weighted_round_robin`: Weights based on success rates
    - `adaptive_priority`: Dynamic priority based on performance
    - `latency_aware`: Selects transport with lowest latency

3. **Distributed Environment Support**: Each process maintains its own circuit breaker state
4. **Automatic Recovery**: Failed transports are automatically retried after timeout

## Advanced Usage

### Performance Tuning

For high-throughput applications, consider these configuration options:

```yaml
framework:
    messenger:
        transports:
            async_redis:
                dsn: 'async-redis://'
                options:
                    max_queue_size: 10000
                    redeliver_timeout: 3600
            
            async_doctrine:
                dsn: 'async-doctrine://'
                options:
                    table_name: 'messenger_messages'
                    queue_name: 'high_priority'
                    redeliver_timeout: 1800
```

### Monitoring and Debugging

The bundle provides built-in monitoring capabilities through stamps and exceptions:

```php
use Tourze\AsyncMessengerBundle\Stamp\FailoverSourceStamp;

// Check which transport processed a message
$sourceStamp = $envelope->last(FailoverSourceStamp::class);
if ($sourceStamp) {
    echo "Message processed by: " . $sourceStamp->getTransportName();
}
```

## Requirements

- PHP 8.1+
- Symfony 6.4+
- Redis extension 4.3.0+ (for Redis transport)
- Doctrine ORM (for Doctrine transport)

## License

This bundle is released under the MIT License. See the [LICENSE](LICENSE) file for details.