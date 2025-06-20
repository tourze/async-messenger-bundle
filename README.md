# AsyncMessengerBundle

A Symfony bundle providing Redis and Doctrine transports for Symfony Messenger with support for delayed and scheduled messages.

## Features

- **Redis Transport**: Uses Redis lists and sorted sets for message queuing
- **Doctrine Transport**: Database-backed message queue
- **Delayed Messages**: Support for message delays
- **Scheduled Messages**: Support for scheduled message delivery
- **Queue Management**: Automatic queue size limiting and message reclaim

## Installation

```bash
composer require tourze/async-messenger-bundle
```

## Configuration

### Automatic Transport Registration

The bundle automatically registers the following transports:
- `async_doctrine`: Doctrine-based transport for database message queuing
- `async_redis`: Redis-based transport for high-performance message queuing

These transports are automatically configured with sensible defaults when the bundle is installed.

The default failure transport is set to `async_doctrine` for reliable message recovery.

### Environment Variables

The bundle automatically registers two transports with minimal configuration:

```bash
# Enable/disable automatic transport registration (default: true)
ASYNC_MESSENGER_AUTO_CONFIGURE=true
```

The transports are registered with these simple DSNs:
- `async_doctrine`: `async-doctrine://`
- `async_redis`: `async-redis://`

All detailed configuration (table names, queue names, timeouts, etc.) is handled internally by the transport factories with sensible defaults.

### Using the Transports

Once installed, the transports are automatically available in your messenger configuration:

```yaml
# config/packages/messenger.yaml
framework:
    messenger:
        routing:
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

## Requirements

- PHP 8.1+
- Symfony 6.4+
- Redis extension 4.3.0+ (for Redis transport)
- Doctrine ORM (for Doctrine transport)