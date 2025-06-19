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

The bundle automatically configures Redis and Doctrine transports without requiring YAML configuration.

### Default Queue Names

- Redis normal queue: `async_messages`
- Redis delayed queue: `async_messages_delayed`
- Doctrine table: `messenger_messages`

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