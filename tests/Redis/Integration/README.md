# Redis Transport Integration Tests

This directory contains comprehensive integration tests for the Redis transport implementation in the AsyncMessengerBundle.

## Test Structure

The integration tests follow the same pattern as the Doctrine integration tests:

1. **RedisIntegrationTestCase** - Base test class that sets up Redis connection and provides helper methods
2. **ConnectionIntegrationTest** - Tests for the Redis Connection class (send, receive, ack, reject operations)
3. **RedisTransportIntegrationTest** - End-to-end tests for RedisTransport
4. **ConcurrentConsumptionTest** - Tests for concurrent message consumption scenarios
5. **MessageRedeliveryTest** - Tests for message redelivery functionality
6. **MultiQueueIsolationTest** - Tests for multi-queue isolation

## Running the Tests

### Prerequisites

- Redis server must be running on localhost:6379
- PHP Redis extension must be installed

### Running Tests

```bash
# Run all Redis integration tests
vendor/bin/phpunit packages/async-messenger-bundle/tests/Redis/Integration/

# Or use the helper script
php packages/async-messenger-bundle/run-redis-integration-tests.php
```

## Test Features

### Redis-Specific Features Tested

1. **Redis Streams** - For message queuing
2. **Sorted Sets** - For delayed messages
3. **List Operations** - For FIFO message processing
4. **Atomic Operations** - For concurrent access safety

### Key Test Scenarios

1. **Basic Operations**
   - Send and receive messages
   - Acknowledge and reject messages
   - Message counting

2. **Delayed Messages**
   - Messages with DelayStamp
   - Time-based message delivery
   - Mixed immediate and delayed messages

3. **Concurrent Consumption**
   - Multiple consumers processing messages
   - No message duplication
   - Proper message locking

4. **Message Redelivery**
   - Abandoned message detection
   - Configurable redelivery timeout
   - Keepalive mechanism

5. **Multi-Queue Isolation**
   - Complete queue isolation
   - Independent queue operations
   - Per-queue configuration

## Configuration Options Tested

- `queue` - Queue name for normal messages
- `delayed_queue` - Queue name for delayed messages
- `redeliver_timeout` - Time before abandoned messages are redelivered
- `claim_interval` - How often to check for abandoned messages
- `queue_max_entries` - Maximum number of messages in queue

## Important Notes

1. Tests use Redis database 15 to avoid conflicts
2. Each test clears the database before and after execution
3. Tests handle Redis unavailability gracefully by skipping
4. All tests are isolated and can run in any order