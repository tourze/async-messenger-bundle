<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;

class MessageRedeliveryTest extends RedisIntegrationTestCase
{
    private PhpSerializer $serializer;

    public function test_abandonedMessage_isRedelivered(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1, // 1秒后重投递
            'claim_interval' => 200, // 200ms 检查间隔
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'will be abandoned';
        $message->id = 'msg-001';
        $envelope = new Envelope($message);

        // Act
        $transport->send($envelope);

        // 第一个消费者获取消息但不处理
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages1 = $consumer1->get();
        $this->assertCount(1, $messages1);
        $this->assertEquals('will be abandoned', $messages1[0]->getMessage()->content);

        // 不调用 ack，模拟消费者崩溃或处理失败
        // 等待重投递
        sleep(2);

        // 第二个消费者应该能获取到消息
        $consumer2 = new RedisTransport($connection, $this->serializer);
        $messages2 = $consumer2->get();

        // Assert
        $this->assertCount(1, $messages2);
        $this->assertEquals('will be abandoned', $messages2[0]->getMessage()->content);
        $this->assertEquals('msg-001', $messages2[0]->getMessage()->id);

        // 清理
        $consumer2->ack($messages2[0]);
    }

    public function test_multipleRedeliveries_maintainMessageIntegrity(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $originalMessage = new \stdClass();
        $originalMessage->content = 'test content';
        $originalMessage->data = ['key' => 'value', 'number' => 42];
        $originalMessage->id = 'unique-id-123';
        $envelope = new Envelope($originalMessage);

        // Act
        $transport->send($envelope);

        // 多次获取但不确认，模拟多次失败
        for ($i = 0; $i < 3; $i++) {
            $consumer = new RedisTransport($connection, $this->serializer);
            $messages = $consumer->get();

            $this->assertCount(1, $messages);
            $receivedMessage = $messages[0]->getMessage();

            // 验证消息完整性
            $this->assertEquals('test content', $receivedMessage->content);
            $this->assertEquals(['key' => 'value', 'number' => 42], $receivedMessage->data);
            $this->assertEquals('unique-id-123', $receivedMessage->id);

            // 不确认，等待重投递
            sleep(2);
        }

        // 最终成功处理
        $finalConsumer = new RedisTransport($connection, $this->serializer);
        $finalMessages = $finalConsumer->get();
        $this->assertCount(1, $finalMessages);
        $finalConsumer->ack($finalMessages[0]);

        // Assert
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function test_keepalive_preventsRedelivery(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 2, // 2秒超时
            'claim_interval' => 500,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'long processing';
        $envelope = new Envelope($message);

        // Act
        $transport->send($envelope);

        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages = $consumer1->get();
        $this->assertCount(1, $messages);
        $processingMessage = $messages[0];

        // 模拟长时间处理（4秒），期间定期调用 keepalive
        for ($i = 0; $i < 4; $i++) {
            sleep(1);
            $consumer1->keepalive($processingMessage);

            // 其他消费者不应该获取到消息
            $consumer2 = new RedisTransport($connection, $this->serializer);
            $otherMessages = $consumer2->get();
            $this->assertEmpty($otherMessages);
        }

        // 完成处理
        $consumer1->ack($processingMessage);

        // Assert
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function test_redeliveryTimeout_configuration(): void
    {
        // 测试不同的重投递超时配置
        $timeouts = [1, 3, 5]; // 秒

        foreach ($timeouts as $timeout) {
            // Arrange
            $options = array_merge($this->getConnectionOptions(), [
                'redeliver_timeout' => $timeout,
                'claim_interval' => 100,
                'queue' => "test_queue_timeout_{$timeout}",
                'delayed_queue' => "test_queue_timeout_{$timeout}_delayed",
            ]);
            $connection = new Connection($this->redis, $options);
            $transport = new RedisTransport($connection, $this->serializer);

            $message = new \stdClass();
            $message->content = "timeout test {$timeout}s";
            $envelope = new Envelope($message);

            // Act
            $transport->send($envelope);

            // 获取但不确认
            $consumer1 = new RedisTransport($connection, $this->serializer);
            $messages1 = $consumer1->get();
            $this->assertCount(1, $messages1);

            // 在超时前尝试获取 - 应该为空
            sleep($timeout - 1);
            $consumer2 = new RedisTransport($connection, $this->serializer);
            $messages2 = $consumer2->get();
            $this->assertEmpty($messages2, "Message should not be available before timeout ({$timeout}s)");

            // 等待超时后
            sleep(2);
            $messages3 = $consumer2->get();
            $this->assertCount(1, $messages3, "Message should be redelivered after timeout ({$timeout}s)");
            $this->assertEquals("timeout test {$timeout}s", $messages3[0]->getMessage()->content);

            // 清理
            $consumer2->ack($messages3[0]);
        }
    }

    public function test_concurrentRedelivery_maintainsConsistency(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        // 发送多个消息
        $messageCount = 5;
        $sentIds = [];
        for ($i = 0; $i < $messageCount; $i++) {
            $message = new \stdClass();
            $message->content = "redelivery test {$i}";
            $message->id = "msg-{$i}";
            $transport->send(new Envelope($message));
            $sentIds[] = "msg-{$i}";
        }

        // Act
        // 多个消费者获取消息但都不确认
        $consumers = [];
        $receivedMessages = [];

        for ($i = 0; $i < 3; $i++) {
            $consumer = new RedisTransport($connection, $this->serializer);
            $messages = [];

            // 每个消费者尝试获取多个消息
            while (true) {
                $batch = $consumer->get();
                if (empty($batch)) {
                    break;
                }
                foreach ($batch as $msg) {
                    $messages[] = $msg;
                }
                if (count($messages) >= 2) {
                    break;
                }
            }

            $consumers[] = $consumer;
            $receivedMessages[] = $messages;
        }

        // 等待重投递
        sleep(2);

        // 新消费者应该能获取所有未确认的消息
        $finalConsumer = new RedisTransport($connection, $this->serializer);
        $redeliveredIds = [];

        while (true) {
            $messages = $finalConsumer->get();
            if (empty($messages)) {
                break;
            }
            foreach ($messages as $msg) {
                $redeliveredIds[] = $msg->getMessage()->id;
                $finalConsumer->ack($msg);
            }
        }

        // Assert
        $this->assertCount($messageCount, $redeliveredIds);
        /** @phpstan-ignore-next-line argument.unresolvableType */
        sort($sentIds);
        /** @phpstan-ignore-next-line argument.unresolvableType */
        sort($redeliveredIds);
        $this->assertEquals($sentIds, $redeliveredIds);
        $this->assertEquals(0, $transport->getMessageCount());
    }

    public function test_mixedDelayedAndRedeliveredMessages(): void
    {
        // Arrange
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 1,
            'claim_interval' => 200,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        // 发送立即消息
        $immediateMessage = new \stdClass();
        $immediateMessage->content = 'immediate';
        $transport->send(new Envelope($immediateMessage));

        // 发送延迟消息（2秒后）
        $delayedMessage = new \stdClass();
        $delayedMessage->content = 'delayed';
        $transport->send(new Envelope($delayedMessage, [new \Symfony\Component\Messenger\Stamp\DelayStamp(2000)]));

        // Act
        // 获取立即消息但不确认（将被重投递）
        $consumer1 = new RedisTransport($connection, $this->serializer);
        $messages1 = $consumer1->get();
        $this->assertCount(1, $messages1);
        $this->assertEquals('immediate', $messages1[0]->getMessage()->content);

        // 等待1.5秒（重投递发生，但延迟消息还未到）
        usleep(1500000);

        // 新消费者应该获取重投递的消息
        $consumer2 = new RedisTransport($connection, $this->serializer);
        $messages2 = $consumer2->get();
        $this->assertCount(1, $messages2);
        $this->assertEquals('immediate', $messages2[0]->getMessage()->content);
        $consumer2->ack($messages2[0]);

        // 再等待0.6秒（延迟消息应该可用了）
        usleep(600000);

        $messages3 = $consumer2->get();
        $this->assertCount(1, $messages3);
        $this->assertEquals('delayed', $messages3[0]->getMessage()->content);
        $consumer2->ack($messages3[0]);

        // Assert
        $this->assertEquals(0, $transport->getMessageCount());
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new PhpSerializer();
    }
}