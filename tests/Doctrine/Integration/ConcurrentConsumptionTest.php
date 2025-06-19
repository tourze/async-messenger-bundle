<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;

class ConcurrentConsumptionTest extends DoctrineIntegrationTestCase
{
    private PhpSerializer $serializer;
    
    public function test_multipleConsumers_processMessagesConcurrently(): void
    {
        // Arrange - 创建多个消息
        $messageCount = 20;
        $producer = $this->createTransport('producer');

        for ($i = 0; $i < $messageCount; $i++) {
            $message = new \stdClass();
            $message->content = "message_{$i}";
            $message->index = $i;
            $producer->send(new Envelope($message));
        }

        // Act - 创建多个消费者并发消费
        $consumer1 = $this->createTransport('consumer1');
        $consumer2 = $this->createTransport('consumer2');
        $consumer3 = $this->createTransport('consumer3');

        $processedMessages = [
            'consumer1' => [],
            'consumer2' => [],
            'consumer3' => [],
        ];

        // 模拟并发消费
        $totalProcessed = 0;
        while ($totalProcessed < $messageCount) {
            // 每个消费者尝试获取一条消息
            $consumers = [
                'consumer1' => $consumer1,
                'consumer2' => $consumer2,
                'consumer3' => $consumer3,
            ];

            foreach ($consumers as $name => $consumer) {
                $envelopes = $consumer->get();
                if (!empty($envelopes)) {
                    $envelope = $envelopes[0];
                    $message = $envelope->getMessage();
                    $processedMessages[$name][] = $message->index;

                    // 模拟处理时间
                    usleep(10000); // 10ms

                    $consumer->ack($envelope);
                    $totalProcessed++;
                }
            }
        }

        // Assert
        // 1. 所有消息都被处理了
        $this->assertEquals($messageCount, $totalProcessed);

        // 2. 没有消息被重复处理
        $allProcessedIndexes = array_merge(
            $processedMessages['consumer1'],
            $processedMessages['consumer2'],
            $processedMessages['consumer3']
        );
        $this->assertCount($messageCount, $allProcessedIndexes);
        $this->assertEquals($messageCount, count(array_unique($allProcessedIndexes)));

        // 3. 每个消费者都处理了一些消息（负载均衡）
        foreach ($processedMessages as $consumerName => $messages) {
            $this->assertNotEmpty($messages, "Consumer {$consumerName} should have processed at least one message");
        }

        // 4. 数据库中没有剩余消息
        $this->assertEquals(0, $producer->getMessageCount());
    }
    
    /**
     * 创建独立的 transport 实例，模拟多个消费者
     */
    private function createTransport(string $consumerId = 'default'): DoctrineTransport
    {
        $options = array_merge($this->getConnectionOptions(), [
            'consumer_id' => $consumerId, // 用于调试
        ]);
        
        $connection = new Connection($options, $this->dbalConnection);
        
        return new DoctrineTransport($connection, $this->serializer);
    }
    
    public function test_concurrentGet_doesNotReturnSameMessage(): void
    {
        // Arrange
        $transport = $this->createTransport();

        // 发送一条消息
        $message = new \stdClass();
        $message->content = 'single message';
        $transport->send(new Envelope($message));

        // Act - 创建多个连接同时获取
        $connection1 = new Connection($this->getConnectionOptions(), $this->dbalConnection);
        $connection2 = new Connection($this->getConnectionOptions(), $this->dbalConnection);

        // 模拟并发获取
        $envelope1 = $connection1->get();
        $envelope2 = $connection2->get();

        // Assert
        // 只有一个连接能获取到消息
        $this->assertTrue(
            ($envelope1 !== null && $envelope2 === null) ||
            ($envelope1 === null && $envelope2 !== null),
            'Only one connection should get the message'
        );
    }
    
    public function test_slowConsumer_doesNotBlockOthers(): void
    {
        // Arrange
        $messageCount = 10;
        $producer = $this->createTransport();

        for ($i = 0; $i < $messageCount; $i++) {
            $message = new \stdClass();
            $message->content = "message_{$i}";
            $producer->send(new Envelope($message));
        }

        // Act
        $slowConsumer = $this->createTransport('slow');
        $fastConsumer = $this->createTransport('fast');

        // 慢消费者获取一条消息但不立即确认
        $slowEnvelopes = $slowConsumer->get();
        $this->assertCount(1, $slowEnvelopes);
        $slowMessage = $slowEnvelopes[0]->getMessage()->content;

        // 快消费者应该能够获取并处理其他消息
        $fastProcessedCount = 0;
        $fastProcessedMessages = [];

        /** @phpstan-ignore-next-line */
        while ($envelopes = $fastConsumer->get()) {
            $envelope = $envelopes[0];
            $fastProcessedMessages[] = $envelope->getMessage()->content;
            $fastConsumer->ack($envelope);
            $fastProcessedCount++;

            if ($fastProcessedCount >= 5) {
                break; // 处理5条后停止
            }
        }

        // Assert
        $this->assertEquals(5, $fastProcessedCount);
        $this->assertNotContains($slowMessage, $fastProcessedMessages); // 慢消费者的消息不应被快消费者处理

        // 最后慢消费者确认消息
        $slowConsumer->ack($slowEnvelopes[0]);

        // 验证剩余消息数量
        $this->assertEquals($messageCount - 6, $producer->getMessageCount()); // 6 = 1(slow) + 5(fast)
    }
    
    public function test_connectionFailure_doesNotLoseMessages(): void
    {
        // Arrange
        $transport = $this->createTransport();

        $message = new \stdClass();
        $message->content = 'important message';
        $transport->send(new Envelope($message));

        // Act - 消费者1获取消息但"崩溃"（不调用ack）
        $consumer1 = $this->createTransport('consumer1');
        $envelopes = $consumer1->get();
        $this->assertCount(1, $envelopes);

        // 模拟消费者崩溃 - 不调用 ack，连接被释放
        unset($consumer1);

        // 设置较短的重投递超时以加快测试
        $options = array_merge($this->getConnectionOptions(), ['redeliver_timeout' => 1]);
        $connection2 = new Connection($options, $this->dbalConnection);
        $consumer2 = new DoctrineTransport($connection2, $this->serializer);

        // 立即尝试获取 - 应该失败因为消息还在被"处理"
        $immediateResult = $consumer2->get();
        $this->assertEmpty($immediateResult);

        // 等待重投递超时
        sleep(2);

        // Assert - 消息应该可以被重新获取
        $redeliveredEnvelopes = $consumer2->get();
        $this->assertCount(1, $redeliveredEnvelopes);
        $this->assertEquals('important message', $redeliveredEnvelopes[0]->getMessage()->content);

        // 确认消息
        $consumer2->ack($redeliveredEnvelopes[0]);
        $this->assertEquals(0, $consumer2->getMessageCount());
    }
    
    public function test_highThroughput_maintainsMessageIntegrity(): void
    {
        // Arrange - 大量消息
        $messageCount = 100;
        $producer = $this->createTransport();

        $sentMessages = [];
        for ($i = 0; $i < $messageCount; $i++) {
            $message = new \stdClass();
            $message->id = uniqid('msg_', true);
            $message->index = $i;
            $sentMessages[$message->id] = $message;

            $producer->send(new Envelope($message));
        }

        // Act - 创建多个消费者快速处理
        $consumers = [];
        for ($i = 0; $i < 5; $i++) {
            $consumers[] = $this->createTransport("consumer_{$i}");
        }

        $processedMessages = [];
        $processedCount = 0;

        // 并发消费直到所有消息处理完成
        while ($processedCount < $messageCount) {
            $hasProcessed = false;

            foreach ($consumers as $consumer) {
                $envelopes = $consumer->get();
                if (!empty($envelopes)) {
                    $envelope = $envelopes[0];
                    $message = $envelope->getMessage();

                    // 记录处理的消息
                    $processedMessages[$message->id] = $message;

                    // 立即确认
                    $consumer->ack($envelope);
                    $processedCount++;
                    $hasProcessed = true;
                }
            }

            // 如果没有消费者处理消息，短暂等待
            if (!$hasProcessed) {
                usleep(1000); // 1ms
            }
        }

        // Assert
        // 1. 所有消息都被处理
        $this->assertCount($messageCount, $processedMessages);

        // 2. 没有消息丢失或重复
        foreach ($sentMessages as $id => $sentMessage) {
            $this->assertArrayHasKey($id, $processedMessages);
            $this->assertEquals($sentMessage->index, $processedMessages[$id]->index);
        }

        // 3. 数据库为空
        $this->assertEquals(0, $producer->getMessageCount());
    }
    
    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new PhpSerializer();
    }
}