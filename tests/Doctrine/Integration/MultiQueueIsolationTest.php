<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;

class MultiQueueIsolationTest extends DoctrineIntegrationTestCase
{
    private PhpSerializer $serializer;
    
    public function test_differentQueues_completeIsolation(): void
    {
        // Arrange
        $queueA = $this->createTransportForQueue('queue_a');
        $queueB = $this->createTransportForQueue('queue_b');
        $queueC = $this->createTransportForQueue('queue_c');

        // 向不同队列发送消息
        $messagesPerQueue = 3;
        for ($i = 0; $i < $messagesPerQueue; $i++) {
            $messageA = new \stdClass();
            $messageA->content = "queue_a_message_{$i}";
            $messageA->queue = 'a';
            $queueA->send(new Envelope($messageA));

            $messageB = new \stdClass();
            $messageB->content = "queue_b_message_{$i}";
            $messageB->queue = 'b';
            $queueB->send(new Envelope($messageB));

            $messageC = new \stdClass();
            $messageC->content = "queue_c_message_{$i}";
            $messageC->queue = 'c';
            $queueC->send(new Envelope($messageC));
        }

        // Act & Assert - 每个队列只能看到自己的消息
        // Queue A
        $queueAMessages = [];
        /** @phpstan-ignore-next-line */
        while ($envelopes = $queueA->get()) {
            $message = $envelopes[0]->getMessage();
            $queueAMessages[] = $message->content;
            $this->assertEquals('a', $message->queue);
            $queueA->ack($envelopes[0]);
        }
        $this->assertCount($messagesPerQueue, $queueAMessages);

        // Queue B
        $queueBMessages = [];
        /** @phpstan-ignore-next-line */
        while ($envelopes = $queueB->get()) {
            $message = $envelopes[0]->getMessage();
            $queueBMessages[] = $message->content;
            $this->assertEquals('b', $message->queue);
            $queueB->ack($envelopes[0]);
        }
        $this->assertCount($messagesPerQueue, $queueBMessages);

        // Queue C
        $queueCMessages = [];
        /** @phpstan-ignore-next-line */
        while ($envelopes = $queueC->get()) {
            $message = $envelopes[0]->getMessage();
            $queueCMessages[] = $message->content;
            $this->assertEquals('c', $message->queue);
            $queueC->ack($envelopes[0]);
        }
        $this->assertCount($messagesPerQueue, $queueCMessages);

        // 验证没有消息泄露到其他队列
        foreach ($queueAMessages as $content) {
            $this->assertStringStartsWith('queue_a_message_', $content);
        }
        foreach ($queueBMessages as $content) {
            $this->assertStringStartsWith('queue_b_message_', $content);
        }
        foreach ($queueCMessages as $content) {
            $this->assertStringStartsWith('queue_c_message_', $content);
        }

        // 验证所有队列都已清空
        $this->assertEquals(0, $queueA->getMessageCount());
        $this->assertEquals(0, $queueB->getMessageCount());
        $this->assertEquals(0, $queueC->getMessageCount());
    }
    
    private function createTransportForQueue(string $queueName): DoctrineTransport
    {
        $options = array_merge($this->getConnectionOptions(), [
            'queue_name' => $queueName,
        ]);
        
        $connection = new Connection($options, $this->dbalConnection);
        
        return new DoctrineTransport($connection, $this->serializer);
    }
    
    public function test_queueSpecificMessageCount(): void
    {
        // Arrange
        $emailQueue = $this->createTransportForQueue('emails');
        $smsQueue = $this->createTransportForQueue('sms');
        $notificationQueue = $this->createTransportForQueue('notifications');

        // 发送不同数量的消息到各队列
        for ($i = 0; $i < 5; $i++) {
            $email = new \stdClass();
            $email->type = 'email';
            $emailQueue->send(new Envelope($email));
        }

        for ($i = 0; $i < 3; $i++) {
            $sms = new \stdClass();
            $sms->type = 'sms';
            $smsQueue->send(new Envelope($sms));
        }

        for ($i = 0; $i < 7; $i++) {
            $notification = new \stdClass();
            $notification->type = 'notification';
            $notificationQueue->send(new Envelope($notification));
        }

        // Act & Assert - 各队列的消息计数应该独立
        $this->assertEquals(5, $emailQueue->getMessageCount());
        $this->assertEquals(3, $smsQueue->getMessageCount());
        $this->assertEquals(7, $notificationQueue->getMessageCount());

        // 总消息数
        $this->assertEquals(15, $this->getMessageCount());

        // 处理部分消息后再次验证
        $emailEnvelopes = $emailQueue->get();
        $emailQueue->ack($emailEnvelopes[0]);

        $smsEnvelopes = $smsQueue->get();
        $smsQueue->ack($smsEnvelopes[0]);

        $this->assertEquals(4, $emailQueue->getMessageCount());
        $this->assertEquals(2, $smsQueue->getMessageCount());
        $this->assertEquals(7, $notificationQueue->getMessageCount()); // 未变化
    }
    
    public function test_concurrentProcessing_acrossQueues(): void
    {
        // Arrange
        $highPriorityQueue = $this->createTransportForQueue('high_priority');
        $normalQueue = $this->createTransportForQueue('normal');
        $lowPriorityQueue = $this->createTransportForQueue('low_priority');

        // 发送消息
        $queues = [
            'high' => $highPriorityQueue,
            'normal' => $normalQueue,
            'low' => $lowPriorityQueue,
        ];

        foreach ($queues as $priority => $queue) {
            for ($i = 0; $i < 10; $i++) {
                $message = new \stdClass();
                $message->priority = $priority;
                $message->index = $i;
                $queue->send(new Envelope($message));
            }
        }

        // Act - 模拟并发处理
        $processed = [
            'high' => 0,
            'normal' => 0,
            'low' => 0,
        ];

        // 处理所有消息
        $totalProcessed = 0;
        while ($totalProcessed < 30) {
            foreach ($queues as $priority => $queue) {
                $envelopes = $queue->get();
                if (!empty($envelopes)) {
                    $envelope = $envelopes[0];
                    $message = $envelope->getMessage();

                    $this->assertEquals($priority, $message->priority);
                    $processed[$priority]++;

                    $queue->ack($envelope);
                    $totalProcessed++;
                }
            }
        }

        // Assert
        $this->assertEquals(10, $processed['high']);
        $this->assertEquals(10, $processed['normal']);
        $this->assertEquals(10, $processed['low']);

        // 所有队列应该为空
        foreach ($queues as $queue) {
            $this->assertEquals(0, $queue->getMessageCount());
        }
    }
    
    public function test_queueIsolation_withFailures(): void
    {
        // Arrange
        $criticalQueue = $this->createTransportForQueue('critical');
        $regularQueue = $this->createTransportForQueue('regular');

        // 发送消息
        for ($i = 0; $i < 5; $i++) {
            $critical = new \stdClass();
            $critical->type = 'critical';
            $critical->id = $i;
            $criticalQueue->send(new Envelope($critical));

            $regular = new \stdClass();
            $regular->type = 'regular';
            $regular->id = $i;
            $regularQueue->send(new Envelope($regular));
        }

        // Act - 处理 critical 队列的消息，但故意失败一些
        $criticalProcessed = [];
        for ($i = 0; $i < 3; $i++) {
            $envelopes = $criticalQueue->get();
            if (!empty($envelopes)) {
                $envelope = $envelopes[0];
                $message = $envelope->getMessage();

                if ($message->id % 2 === 0) {
                    // 偶数ID的消息"失败"，拒绝它们
                    $criticalQueue->reject($envelope);
                } else {
                    // 奇数ID的消息成功
                    $criticalProcessed[] = $message->id;
                    $criticalQueue->ack($envelope);
                }
            }
        }

        // 正常处理 regular 队列
        $regularProcessed = [];
        /** @phpstan-ignore-next-line */
        while ($envelopes = $regularQueue->get()) {
            $envelope = $envelopes[0];
            $message = $envelope->getMessage();
            $regularProcessed[] = $message->id;
            $regularQueue->ack($envelope);
        }

        // Assert
        // Critical 队列应该只剩下未处理的消息
        $remainingCritical = $criticalQueue->getMessageCount();
        $this->assertEquals(2, $remainingCritical); // 5 - 3 处理的 = 2

        // Regular 队列应该全部处理完成
        $this->assertEquals(0, $regularQueue->getMessageCount());
        $this->assertCount(5, $regularProcessed);

        // 验证队列之间的失败不会相互影响
        sort($regularProcessed);
        $this->assertEquals([0, 1, 2, 3, 4], $regularProcessed);
    }
    
    public function test_dynamicQueueCreation(): void
    {
        // Arrange & Act
        $dynamicQueues = [];
        $queueNames = ['dynamic_1', 'dynamic_2', 'dynamic_3', 'dynamic_4', 'dynamic_5'];

        // 动态创建队列并发送消息
        foreach ($queueNames as $index => $queueName) {
            $queue = $this->createTransportForQueue($queueName);
            $dynamicQueues[$queueName] = $queue;

            // 发送与索引相关数量的消息
            for ($i = 0; $i <= $index; $i++) {
                $message = new \stdClass();
                $message->queue = $queueName;
                $message->index = $i;
                $queue->send(new Envelope($message));
            }
        }

        // Assert - 验证每个队列的消息数量
        $this->assertEquals(1, $dynamicQueues['dynamic_1']->getMessageCount());
        $this->assertEquals(2, $dynamicQueues['dynamic_2']->getMessageCount());
        $this->assertEquals(3, $dynamicQueues['dynamic_3']->getMessageCount());
        $this->assertEquals(4, $dynamicQueues['dynamic_4']->getMessageCount());
        $this->assertEquals(5, $dynamicQueues['dynamic_5']->getMessageCount());

        // 验证总消息数
        $totalExpected = 1 + 2 + 3 + 4 + 5; // = 15
        $this->assertEquals($totalExpected, $this->getMessageCount());

        // 清理所有队列
        foreach ($dynamicQueues as $queueName => $queue) {
            /** @phpstan-ignore-next-line */
            while ($envelopes = $queue->get()) {
                $message = $envelopes[0]->getMessage();
                $this->assertEquals($queueName, $message->queue);
                $queue->ack($envelopes[0]);
            }
        }

        // 验证所有队列都已清空
        foreach ($dynamicQueues as $queue) {
            $this->assertEquals(0, $queue->getMessageCount());
        }
    }
    
    public function test_queueNaming_specialCharacters(): void
    {
        // Arrange - 测试各种特殊命名的队列
        $specialQueues = [
            'queue-with-dash',
            'queue_with_underscore',
            'queue.with.dot',
            'UPPERCASE_QUEUE',
            'mixed_Case_Queue',
            'queue123',
            '123queue',
        ];

        $transports = [];
        foreach ($specialQueues as $queueName) {
            $transport = $this->createTransportForQueue($queueName);
            $transports[$queueName] = $transport;

            // 发送一条消息
            $message = new \stdClass();
            $message->queueName = $queueName;
            $transport->send(new Envelope($message));
        }

        // Act & Assert - 验证每个队列独立工作
        foreach ($transports as $queueName => $transport) {
            $envelopes = $transport->get();
            $this->assertCount(1, $envelopes);

            $message = $envelopes[0]->getMessage();
            $this->assertEquals($queueName, $message->queueName);

            $transport->ack($envelopes[0]);
            $this->assertEquals(0, $transport->getMessageCount());
        }
    }
    
    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new PhpSerializer();
    }
}