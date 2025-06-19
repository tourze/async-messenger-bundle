<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;

class MessageRedeliveryTest extends DoctrineIntegrationTestCase
{
    private PhpSerializer $serializer;
    
    public function test_messageRedelivery_afterTimeout(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(2); // 2秒超时

        $message = new \stdClass();
        $message->content = 'will be redelivered';
        $envelope = new Envelope($message);

        // Act - 发送消息
        $transport->send($envelope);

        // 第一次获取消息
        $firstDelivery = $transport->get();
        $this->assertCount(1, $firstDelivery);
        $firstEnvelope = $firstDelivery[0];

        // 记录第一次投递的信息
        $receivedStamp = $firstEnvelope->last(\Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp::class);
        $firstDeliveryId = $receivedStamp->getId();

        // 不确认消息，模拟处理失败
        // 立即尝试再次获取，应该为空（消息被锁定）
        $immediateRetry = $transport->get();
        $this->assertEmpty($immediateRetry);

        // 等待超过重投递超时时间
        sleep(3);

        // Assert - 消息应该可以被重新获取
        $secondDelivery = $transport->get();
        $this->assertCount(1, $secondDelivery);
        $secondEnvelope = $secondDelivery[0];

        // 验证是同一条消息
        $this->assertEquals(
            $message->content,
            $secondEnvelope->getMessage()->content
        );

        // 验证消息ID相同（同一条数据库记录）
        $secondReceivedStamp = $secondEnvelope->last(\Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp::class);
        $this->assertEquals($firstDeliveryId, $secondReceivedStamp->getId());

        // 这次确认消息
        $transport->ack($secondEnvelope);

        // 验证消息已被删除
        $this->assertEquals(0, $transport->getMessageCount());
    }
    
    private function createTransportWithTimeout(int $redeliverTimeout): DoctrineTransport
    {
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => $redeliverTimeout,
        ]);
        
        $connection = new Connection($options, $this->dbalConnection);
        
        return new DoctrineTransport($connection, $this->serializer);
    }
    
    public function test_keepalive_preventsRedelivery(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(2); // 2秒超时

        $message = new \stdClass();
        $message->content = 'kept alive';
        $envelope = new Envelope($message);

        // Act
        $transport->send($envelope);
        $envelopes = $transport->get();
        $this->assertCount(1, $envelopes);
        $receivedEnvelope = $envelopes[0];

        // 在超时前多次调用 keepalive
        for ($i = 0; $i < 3; $i++) {
            sleep(1);
            $transport->keepalive($receivedEnvelope);
        }

        // 总共已经过了3秒，超过了2秒的超时时间
        // 但由于调用了 keepalive，消息不应该被重投递

        // Assert
        $shouldBeEmpty = $transport->get();
        $this->assertEmpty($shouldBeEmpty);

        // 最终确认消息
        $transport->ack($receivedEnvelope);
        $this->assertEquals(0, $transport->getMessageCount());
    }
    
    public function test_multipleRedeliveries_untilSuccess(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(1); // 1秒超时

        $message = new \stdClass();
        $message->content = 'retry multiple times';
        $message->id = uniqid();
        $envelope = new Envelope($message);

        // Act
        $transport->send($envelope);

        $deliveryAttempts = [];
        $maxAttempts = 3;

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            $envelopes = $transport->get();

            if (empty($envelopes)) {
                // 等待重投递
                sleep(2);
                $envelopes = $transport->get();
            }

            $this->assertCount(1, $envelopes);
            $receivedEnvelope = $envelopes[0];

            // 记录投递尝试
            $deliveryAttempts[] = [
                'attempt' => $attempt,
                'message_id' => $receivedEnvelope->getMessage()->id,
                'content' => $receivedEnvelope->getMessage()->content,
            ];

            if ($attempt < $maxAttempts) {
                // 前几次不确认，模拟处理失败
                sleep(2); // 等待超时
            } else {
                // 最后一次确认
                $transport->ack($receivedEnvelope);
            }
        }

        // Assert
        $this->assertCount($maxAttempts, $deliveryAttempts);

        // 验证每次都是同一条消息
        foreach ($deliveryAttempts as $delivery) {
            $this->assertEquals($message->id, $delivery['message_id']);
            $this->assertEquals($message->content, $delivery['content']);
        }

        // 验证消息已被删除
        $this->assertEquals(0, $transport->getMessageCount());
    }
    
    public function test_redeliveryTimeout_perMessage(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(2); // 2秒默认超时

        // 发送多条消息
        $messages = [];
        for ($i = 0; $i < 3; $i++) {
            $message = new \stdClass();
            $message->content = "message_{$i}";
            $messages[] = $message;
            $transport->send(new Envelope($message));
        }

        // Act - 获取所有消息但不确认
        $receivedEnvelopes = [];
        for ($i = 0; $i < 3; $i++) {
            $envelopes = $transport->get();
            $this->assertCount(1, $envelopes);
            $receivedEnvelopes[] = $envelopes[0];
            usleep(500000); // 0.5秒间隔
        }

        // 只对第二条消息调用 keepalive
        sleep(1);
        $transport->keepalive($receivedEnvelopes[1]);

        // 再等待，使第一条和第三条消息超时
        sleep(2);

        // Assert - 应该能重新获取第一条和第三条消息
        $redelivered = [];
        /** @phpstan-ignore-next-line */
        while ($envelopes = $transport->get()) {
            $redelivered[] = $envelopes[0]->getMessage()->content;
            $transport->ack($envelopes[0]);
        }

        $this->assertCount(2, $redelivered);
        $this->assertContains('message_0', $redelivered);
        $this->assertContains('message_2', $redelivered);
        $this->assertNotContains('message_1', $redelivered); // 这条被 keepalive 了

        // 确认第二条消息
        $transport->ack($receivedEnvelopes[1]);

        // 验证所有消息都已处理
        $this->assertEquals(0, $transport->getMessageCount());
    }
    
    public function test_immediateRedelivery_notPossible(): void
    {
        // Arrange
        $transport = $this->createTransportWithTimeout(3600); // 1小时超时

        $message = new \stdClass();
        $message->content = 'locked message';
        $transport->send(new Envelope($message));

        // Act
        $firstGet = $transport->get();
        $this->assertCount(1, $firstGet);

        // 立即尝试多次获取
        $attempts = [];
        for ($i = 0; $i < 5; $i++) {
            $attempts[] = $transport->get();
            usleep(100000); // 100ms
        }

        // Assert - 所有尝试都应该失败
        foreach ($attempts as $attempt) {
            $this->assertEmpty($attempt);
        }

        // 清理
        $transport->ack($firstGet[0]);
    }
    
    public function test_redeliveryAfterDatabaseReconnection(): void
    {
        // Arrange
        $shortTimeout = 2;
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => $shortTimeout,
        ]);

        // 第一个连接
        $connection1 = new Connection($options, $this->dbalConnection);
        $transport1 = new DoctrineTransport($connection1, $this->serializer);

        $message = new \stdClass();
        $message->content = 'survive reconnection';
        $transport1->send(new Envelope($message));

        // Act - 第一个连接获取消息
        $envelopes = $transport1->get();
        $this->assertCount(1, $envelopes);

        // 模拟连接断开（不确认消息）
        unset($transport1, $connection1);

        // 等待超时
        sleep($shortTimeout + 1);

        // 新连接
        $connection2 = new Connection($options, $this->dbalConnection);
        $transport2 = new DoctrineTransport($connection2, $this->serializer);

        // Assert - 新连接应该能获取到消息
        $redelivered = $transport2->get();
        $this->assertCount(1, $redelivered);
        $this->assertEquals('survive reconnection', $redelivered[0]->getMessage()->content);

        // 确认消息
        $transport2->ack($redelivered[0]);
        $this->assertEquals(0, $transport2->getMessageCount());
    }
    
    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new PhpSerializer();
    }
}