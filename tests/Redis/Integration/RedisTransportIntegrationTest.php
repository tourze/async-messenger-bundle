<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Redis\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Redis\Connection;
use Tourze\AsyncMessengerBundle\Redis\RedisTransport;
use Tourze\AsyncMessengerBundle\Stamp\RedisReceivedStamp;

class RedisTransportIntegrationTest extends RedisIntegrationTestCase
{
    private RedisTransport $transport;
    private Connection $connection;
    private PhpSerializer $serializer;
    
    public function test_sendAndReceive_completeMessageLifecycle(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'test content';
        $message->id = uniqid();
        $envelope = new Envelope($message);

        // Act - 发送消息
        $sentEnvelope = $this->transport->send($envelope);

        // Assert - 验证发送结果
        $this->assertInstanceOf(Envelope::class, $sentEnvelope);
        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $this->assertNotNull($transportIdStamp);
        $this->assertNotEmpty($transportIdStamp->getId());

        // Act - 接收消息
        $receivedEnvelopes = $this->transport->get();

        // Assert - 验证接收结果
        $this->assertCount(1, $receivedEnvelopes);
        $receivedEnvelope = $receivedEnvelopes[0];

        $this->assertInstanceOf(Envelope::class, $receivedEnvelope);
        $this->assertEquals($message->content, $receivedEnvelope->getMessage()->content);
        $this->assertEquals($message->id, $receivedEnvelope->getMessage()->id);

        // 验证 stamps
        $receivedStamp = $receivedEnvelope->last(RedisReceivedStamp::class);
        $this->assertNotNull($receivedStamp);
        $this->assertEquals($transportIdStamp->getId(), $receivedStamp->getId());

        // Act - 确认消息
        $this->transport->ack($receivedEnvelope);

        // Assert - 验证消息已被处理
        $this->assertEquals(0, $this->transport->getMessageCount());
    }
    
    public function test_sendWithDelay_delaysMessageDelivery(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'delayed message';
        $delayInSeconds = 2;
        $envelope = new Envelope($message, [new DelayStamp($delayInSeconds * 1000)]);

        // Act - 发送延迟消息
        $this->transport->send($envelope);

        // Assert - 立即获取应该返回空
        $immediateResult = $this->transport->get();
        $this->assertEmpty($immediateResult);

        // 等待延迟时间
        sleep($delayInSeconds + 1);

        // Assert - 延迟后应该能获取到消息
        $delayedResult = $this->transport->get();
        $this->assertCount(1, $delayedResult);
        $this->assertEquals('delayed message', $delayedResult[0]->getMessage()->content);
    }
    
    public function test_reject_removesMessageWithoutProcessing(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'to be rejected';
        $envelope = new Envelope($message);

        // Act
        $this->transport->send($envelope);
        $receivedEnvelopes = $this->transport->get();
        $this->assertCount(1, $receivedEnvelopes);

        $this->transport->reject($receivedEnvelopes[0]);

        // Assert
        $this->assertEquals(0, $this->transport->getMessageCount());
        $afterReject = $this->transport->get();
        $this->assertEmpty($afterReject);
    }
    
    public function test_keepalive_preventsMessageRedelivery(): void
    {
        // Arrange - 设置短的重投递超时
        $options = array_merge($this->getConnectionOptions(), [
            'redeliver_timeout' => 0.5,
            'claim_interval' => 100,
        ]);
        $connection = new Connection($this->redis, $options);
        $transport = new RedisTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'long processing';
        $envelope = new Envelope($message);

        // Act
        $transport->send($envelope);
        $receivedEnvelopes = $transport->get();
        $this->assertCount(1, $receivedEnvelopes);
        $receivedEnvelope = $receivedEnvelopes[0];

        // 模拟长时间处理，期间调用 keepalive
        usleep(300000); // 0.3秒
        $transport->keepalive($receivedEnvelope);
        usleep(400000); // 再0.4秒，总共 0.7 秒，超过了 redeliver_timeout

        // Assert - 在 list-based 实现中，get() 会触发重投递检查
        // keepalive 更新了 timestamp，所以消息不会被重投递
        $secondGet = $transport->get();
        $this->assertEmpty($secondGet);

        // 清理
        $transport->ack($receivedEnvelope);
    }
    
    public function test_getMessageCount_returnsCorrectCount(): void
    {
        // Arrange
        $messages = [];
        for ($i = 0; $i < 5; $i++) {
            $message = new \stdClass();
            $message->content = "message {$i}";
            $messages[] = new Envelope($message);
        }

        // Act - 发送消息
        foreach ($messages as $envelope) {
            $this->transport->send($envelope);
        }

        // Assert
        $this->assertEquals(5, $this->transport->getMessageCount());

        // Act - 接收并确认部分消息
        $received = $this->transport->get();
        $this->transport->ack($received[0]);

        // Assert
        $this->assertEquals(4, $this->transport->getMessageCount());
    }
    
    public function test_multipleGet_returnsAllPendingMessages(): void
    {
        // Arrange
        $messageCount = 3;
        for ($i = 0; $i < $messageCount; $i++) {
            $message = new \stdClass();
            $message->content = "message {$i}";
            $this->transport->send(new Envelope($message));
        }

        // Act - 获取所有消息
        $allMessages = [];
        while (true) {
            $messages = $this->transport->get();
            if (empty($messages)) {
                break;
            }
            foreach ($messages as $msg) {
                $allMessages[] = $msg;
            }
            if (count($allMessages) >= $messageCount) {
                break;
            }
        }

        // Assert
        $this->assertCount($messageCount, $allMessages);

        // 验证消息内容（注意：Redis 使用 LIFO 顺序）
        $contents = array_map(function($envelope) {
            return $envelope->getMessage()->content;
        }, $allMessages);
        
        $this->assertContains("message 0", $contents);
        $this->assertContains("message 1", $contents);
        $this->assertContains("message 2", $contents);
        
        // 清理
        foreach ($allMessages as $msg) {
            $this->transport->ack($msg);
        }
    }
    
    public function test_messageWithId_canBeProcessed(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'findable message';
        $sentEnvelope = $this->transport->send(new Envelope($message));

        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $messageId = $transportIdStamp->getId();

        // Act - 获取消息并验证 ID
        $messages = $this->transport->get();
        $this->assertCount(1, $messages);
        $receivedEnvelope = $messages[0];
        
        // Assert
        $receivedStamp = $receivedEnvelope->last(RedisReceivedStamp::class);
        $this->assertNotNull($receivedStamp);
        $this->assertEquals($messageId, $receivedStamp->getId());
        $this->assertEquals('findable message', $receivedEnvelope->getMessage()->content);
        
        // 清理
        $this->transport->ack($receivedEnvelope);
    }
    
    public function test_setup_doesNotThrowError(): void
    {
        // Redis transport 的 setup 是一个空操作，但应该不会抛出错误
        // Act & Assert
        $this->transport->setup();
        $this->assertTrue(true); // 如果没有异常，测试通过
    }
    
    protected function setUp(): void
    {
        parent::setUp();

        $this->connection = new Connection($this->redis, $this->getConnectionOptions());
        $this->serializer = new PhpSerializer();

        $this->transport = new RedisTransport($this->connection, $this->serializer);
    }
    
    public function test_multipleSenders_canSendConcurrently(): void
    {
        // Arrange
        $messageCount = 10;
        $envelopes = [];
        
        for ($i = 0; $i < $messageCount; $i++) {
            $message = new \stdClass();
            $message->content = "concurrent message {$i}";
            $envelopes[] = new Envelope($message);
        }
        
        // Act - 发送所有消息
        $sentIds = [];
        foreach ($envelopes as $envelope) {
            $sentEnvelope = $this->transport->send($envelope);
            $stamp = $sentEnvelope->last(TransportMessageIdStamp::class);
            $sentIds[] = $stamp->getId();
        }
        
        // Assert
        $this->assertCount($messageCount, array_unique($sentIds)); // 所有 ID 都是唯一的
        $this->assertEquals($messageCount, $this->transport->getMessageCount());
        
        // 验证所有消息都可以被接收
        $receivedCount = 0;
        while (true) {
            $messages = $this->transport->get();
            if (empty($messages)) {
                break;
            }
            $receivedCount += count($messages);
            foreach ($messages as $message) {
                $this->transport->ack($message);
            }
        }
        
        $this->assertEquals($messageCount, $receivedCount);
    }
    
    public function test_delayedAndNormalMessages_processedInCorrectOrder(): void
    {
        // Arrange
        // 发送立即消息
        $immediateMessage = new \stdClass();
        $immediateMessage->content = 'immediate';
        $this->transport->send(new Envelope($immediateMessage));
        
        // 发送延迟消息（1秒后）
        $delayedMessage1 = new \stdClass();
        $delayedMessage1->content = 'delayed 1 second';
        $this->transport->send(new Envelope($delayedMessage1, [new DelayStamp(1000)]));
        
        // 发送另一个立即消息
        $immediateMessage2 = new \stdClass();
        $immediateMessage2->content = 'immediate 2';
        $this->transport->send(new Envelope($immediateMessage2));
        
        // 发送延迟消息（0.5秒后）
        $delayedMessage2 = new \stdClass();
        $delayedMessage2->content = 'delayed 0.5 second';
        $this->transport->send(new Envelope($delayedMessage2, [new DelayStamp(500)]));
        
        // Act & Assert
        // 立即获取 - 应该得到两个立即消息
        $immediateResults = [];
        while (true) {
            $envelopes = $this->transport->get();
            if (empty($envelopes)) {
                break;
            }
            foreach ($envelopes as $envelope) {
                $immediateResults[] = $envelope->getMessage()->content;
                $this->transport->ack($envelope);
            }
        }
        
        $this->assertCount(2, $immediateResults);
        $this->assertContains('immediate', $immediateResults);
        $this->assertContains('immediate 2', $immediateResults);
        
        // 等待0.6秒
        usleep(600000);
        
        // 应该得到0.5秒的延迟消息
        $delayedResults1 = $this->transport->get();
        $this->assertCount(1, $delayedResults1);
        $this->assertEquals('delayed 0.5 second', $delayedResults1[0]->getMessage()->content);
        $this->transport->ack($delayedResults1[0]);
        
        // 等待另外0.5秒
        usleep(500000);
        
        // 应该得到1秒的延迟消息
        $delayedResults2 = $this->transport->get();
        $this->assertCount(1, $delayedResults2);
        $this->assertEquals('delayed 1 second', $delayedResults2[0]->getMessage()->content);
        $this->transport->ack($delayedResults2[0]);
        
        // 确保没有更多消息
        $this->assertEquals(0, $this->transport->getMessageCount());
    }
}