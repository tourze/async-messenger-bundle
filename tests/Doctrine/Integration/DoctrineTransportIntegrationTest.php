<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;
use Tourze\AsyncMessengerBundle\Doctrine\DoctrineTransport;
use Tourze\AsyncMessengerBundle\Stamp\DoctrineReceivedStamp;

class DoctrineTransportIntegrationTest extends DoctrineIntegrationTestCase
{
    private DoctrineTransport $transport;
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
        $receivedStamp = $receivedEnvelope->last(DoctrineReceivedStamp::class);
        $this->assertNotNull($receivedStamp);
        $this->assertEquals($transportIdStamp->getId(), $receivedStamp->getId());

        // Act - 确认消息
        $this->transport->ack($receivedEnvelope);

        // Assert - 验证消息已被删除
        $this->assertEquals(0, $this->getMessageCount());
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
        $this->assertEquals(0, $this->getMessageCount());
        $afterReject = $this->transport->get();
        $this->assertEmpty($afterReject);
    }
    
    public function test_keepalive_preventsMessageRedelivery(): void
    {
        // Arrange - 设置短的重投递超时
        $options = array_merge($this->getConnectionOptions(), ['redeliver_timeout' => 2]);
        $connection = new Connection($options, $this->dbalConnection);
        $transport = new DoctrineTransport($connection, $this->serializer);

        $message = new \stdClass();
        $message->content = 'long processing';
        $envelope = new Envelope($message);

        // Act
        $transport->send($envelope);
        $receivedEnvelopes = $transport->get();
        $this->assertCount(1, $receivedEnvelopes);
        $receivedEnvelope = $receivedEnvelopes[0];

        // 模拟长时间处理，期间调用 keepalive
        sleep(1);
        $transport->keepalive($receivedEnvelope);
        sleep(2); // 总共 3 秒，超过了 redeliver_timeout

        // Assert - 消息不应该被重新投递
        $secondGet = $transport->get();
        $this->assertEmpty($secondGet);

        // 确认消息仍然在数据库中（但被锁定，所以 getMessageCount 返回 0）
        $sql = "SELECT COUNT(*) FROM {$this->tableName} WHERE queue_name = ?";
        $count = $this->dbalConnection->fetchOne($sql, [$options['queue_name']]);
        $this->assertEquals(1, $count);
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
    
    public function test_all_returnsAllPendingMessages(): void
    {
        // Arrange
        $messageCount = 3;
        for ($i = 0; $i < $messageCount; $i++) {
            $message = new \stdClass();
            $message->content = "message {$i}";
            $this->transport->send(new Envelope($message));
        }

        // Act
        $allMessages = iterator_to_array($this->transport->all());

        // Assert
        $this->assertCount($messageCount, $allMessages);

        foreach ($allMessages as $index => $envelope) {
            $this->assertInstanceOf(Envelope::class, $envelope);
            $this->assertEquals("message {$index}", $envelope->getMessage()->content);
        }
    }
    
    public function test_find_returnsSpecificMessage(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'findable message';
        $sentEnvelope = $this->transport->send(new Envelope($message));

        $transportIdStamp = $sentEnvelope->last(TransportMessageIdStamp::class);
        $messageId = $transportIdStamp->getId();

        // Act
        $foundEnvelope = $this->transport->find($messageId);

        // Assert
        $this->assertInstanceOf(Envelope::class, $foundEnvelope);
        $this->assertEquals('findable message', $foundEnvelope->getMessage()->content);

        // 验证找不到的消息返回 null
        $notFound = $this->transport->find('non-existent-id');
        $this->assertNull($notFound);
    }
    
    public function test_setup_createsRequiredInfrastructure(): void
    {
        // Arrange
        $newTableName = 'messenger_test_setup';
        $options = array_merge($this->getConnectionOptions(), [
            'table_name' => $newTableName,
            'auto_setup' => false,
        ]);

        $connection = new Connection($options, $this->dbalConnection);
        $transport = new DoctrineTransport($connection, $this->serializer);

        // Assert - 表不存在
        $this->assertFalse($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // Act
        $transport->setup();

        // Assert - 表已创建
        $this->assertTrue($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // 测试表可以正常使用
        $message = new \stdClass();
        $transport->send(new Envelope($message));
        $this->assertEquals(1, $transport->getMessageCount());

        // Cleanup
        $this->dbalConnection->executeStatement("DROP TABLE {$newTableName}");
    }
    
    protected function setUp(): void
    {
        parent::setUp();

        $this->connection = new Connection($this->getConnectionOptions(), $this->dbalConnection);
        $this->serializer = new PhpSerializer();

        $this->transport = new DoctrineTransport($this->connection, $this->serializer);
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
        /** @phpstan-ignore-next-line */
        while ($messages = $this->transport->get()) {
            $receivedCount += count($messages);
            foreach ($messages as $message) {
                $this->transport->ack($message);
            }
        }
        
        $this->assertEquals($messageCount, $receivedCount);
    }
}