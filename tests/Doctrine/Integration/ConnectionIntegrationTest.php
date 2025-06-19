<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Doctrine\Integration;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Tourze\AsyncMessengerBundle\Doctrine\Connection;

class ConnectionIntegrationTest extends DoctrineIntegrationTestCase
{
    private Connection $connection;
    private PhpSerializer $serializer;
    
    public function test_send_savesMessageToDatabase(): void
    {
        // Arrange
        $message = new \stdClass();
        $message->content = 'test message';
        $envelope = new Envelope($message);
        $encodedEnvelope = $this->serializer->encode($envelope);

        // Act
        $messageId = $this->connection->send(
            $encodedEnvelope['body'],
            $encodedEnvelope['headers'] ?? []
        );

        // Assert
        $this->assertEquals(1, $this->getMessageCount());

        $messages = $this->connection->findAll();
        $this->assertCount(1, $messages);

        $savedMessage = $messages[0];
        $this->assertEquals($encodedEnvelope['body'], $savedMessage['body']);
        $this->assertIsArray($savedMessage['headers']);
        $this->assertEquals([], $savedMessage['headers']); // PhpSerializer doesn't include headers
        $this->assertEquals('test_queue', $savedMessage['queue_name']);
        $this->assertNull($savedMessage['delivered_at']);
    }
    
    public function test_send_withDelay_setsCorrectAvailableAt(): void
    {
        // Arrange
        $message = new \stdClass();
        $envelope = new Envelope($message, [new DelayStamp(5000)]); // 5 秒延迟
        $encodedEnvelope = $this->serializer->encode($envelope);

        $beforeSend = new \DateTime();

        // Act
        $this->connection->send(
            $encodedEnvelope['body'],
            $encodedEnvelope['headers'] ?? [],
            5000
        );

        // Assert
        // 使用原始查询，因为 findAll 只返回当前可用的消息
        $sql = "SELECT * FROM {$this->tableName} WHERE queue_name = ?";
        $messages = $this->dbalConnection->fetchAllAssociative($sql, ['test_queue']);
        $this->assertCount(1, $messages);
        $savedMessage = $messages[0];

        $this->assertNotNull($savedMessage['available_at']);
        $availableAt = new \DateTime($savedMessage['available_at']);
        $expectedAvailableAt = (clone $beforeSend)->modify('+5 seconds');

        // 允许 1 秒的误差
        $this->assertLessThanOrEqual(1, abs($availableAt->getTimestamp() - $expectedAvailableAt->getTimestamp()));
    }
    
    public function test_get_returnsNextAvailableMessage(): void
    {
        // Arrange
        $message1 = ['body' => 'message 1', 'headers' => '{}'];
        $message2 = ['body' => 'message 2', 'headers' => '{}'];
        $message3 = ['body' => 'message 3', 'headers' => '{}', 'available_at' => new \DateTime('+1 hour')];

        $id1 = $this->insertTestMessage($message1);
        $id2 = $this->insertTestMessage($message2);
        $id3 = $this->insertTestMessage($message3);

        // Act
        $doctrineEnvelope1 = $this->connection->get();
        $doctrineEnvelope2 = $this->connection->get();
        $doctrineEnvelope3 = $this->connection->get();

        // Assert
        $this->assertNotNull($doctrineEnvelope1);
        $this->assertNotNull($doctrineEnvelope2);
        $this->assertNull($doctrineEnvelope3); // message 3 还不可用

        $this->assertEquals('message 1', $doctrineEnvelope1['body']);
        $this->assertEquals('message 2', $doctrineEnvelope2['body']);
    }
    
    public function test_get_marksMessageAsDelivered(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);

        // Act
        $doctrineEnvelope = $this->connection->get();

        // Assert
        $this->assertNotNull($doctrineEnvelope);

        $message = $this->connection->find($id);
        $this->assertNotNull($message['delivered_at']);

        // 验证其他消费者无法获取已投递的消息
        $anotherEnvelope = $this->connection->get();
        $this->assertNull($anotherEnvelope);
    }
    
    public function test_ack_removesMessageFromDatabase(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);
        $doctrineEnvelope = $this->connection->get();

        // Act
        $this->connection->ack($id);

        // Assert
        $this->assertMessageNotInDatabase($id);
    }
    
    public function test_reject_removesMessageFromDatabase(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);
        $doctrineEnvelope = $this->connection->get();

        // Act
        $this->connection->reject($id);

        // Assert
        $this->assertMessageNotInDatabase($id);
    }
    
    public function test_keepalive_updatesDeliveredAt(): void
    {
        // Arrange
        $id = $this->insertTestMessage(['body' => 'test message']);
        $doctrineEnvelope = $this->connection->get();

        $originalDeliveredAt = $this->connection->find($id)['delivered_at'];
        sleep(1); // 确保时间差异

        // Act
        $this->connection->keepalive($id);

        // Assert
        $updatedMessage = $this->connection->find($id);
        $this->assertNotEquals($originalDeliveredAt, $updatedMessage['delivered_at']);
        $this->assertGreaterThan(
            new \DateTime($originalDeliveredAt),
            new \DateTime($updatedMessage['delivered_at'])
        );
    }
    
    public function test_getMessageCount_returnsCorrectCount(): void
    {
        // Arrange
        // Connection 是为 'test_queue' 配置的
        $this->insertTestMessage(['queue_name' => 'test_queue']);
        $this->insertTestMessage(['queue_name' => 'test_queue']);
        $this->insertTestMessage(['queue_name' => 'other_queue']);

        // Act & Assert
        // getMessageCount 只计算当前队列的消息
        $this->assertEquals(2, $this->connection->getMessageCount());

        // 验证总消息数（使用基类方法）
        $this->assertEquals(3, $this->getMessageCount());
        $this->assertEquals(2, $this->getMessageCount('test_queue'));
        $this->assertEquals(1, $this->getMessageCount('other_queue'));
    }
    
    public function test_setup_createsTableIfNotExists(): void
    {
        // Arrange
        $newTableName = 'messenger_messages_new';
        $options = array_merge($this->getConnectionOptions(), ['table_name' => $newTableName]);
        $connection = new Connection($options, $this->dbalConnection);

        // 确保表不存在
        $this->assertFalse($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // Act
        $connection->setup();

        // Assert
        $this->assertTrue($this->dbalConnection->createSchemaManager()->tablesExist([$newTableName]));

        // Cleanup
        $this->dbalConnection->executeStatement("DROP TABLE {$newTableName}");
    }
    
    protected function setUp(): void
    {
        parent::setUp();

        $this->connection = new Connection($this->getConnectionOptions(), $this->dbalConnection);
        $this->serializer = new PhpSerializer();
    }
    
    public function test_multipleQueues_isolatesMessages(): void
    {
        // Arrange
        $this->insertTestMessage(['body' => 'queue1_msg1', 'queue_name' => 'queue1']);
        $this->insertTestMessage(['body' => 'queue1_msg2', 'queue_name' => 'queue1']);
        $this->insertTestMessage(['body' => 'queue2_msg1', 'queue_name' => 'queue2']);
        
        // Act
        $connection1 = new Connection(
            array_merge($this->getConnectionOptions(), ['queue_name' => 'queue1']),
            $this->dbalConnection
        );
        $connection2 = new Connection(
            array_merge($this->getConnectionOptions(), ['queue_name' => 'queue2']),
            $this->dbalConnection
        );
        
        $envelope1_1 = $connection1->get();
        $envelope1_2 = $connection1->get();
        $envelope1_3 = $connection1->get();
        
        $envelope2_1 = $connection2->get();
        $envelope2_2 = $connection2->get();
        
        // Assert
        $this->assertNotNull($envelope1_1);
        $this->assertNotNull($envelope1_2);
        $this->assertNull($envelope1_3); // queue1 只有 2 条消息
        
        $this->assertNotNull($envelope2_1);
        $this->assertNull($envelope2_2); // queue2 只有 1 条消息
        
        $this->assertEquals('queue1_msg1', $envelope1_1['body']);
        $this->assertEquals('queue1_msg2', $envelope1_2['body']);
        $this->assertEquals('queue2_msg1', $envelope2_1['body']);
    }
    
    public function test_send_returnsUniqueId(): void
    {
        // Arrange
        $message = new \stdClass();
        $envelope = new Envelope($message);
        $encodedEnvelope = $this->serializer->encode($envelope);
        
        // Act
        $id1 = $this->connection->send($encodedEnvelope['body'], $encodedEnvelope['headers'] ?? []);
        $id2 = $this->connection->send($encodedEnvelope['body'], $encodedEnvelope['headers'] ?? []);
        
        // Assert
        $this->assertNotEmpty($id1);
        $this->assertNotEmpty($id2);
        $this->assertNotEquals($id1, $id2);
        
        // 验证返回的 ID 确实存在于数据库中
        $this->assertNotNull($this->connection->find($id1));
        $this->assertNotNull($this->connection->find($id2));
    }
}