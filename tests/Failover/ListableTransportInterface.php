<?php

declare(strict_types=1);

namespace Tourze\AsyncMessengerBundle\Tests\Failover;

use Symfony\Component\Messenger\Transport\Receiver\KeepaliveReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * 测试接口，结合了所有必要的接收器接口
 */
interface ListableTransportInterface extends TransportInterface, ListableReceiverInterface, KeepaliveReceiverInterface
{
}
