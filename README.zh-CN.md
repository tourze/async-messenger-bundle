# AsyncMessengerBundle

为 Symfony Messenger 提供 Redis 和 Doctrine 传输的 Symfony 包，支持延迟和定时消息。

## 功能特性

- **Redis 传输**：使用 Redis 列表和有序集合进行消息队列
- **Doctrine 传输**：基于数据库的消息队列
- **延迟消息**：支持消息延迟发送
- **定时消息**：支持定时消息投递
- **队列管理**：自动队列大小限制和消息回收

## 安装

```bash
composer require tourze/async-messenger-bundle
```

## 配置

该包会自动配置 Redis 和 Doctrine 传输，无需 YAML 配置。

### 默认队列名称

- Redis 普通队列：`async_messages`
- Redis 延迟队列：`async_messages_delayed`
- Doctrine 数据表：`messenger_messages`

## 使用方法

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
        // 立即发送消息
        $this->bus->dispatch(new MyMessage());
        
        // 延迟 5 秒发送消息
        $this->bus->dispatch(
            new MyMessage(),
            [new DelayStamp(5000)]
        );
    }
}
```

## 系统要求

- PHP 8.1+
- Symfony 6.4+
- Redis 扩展 4.3.0+（用于 Redis 传输）
- Doctrine ORM（用于 Doctrine 传输）