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

### 自动传输注册

该 Bundle 会自动注册以下传输：
- `async_doctrine`：基于 Doctrine 的数据库消息队列传输
- `async_redis`：基于 Redis 的高性能消息队列传输

安装 Bundle 后，这些传输会使用合理的默认值自动配置。

默认的失败传输设置为 `async_doctrine` 以确保可靠的消息恢复。

### 环境变量

Bundle 会以最简配置自动注册两个传输：

```bash
# 启用/禁用自动传输注册（默认：true）
ASYNC_MESSENGER_AUTO_CONFIGURE=true
```

传输使用简单的 DSN 注册：
- `async_doctrine`：`async-doctrine://`
- `async_redis`：`async-redis://`

所有详细配置（表名、队列名、超时等）都由传输工厂内部处理，并提供合理的默认值。

### 使用传输

安装后，传输会自动在您的 messenger 配置中可用：

```yaml
# config/packages/messenger.yaml
framework:
    messenger:
        routing:
            'App\Message\EmailMessage': async_doctrine
            'App\Message\NotificationMessage': async_redis
```

### 自定义传输配置

如果需要自定义配置，可以在 `messenger.yaml` 中覆盖传输：

```yaml
framework:
    messenger:
        transports:
            # 覆盖默认的 async_doctrine 传输
            async_doctrine:
                dsn: 'async-doctrine://'
                options:
                    table_name: 'my_custom_messages'
                    queue_name: 'priority'
                    redeliver_timeout: 7200
```

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