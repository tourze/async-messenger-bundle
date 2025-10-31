# AsyncMessengerBundle

[English](README.md) | [中文](README.zh-CN.md)

[![PHP](https://img.shields.io/badge/php-%5E8.1-blue)](https://www.php.net/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Symfony](https://img.shields.io/badge/symfony-%5E6.4-green)](https://symfony.com/)

[![CI](https://github.com/tourze/php-monorepo/workflows/CI/badge.svg)](https://github.com/tourze/php-monorepo/actions)
[![codecov](https://codecov.io/gh/tourze/php-monorepo/graph/badge.svg?flag=async-messenger-bundle)](https://codecov.io/gh/tourze/php-monorepo)

为 Symfony Messenger 提供 Redis 和 Doctrine 传输的 Symfony 包，支持延迟和定时消息。

## 目录

- [功能特性](#功能特性)
- [安装](#安装)
- [配置](#配置)
  - [自动传输注册](#自动传输注册)
  - [环境变量](#环境变量)
- [使用传输](#使用传输)
  - [基础配置](#基础配置)
  - [自定义传输配置](#自定义传输配置)
- [使用方法](#使用方法)
- [高级用法](#高级用法)
  - [性能调优](#性能调优)
  - [监控和调试](#监控和调试)
- [系统要求](#系统要求)
- [许可证](#许可证)

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

## 使用传输

### 基础配置

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

## 高级用法

### 性能调优

对于高吞吐量应用，可以考虑以下配置选项：

```yaml
framework:
    messenger:
        transports:
            async_redis:
                dsn: 'async-redis://'
                options:
                    max_queue_size: 10000
                    redeliver_timeout: 3600
            
            async_doctrine:
                dsn: 'async-doctrine://'
                options:
                    table_name: 'messenger_messages'
                    queue_name: 'high_priority'
                    redeliver_timeout: 1800
```

### 监控和调试

Bundle 通过戳记和异常提供内置监控功能：

```php
use Tourze\AsyncMessengerBundle\Stamp\FailoverSourceStamp;

// 检查消息由哪个传输处理
$sourceStamp = $envelope->last(FailoverSourceStamp::class);
if ($sourceStamp) {
    echo "消息由以下传输处理: " . $sourceStamp->getTransportName();
}
```

## 系统要求

- PHP 8.1+
- Symfony 6.4+
- Redis 扩展 4.3.0+（用于 Redis 传输）
- Doctrine ORM（用于 Doctrine 传输）

## 许可证

此包采用 MIT 许可证发布。详情请参阅 [LICENSE](LICENSE) 文件。