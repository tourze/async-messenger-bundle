# 时区处理说明

## 概述

AsyncMessengerBundle 使用 PHP 应用的默认时区来处理所有时间相关的操作。这确保了消息队列系统与应用的其他部分保持时区一致性。

## 时区设置

### 1. 使用应用默认时区（推荐）

Bundle 会自动使用 PHP 的默认时区设置。你可以在应用启动时设置默认时区：

```php
// 在 public/index.php 或 config/bootstrap.php 中
date_default_timezone_set('Asia/Shanghai');
```

或在 `php.ini` 中设置：

```ini
date.timezone = "Asia/Shanghai"
```

### 2. Symfony 配置

在 Symfony 中，你也可以通过框架配置设置时区：

```yaml
# config/packages/framework.yaml
framework:
    default_timezone: 'Asia/Shanghai'
```

## 注意事项

### 数据库存储

- Doctrine 会使用应用的时区将 `DateTimeImmutable` 对象转换为数据库的 datetime 格式
- 读取时，会将数据库的 datetime 值转换回使用应用时区的 `DateTimeImmutable` 对象
- 确保数据库服务器和 PHP 应用使用相同的时区，或者使用 `TIMESTAMP` 类型（自动处理时区）

### 分布式系统

如果你的应用运行在多个服务器上：

1. **统一时区**：确保所有服务器使用相同的时区设置
2. **使用 UTC**：在分布式环境中，建议统一使用 UTC：
   ```php
   date_default_timezone_set('UTC');
   ```
3. **显示时转换**：在向用户显示时间时再转换到用户的本地时区

### 延迟消息

延迟消息的处理时间基于应用的时区。例如，如果你设置了 5 分钟的延迟：

```php
$bus->dispatch(
    new MyMessage(),
    [new DelayStamp(300000)] // 300秒 = 5分钟
);
```

消息将在应用时区的 5 分钟后可用。

## 最佳实践

1. **一致性**：在整个应用中使用一致的时区设置
2. **文档化**：在项目文档中明确说明使用的时区
3. **监控**：定期检查服务器时间和时区设置
4. **测试**：在不同时区下测试延迟消息功能

## 故障排查

如果遇到时间相关的问题：

1. 检查 PHP 时区设置：
   ```php
   echo date_default_timezone_get();
   ```

2. 检查数据库时区：
   ```sql
   -- MySQL
   SELECT @@global.time_zone, @@session.time_zone;
   
   -- PostgreSQL
   SHOW TIMEZONE;
   ```

3. 确保所有服务器时间同步（使用 NTP）

4. 查看消息表中的时间戳，确认它们符合预期
