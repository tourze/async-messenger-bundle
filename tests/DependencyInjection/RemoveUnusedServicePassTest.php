<?php

namespace Tourze\AsyncMessengerBundle\Tests\DependencyInjection;

use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Tourze\AsyncContracts\AsyncMessageInterface;
use Tourze\AsyncMessengerBundle\DependencyInjection\RemoveUnusedServicePass;

class RemoveUnusedServicePassTest extends TestCase
{
    private ContainerBuilder $container;
    private RemoveUnusedServicePass $compilerPass;

    public function test_process_removesAsyncMessageServices(): void
    {
        // 创建一个实现 AsyncMessageInterface 的测试类
        $testAsyncMessageClass = new class implements AsyncMessageInterface {};
        $className = get_class($testAsyncMessageClass);

        // 添加服务定义
        $definition = new Definition($className);
        $this->container->setDefinition('test.async.message', $definition);

        // 验证服务存在
        $this->assertTrue($this->container->hasDefinition('test.async.message'));

        // 执行编译器通道
        $this->compilerPass->process($this->container);

        // 验证服务被移除
        $this->assertFalse($this->container->hasDefinition('test.async.message'));
    }

    public function test_process_keepsNonAsyncMessageServices(): void
    {
        // 创建一个普通类
        $testClass = new class {};
        $className = get_class($testClass);

        // 添加服务定义
        $definition = new Definition($className);
        $this->container->setDefinition('test.normal.service', $definition);

        // 验证服务存在
        $this->assertTrue($this->container->hasDefinition('test.normal.service'));

        // 执行编译器通道
        $this->compilerPass->process($this->container);

        // 验证服务保留
        $this->assertTrue($this->container->hasDefinition('test.normal.service'));
    }

    public function test_process_handlesEmptyClassDefinition(): void
    {
        // 添加没有类名的服务定义
        $definition = new Definition();
        $this->container->setDefinition('test.empty.service', $definition);

        // 验证服务存在
        $this->assertTrue($this->container->hasDefinition('test.empty.service'));

        // 执行编译器通道，不应该抛出异常
        $this->compilerPass->process($this->container);

        // 验证服务保留
        $this->assertTrue($this->container->hasDefinition('test.empty.service'));
    }

    public function test_process_handlesNonExistentClass(): void
    {
        // 添加不存在的类的服务定义
        $definition = new Definition('NonExistentClass');
        $this->container->setDefinition('test.nonexistent.service', $definition);

        // 验证服务存在
        $this->assertTrue($this->container->hasDefinition('test.nonexistent.service'));

        // 执行编译器通道，不应该抛出异常
        $this->compilerPass->process($this->container);

        // 验证服务保留（因为类不存在，跳过处理）
        $this->assertTrue($this->container->hasDefinition('test.nonexistent.service'));
    }

    protected function setUp(): void
    {
        $this->container = new ContainerBuilder();
        $this->compilerPass = new RemoveUnusedServicePass();
    }
}