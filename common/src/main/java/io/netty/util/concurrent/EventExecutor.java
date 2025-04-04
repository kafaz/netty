/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

/**
 * EventExecutor是一种特殊的EventExecutorGroup，提供了一些便捷方法来判断线程是否在事件循环中执行。
 * <p>
 * EventExecutor除了继承EventExecutorGroup接口外，还提供了一种通用方式来访问事件执行器的各种方法。
 * 它是Netty异步事件处理模型的核心组件之一，负责管理和调度事件的执行。
 * </p>
 * 
 * <h2>主要功能</h2>
 * <ul>
 * <li>管理事件循环线程</li>
 * <li>提供线程执行环境检测</li>
 * <li>创建和管理Promise和Future</li>
 * <li>调度任务执行</li>
 * </ul>
 * 
 * <h2>使用场景</h2>
 * <p>
 * EventExecutor通常在以下场景中使用：
 * <ul>
 * <li>需要确保代码在特定事件循环线程中执行</li>
 * <li>需要创建异步操作的Promise或Future</li>
 * <li>需要调度延迟任务或周期性任务</li>
 * </ul>
 * </p>
 * 
 * @see EventExecutorGroup
 * @see io.netty.channel.EventLoop
 */
public interface EventExecutor extends EventExecutorGroup {

    /**
     * 返回对自身的引用。
     * <p>
     * 由于EventExecutor本身就是一个单一的执行器实例，因此此方法简单地返回自身。
     * 这与EventExecutorGroup的next()方法形成对比，后者会从组中选择一个执行器。
     * </p>
     * 
     * @return 当前EventExecutor实例
     */
    @Override
    EventExecutor next();

    /**
     * 返回作为此EventExecutor父级的EventExecutorGroup引用。
     * <p>
     * 此方法可用于访问创建此执行器的执行器组，从而获取更广泛的上下文信息或资源。
     * 如果此执行器不属于任何组，则可能返回null。
     * </p>
     * 
     * @return 父级执行器组，如果不存在则可能为null
     */
    EventExecutorGroup parent();

    /**
     * 通过以当前线程(Thread.currentThread())作为参数调用inEventLoop(Thread)方法，
     * 来判断当前线程是否是事件循环线程。
     * <p>
     * 这是一个便捷方法，用于快速检查当前执行代码的线程是否是事件循环线程。
     * 这对于确保代码在正确的线程上下文中执行非常重要。
     * </p>
     * 
     * @return 如果当前线程是事件循环线程，则返回true；否则返回false
     */
    boolean inEventLoop();

    /**
     * 判断给定的线程是否在事件循环中执行。
     * <p>
     * 此方法用于确定特定线程是否是事件循环线程。在Netty的线程模型中，
     * 某些操作必须在事件循环线程中执行，而其他操作则必须避免在事件循环线程中执行，
     * 以防止阻塞事件处理。
     * </p>
     * 
     * @param thread 要检查的线程
     * @return 如果给定线程是事件循环线程，则返回true；否则返回false
     */
    boolean inEventLoop(Thread thread);

    /**
     * 创建一个新的Promise实例。
     * <p>
     * Promise是Future的可写版本，允许设置操作结果或失败原因。
     * 通过此方法创建的Promise与当前执行器相关联，这意味着其监听器将在此执行器的
     * 事件循环线程中执行。
     * </p>
     * 
     * @param <V> Promise中结果的类型
     * @return 新创建的Promise实例
     */
    <V> Promise<V> newPromise();

    /**
     * 创建一个新的ProgressivePromise实例。
     * <p>
     * ProgressivePromise扩展了Promise，增加了报告和监听操作进度的能力。
     * 这对于文件传输等可以报告进度的长时间运行操作特别有用。
     * </p>
     * 
     * @param <V> ProgressivePromise中结果的类型
     * @return 新创建的ProgressivePromise实例
     */
    <V> ProgressivePromise<V> newProgressivePromise();

    /**
     * 创建一个已标记为成功的Future实例。
     * <p>
     * 此方法返回的Future已经完成，isSuccess()将返回true。
     * 所有添加到此Future的FutureListener都会被立即通知。
     * 所有阻塞方法的调用也会立即返回，不会阻塞。
     * </p>
     * 
     * @param <V>    Future中结果的类型
     * @param result 要设置为Future结果的值
     * @return 已成功完成的Future实例
     */
    <V> Future<V> newSucceededFuture(V result);

    /**
     * 创建一个已标记为失败的Future实例。
     * <p>
     * 此方法返回的Future已经完成，但isSuccess()将返回false。
     * 所有添加到此Future的FutureListener都会被立即通知。
     * 所有阻塞方法的调用也会立即返回，不会阻塞。
     * 调用cause()方法将返回指定的失败原因。
     * </p>
     * 
     * @param <V>   Future中结果的类型
     * @param cause Future的失败原因
     * @return 已失败完成的Future实例
     */
    <V> Future<V> newFailedFuture(Throwable cause);
}
