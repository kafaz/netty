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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * EventExecutorGroup负责通过其next()方法提供EventExecutor实例。
 * <p>
 * 除此之外，EventExecutorGroup还负责管理EventExecutor的生命周期，并允许以全局方式关闭它们。
 * 该接口扩展了Java标准库的ScheduledExecutorService，增加了Netty特有的事件处理和并发控制功能。
 * </p>
 * 
 * <h2>主要职责</h2>
 * <ul>
 *   <li>管理多个EventExecutor实例</li>
 *   <li>提供EventExecutor的选择策略</li>
 *   <li>控制EventExecutor的生命周期</li>
 *   <li>支持任务调度和执行</li>
 * </ul>
 * 
 * <h2>在Reactor模式中的角色</h2>
 * <p>
 * 在Netty的Reactor模型中，EventExecutorGroup通常表示一组事件处理器，例如NioEventLoopGroup。
 * 在多Reactor模型中，通常会有两个EventExecutorGroup：一个用于接受连接（boss组），另一个用于处理I/O（worker组）。
 * </p>
 * 
 * @see EventExecutor
 * @see io.netty.channel.EventLoopGroup
 */
public interface EventExecutorGroup extends ScheduledExecutorService, Iterable<EventExecutor> {

    /**
     * 如果此EventExecutorGroup管理的所有EventExecutor都正在优雅关闭或已经关闭，则返回true。
     * <p>
     * 此方法用于检查关闭过程是否已经开始，可以作为应用程序关闭逻辑的一部分。
     * 当调用shutdownGracefully()方法后，此方法将开始返回true，直到所有执行器完全终止。
     * </p>
     * 
     * @return 如果所有执行器都在关闭中或已关闭，则返回true；否则返回false
     */
    boolean isShuttingDown();

    /**
     * shutdownGracefully(long, long, TimeUnit)方法的快捷版本，使用合理的默认值。
     * <p>
     * 此方法使用预设的静默期和超时值，启动执行器的优雅关闭过程。
     * 调用此方法后，执行器将停止接受新任务，等待现有任务完成，然后终止。
     * </p>
     * 
     * @return 终止Future对象，当所有执行器完全终止时，此Future将得到通知
     */
    Future<?> shutdownGracefully();

    /**
     * 表示调用者希望执行器关闭的信号。一旦调用此方法，isShuttingDown()开始返回true，
     * 执行器准备关闭自身。
     * <p>
     * 与shutdown()不同，优雅关闭确保在关闭前的"静默期"（通常为几秒钟）内不提交任务。
     * 如果在静默期内提交了任务，则保证接受该任务，并且静默期将重新开始。
     * </p>
     * <p>
     * 这种机制确保执行器可以安全地终止，而不会意外丢弃已提交但尚未执行的任务。
     * 如果在指定的超时时间内无法完成所有任务，执行器将强制关闭。
     * </p>
     * 
     * @param quietPeriod 静默期，如文档中所述
     * @param timeout 等待执行器完全关闭的最大时间，无论静默期内是否提交了任务
     * @param unit quietPeriod和timeout的时间单位
     * 
     * @return 终止Future对象，当所有执行器完全终止时，此Future将得到通知
     */
    Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit);

    /**
     * 返回一个Future对象，当此EventExecutorGroup管理的所有EventExecutor终止时，
     * 该Future将收到通知。
     * <p>
     * 此方法返回的Future可用于等待所有执行器完全终止，这对于确保应用程序干净退出很有用。
     * </p>
     * 
     * @return 当所有执行器终止时将完成的Future对象
     */
    Future<?> terminationFuture();

    /**
     * @deprecated 请使用shutdownGracefully(long, long, TimeUnit)或shutdownGracefully()代替。
     */
    @Override
    @Deprecated
    void shutdown();

    /**
     * @deprecated 请使用shutdownGracefully(long, long, TimeUnit)或shutdownGracefully()代替。
     */
    @Override
    @Deprecated
    List<Runnable> shutdownNow();

    /**
     * 返回此EventExecutorGroup管理的一个EventExecutor实例。
     * <p>
     * 此方法使用内部选择策略（通常是轮询或最小负载）来选择一个执行器，
     * 这对于负载均衡非常重要。多次调用此方法可能会返回不同的EventExecutor实例。
     * </p>
     * 
     * @return 由此EventExecutorGroup管理的一个EventExecutor实例
     */
    EventExecutor next();

    /**
     * 返回包含此EventExecutorGroup管理的所有EventExecutor的迭代器。
     * <p>
     * 此方法使EventExecutorGroup可迭代，允许遍历所有执行器，例如，
     * 查询状态或执行管理操作。
     * </p>
     * 
     * @return EventExecutor实例的迭代器
     */
    @Override
    Iterator<EventExecutor> iterator();

    /**
     * 提交一个任务以供执行，并返回代表该任务的Future。
     * <p>
     * 提交的任务将由next()方法选择的执行器执行。返回的Future将在任务完成时得到通知。
     * </p>
     * 
     * @param task 要提交的任务
     * @return 代表待处理任务完成的Future
     */
    @Override
    Future<?> submit(Runnable task);

    /**
     * 提交一个任务以供执行，并返回代表该任务的Future。
     * <p>
     * 提交的任务将由next()方法选择的执行器执行。当任务成功完成时，返回的Future将包含指定的结果值。
     * </p>
     * 
     * @param task 要提交的任务
     * @param result 任务成功完成后Future将返回的结果
     * @return 代表待处理任务完成的Future65                                             
     */
    @Override
    <T> Future<T> submit(Runnable task, T result);

    /**
     * 提交一个返回值的任务以供执行，并返回代表该任务的Future。
     * <p>
     * 提交的任务将由next()方法选择的执行器执行。当任务成功完成时，返回的Future将包含任务的返回值。
     * </p>
     * 
     * @param task 要提交的任务
     * @return 代表待处理任务完成的Future，任务的返回值作为Future的结果
     */
    @Override
    <T> Future<T> submit(Callable<T> task);

    /**
     * 创建并执行在给定延迟后运行的一次性任务。
     * <p>
     * 任务将由next()方法选择的执行器在指定的延迟后执行一次。
     * </p>
     * 
     * @param command 要执行的任务
     * @param delay 从现在开始延迟执行的时间
     * @param unit delay参数的时间单位
     * @return 可用于提取结果或取消的ScheduledFuture
     */
    @Override
    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    /**
     * 创建并执行在给定延迟后运行的一次性任务。
     * <p>
     * 任务将由next()方法选择的执行器在指定的延迟后执行一次。
     * 任务完成后，其结果可通过返回的ScheduledFuture获取。
     * </p>
     * 
     * @param callable 要执行的任务
     * @param delay 从现在开始延迟执行的时间
     * @param unit delay参数的时间单位
     * @return 可用于提取结果或取消的ScheduledFuture
     */
    @Override
    <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit);

    /**
     * 创建并执行一个周期性任务，该任务在给定的初始延迟后首次启用，然后以给定的周期执行。
     * <p>
     * 任务将由next()方法选择的执行器按固定频率执行。
     * 如果任务的执行时间超过了其周期，则后续执行可能会延迟，但不会同时执行。
     * </p>
     * 
     * @param command 要执行的任务
     * @param initialDelay 首次执行前的延迟时间
     * @param period 连续执行之间的周期
     * @param unit initialDelay和period参数的时间单位
     * @return 可用于取消任务的ScheduledFuture
     */
    @Override
    ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

    /**
     * 创建并执行一个周期性任务，该任务在给定的初始延迟后首次启用，然后在一次执行终止和下一次执行开始之间具有给定延迟的情况下重复执行。
     * <p>
     * 任务将由next()方法选择的执行器周期性执行。
     * 与scheduleAtFixedRate不同，此方法保证各个执行之间的延迟，而不是固定的执行频率。
     * </p>
     * 
     * @param command 要执行的任务
     * @param initialDelay 首次执行前的延迟时间
     * @param delay 一次执行结束到下一次执行开始之间的延迟
     * @param unit initialDelay和delay参数的时间单位
     * @return 可用于取消任务的ScheduledFuture
     */
    @Override
    ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit);
}