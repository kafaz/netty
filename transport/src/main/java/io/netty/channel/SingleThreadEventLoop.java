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
package io.netty.channel;

import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.UnstableApi;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * 单线程事件循环的抽象基类，它在单个线程中执行所有提交的任务。
 * <p>
 * 这个类是 {@link EventLoop} 的实现，负责执行任务和处理 IO 事件，所有操作都在同一个线程中进行。
 * 它继承自 {@link SingleThreadEventExecutor} 并实现了 {@link EventLoop} 接口，提供了在单线程环境下
 * 执行任务的基础设施，同时增加了与通道（Channel）相关的特定功能。
 * </p>
 * <p>
 * 单线程事件循环模型是 Netty 的核心设计之一，它保证了所有对特定通道的操作都在同一个线程中执行，
 * 从而避免了多线程并发问题，简化了编程模型。
 * </p>
 * 
 * @see EventLoop
 * @see SingleThreadEventExecutor
 * @see EventLoopGroup
 */
public abstract class SingleThreadEventLoop extends SingleThreadEventExecutor implements EventLoop {

    /**
     * 默认的最大待处理任务数。
     * <p>
     * 此值是通过系统属性 "io.netty.eventLoop.maxPendingTasks" 配置的，
     * 如果没有设置，则取 16 和 Integer.MAX_VALUE 中的较大值。
     * </p>
     * <p>
     * 这个参数控制事件循环中允许排队的最大任务数，超过此限制的任务提交会被拒绝。
     * </p>
     */
    protected static final int DEFAULT_MAX_PENDING_TASKS = Math.max(16,
            SystemPropertyUtil.getInt("io.netty.eventLoop.maxPendingTasks", Integer.MAX_VALUE));

    /**
     * 存储在事件循环迭代结束时运行的任务队列。
     * <p>
     * 这些任务通过 {@link #executeAfterEventLoopIteration(Runnable)} 方法添加，
     * 并在每次事件循环迭代结束时执行。
     * </p>
     */
    private final Queue<Runnable> tailTasks;

    /**
     * 创建一个新的单线程事件循环实例。
     * 
     * @param parent         父事件循环组，用于管理此事件循环
     * @param threadFactory  创建执行线程的工厂
     * @param addTaskWakesUp 添加任务时是否唤醒事件循环
     */
    protected SingleThreadEventLoop(EventLoopGroup parent, ThreadFactory threadFactory, boolean addTaskWakesUp) {
        this(parent, threadFactory, addTaskWakesUp, DEFAULT_MAX_PENDING_TASKS, RejectedExecutionHandlers.reject());
    }

    /**
     * 创建一个新的单线程事件循环实例。
     * 
     * @param parent         父事件循环组，用于管理此事件循环
     * @param executor       用于执行任务的执行器
     * @param addTaskWakesUp 添加任务时是否唤醒事件循环
     */
    protected SingleThreadEventLoop(EventLoopGroup parent, Executor executor, boolean addTaskWakesUp) {
        this(parent, executor, addTaskWakesUp, DEFAULT_MAX_PENDING_TASKS, RejectedExecutionHandlers.reject());
    }

    /**
     * 创建一个新的单线程事件循环实例，指定最大待处理任务数和拒绝执行处理器。
     * 
     * @param parent                   父事件循环组，用于管理此事件循环
     * @param threadFactory            创建执行线程的工厂
     * @param addTaskWakesUp           添加任务时是否唤醒事件循环
     * @param maxPendingTasks          允许的最大待处理任务数
     * @param rejectedExecutionHandler 当任务队列已满时使用的拒绝执行处理器
     */
    protected SingleThreadEventLoop(EventLoopGroup parent, ThreadFactory threadFactory,
            boolean addTaskWakesUp, int maxPendingTasks,
            RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, threadFactory, addTaskWakesUp, maxPendingTasks, rejectedExecutionHandler);
        tailTasks = newTaskQueue(maxPendingTasks);
    }

    /**
     * 创建一个新的单线程事件循环实例，指定最大待处理任务数和拒绝执行处理器。
     * 
     * @param parent                   父事件循环组，用于管理此事件循环
     * @param executor                 用于执行任务的执行器
     * @param addTaskWakesUp           添加任务时是否唤醒事件循环
     * @param maxPendingTasks          允许的最大待处理任务数
     * @param rejectedExecutionHandler 当任务队列已满时使用的拒绝执行处理器
     */
    protected SingleThreadEventLoop(EventLoopGroup parent, Executor executor,
            boolean addTaskWakesUp, int maxPendingTasks,
            RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, addTaskWakesUp, maxPendingTasks, rejectedExecutionHandler);
        tailTasks = newTaskQueue(maxPendingTasks);
    }

    /**
     * 创建一个新的单线程事件循环实例，指定任务队列和尾部任务队列。
     * 
     * @param parent                   父事件循环组，用于管理此事件循环
     * @param executor                 用于执行任务的执行器
     * @param addTaskWakesUp           添加任务时是否唤醒事件循环
     * @param taskQueue                用于存储常规任务的队列
     * @param tailTaskQueue            用于存储尾部任务的队列
     * @param rejectedExecutionHandler 当任务队列已满时使用的拒绝执行处理器
     */
    protected SingleThreadEventLoop(EventLoopGroup parent, Executor executor,
            boolean addTaskWakesUp, Queue<Runnable> taskQueue, Queue<Runnable> tailTaskQueue,
            RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, addTaskWakesUp, taskQueue, rejectedExecutionHandler);
        tailTasks = ObjectUtil.checkNotNull(tailTaskQueue, "tailTaskQueue");
    }

    /**
     * 返回此事件循环的父事件循环组。
     * 
     * @return 父事件循环组
     */
    @Override
    public EventLoopGroup parent() {
        return (EventLoopGroup) super.parent();
    }

    /**
     * 返回由父事件循环组分配的下一个事件循环。
     * 
     * @return 下一个事件循环
     */
    @Override
    public EventLoop next() {
        return (EventLoop) super.next();
    }

    /**
     * 将通道注册到此事件循环，返回一个默认的通道Promise。
     * 
     * @param channel 要注册的通道
     * @return 代表注册操作的通道未来结果
     */
    @Override
    public ChannelFuture register(Channel channel) {
        return register(new DefaultChannelPromise(channel, this));
    }

    /**
     * 将通道注册到此事件循环，使用提供的Promise。
     * <p>
     * 此方法负责将{@link Channel}注册到当前的{@link SingleThreadEventLoop}实例上。注册过程是异步的，
     * 操作的结果通过传入的{@link ChannelPromise}参数通知调用者。
     * <p>
     * 实际的注册操作由Channel的unsafe()方法返回的{@link io.netty.channel.Channel.Unsafe}实例处理，
     * 这是Netty内部的低级API，不应该被应用程序直接使用。
     * <p>
     * 注册完成后，该Channel将由此EventLoop管理，所有Channel相关的I/O操作都将在此EventLoop的线程上执行，
     * 确保线程安全性。
     * 
     * @param promise 用于表示注册操作结果的Promise。当注册成功时，promise将被设置为成功；
     *                如果注册过程中发生异常，promise将被设置为失败，并携带相应的异常信息。
     *                通过此promise，调用者可以异步获取注册操作的结果。
     * 
     * @return 代表注册操作的通道未来结果，实际上就是传入的promise参数。
     *         可以通过返回的ChannelFuture添加监听器，以便在注册操作完成时得到通知。
     * 
     * @throws NullPointerException 如果Promise为null，将立即抛出此异常。
     *                              异常由{@link io.netty.util.internal.ObjectUtil#checkNotNull}方法检测并抛出。
     * 
     * @see io.netty.channel.Channel.Unsafe#register(io.netty.channel.EventLoop,
     *      io.netty.channel.ChannelPromise)
     * @see io.netty.channel.ChannelFuture
     * @see io.netty.channel.ChannelPromise
     */
    @Override
    public ChannelFuture register(final ChannelPromise promise) {
        // 验证传入的promise参数不为null，如果为null则抛出NullPointerException异常
        // 这是一个防御性编程的实践，确保输入参数的有效性
        ObjectUtil.checkNotNull(promise, "promise");

        // 从promise中获取关联的Channel对象，然后调用其unsafe()方法获取Channel的内部操作接口
        // unsafe()是Netty内部API，提供直接操作Channel的低级方法
        // 调用register方法将当前EventLoop与Channel关联，并传入promise用于异步通知操作结果
        // 这是实际执行注册操作的核心代码，将I/O操作委托给Channel的unsafe实现
        promise.channel().unsafe().register(this, promise);

        // 返回传入的promise对象，它也实现了ChannelFuture接口
        // 调用者可以通过返回的ChannelFuture添加监听器或等待操作完成
        // 注册操作是异步的，此方法立即返回，实际注册结果将通过promise通知
        return promise;
    }

    /**
     * 将通道注册到此事件循环，使用提供的通道和Promise。
     * 
     * @param channel 要注册的通道
     * @param promise 用于表示注册操作结果的Promise
     * @return 代表注册操作的通道未来结果
     * @throws NullPointerException 如果通道或Promise为 null
     * @deprecated 使用 {@link #register(ChannelPromise)} 代替
     */
    @Deprecated
    @Override
    public ChannelFuture register(final Channel channel, final ChannelPromise promise) {
        ObjectUtil.checkNotNull(promise, "promise");
        ObjectUtil.checkNotNull(channel, "channel");
        channel.unsafe().register(this, promise);
        return promise;
    }

    /**
     * 添加一个任务，在下一次（或当前）事件循环迭代结束时运行一次。
     * <p>
     * 这些任务在每次事件循环迭代的所有常规任务执行完毕后执行。
     * </p>
     *
     * @param task 要添加的任务
     * @throws NullPointerException       如果任务为 null
     * @throws RejectedExecutionException 如果事件循环已关闭或任务队列已满
     */
    public final void executeAfterEventLoopIteration(Runnable task) {
        ObjectUtil.checkNotNull(task, "task");
        if (isShutdown()) {
            reject();
        }

        if (!tailTasks.offer(task)) {
            reject(task);
        }

        if (wakesUpForTask(task)) {
            wakeup(inEventLoop());
        }
    }

    /**
     * 移除之前通过 {@link #executeAfterEventLoopIteration(Runnable)} 添加的任务。
     *
     * @param task 要移除的任务
     * @return 如果任务被成功移除，则为 {@code true}
     * @throws NullPointerException 如果任务为 null
     */
    final boolean removeAfterEventLoopIterationTask(Runnable task) {
        return tailTasks.remove(ObjectUtil.checkNotNull(task, "task"));
    }

    /**
     * 在运行所有常规任务后执行的操作，处理尾部任务队列中的任务。
     * <p>
     * 此方法在每次事件循环迭代结束时调用，用于执行所有在尾部任务队列中的任务。
     * </p>
     */
    @Override
    protected void afterRunningAllTasks() {
        runAllTasksFrom(tailTasks);
    }

    /**
     * 检查事件循环是否有待处理的任务。
     * 
     * @return 如果有常规任务或尾部任务待处理，则为 {@code true}
     */
    @Override
    protected boolean hasTasks() {
        return super.hasTasks() || !tailTasks.isEmpty();
    }

    /**
     * 返回事件循环中待处理的任务总数。
     * 
     * @return 常规任务和尾部任务的总数
     */
    @Override
    public int pendingTasks() {
        return super.pendingTasks() + tailTasks.size();
    }

    /**
     * 返回注册到此事件循环的 {@link Channel} 数量，如果不支持此操作则返回 {@code -1}。
     * <p>
     * 返回值不保证完全准确，应视为尽力而为的结果。
     * </p>
     * 
     * @return 注册的通道数量，或 {@code -1}（如果不支持）
     */
    @UnstableApi
    public int registeredChannels() {
        return -1;
    }

    /**
     * 返回注册到此事件循环的活跃 {@link Channel} 的只读迭代器。
     * <p>
     * 返回值不保证完全准确，应视为尽力而为的结果。此方法应该在事件循环内调用。
     * </p>
     * 
     * @return 活跃通道的只读迭代器
     * @throws UnsupportedOperationException 如果实现不支持此操作
     */
    @UnstableApi
    public Iterator<Channel> registeredChannelsIterator() {
        throw new UnsupportedOperationException("registeredChannelsIterator");
    }

    /**
     * 通道的只读迭代器实现，提供对注册通道的安全遍历。
     * 
     * @param <T> 通道类型
     */
    protected static final class ChannelsReadOnlyIterator<T extends Channel> implements Iterator<Channel> {
        /**
         * 底层通道迭代器
         */
        private final Iterator<T> channelIterator;

        /**
         * 创建一个新的通道只读迭代器。
         * 
         * @param channelIterable 通道集合
         * @throws NullPointerException 如果通道集合为 null
         */
        public ChannelsReadOnlyIterator(Iterable<T> channelIterable) {
            this.channelIterator = ObjectUtil.checkNotNull(channelIterable, "channelIterable").iterator();
        }

        /**
         * 检查迭代器是否有下一个元素。
         * 
         * @return 如果有下一个元素，则为 {@code true}
         */
        @Override
        public boolean hasNext() {
            return channelIterator.hasNext();
        }

        /**
         * 返回迭代器中的下一个通道。
         * 
         * @return 下一个通道
         */
        @Override
        public Channel next() {
            return channelIterator.next();
        }

        /**
         * 此操作不支持，会抛出异常。
         * 
         * @throws UnsupportedOperationException 总是抛出
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        /**
         * 返回一个空的迭代器。
         * 
         * @param <T> 元素类型
         * @return 空迭代器
         */
        @SuppressWarnings("unchecked")
        public static <T> Iterator<T> empty() {
            return (Iterator<T>) EMPTY;
        }

        /**
         * 空迭代器的实现。
         */
        private static final Iterator<Object> EMPTY = new Iterator<Object>() {
            /**
             * 始终返回 false，表示没有下一个元素。
             * 
             * @return 始终为 {@code false}
             */
            @Override
            public boolean hasNext() {
                return false;
            }

            /**
             * 尝试获取下一个元素时抛出异常，因为没有元素。
             * 
             * @return 不会返回值
             * @throws NoSuchElementException 总是抛出
             */
            @Override
            public Object next() {
                throw new NoSuchElementException();
            }

            /**
             * 此操作不支持，会抛出异常。
             * 
             * @throws UnsupportedOperationException 总是抛出
             */
            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }
        };
    }
}
