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
package io.netty.channel.embedded;

import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.DefaultChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.RecyclableArrayList;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * 用于嵌入式场景的 {@link Channel} 实现基类。
 * 
 * <p>EmbeddedChannel 提供了一种在不使用真实网络传输的情况下测试 {@link ChannelHandler} 的机制。
 * 它模拟了一个完整的 {@link Channel} 堆栈，允许应用程序代码直接与 Channel 交互，
 * 而无需建立实际的网络连接或处理底层网络 I/O。</p>
 * 
 * <h3>主要特性</h3>
 * <ul>
 *   <li>支持同步测试 - 无需处理异步回调或多线程问题</li>
 *   <li>提供完整的 {@link ChannelPipeline} 功能</li>
 *   <li>可以检查入站和出站消息队列</li>
 *   <li>支持模拟触发入站和出站事件</li>
 *   <li>内置时钟控制，可以测试基于时间的操作</li>
 * </ul>
 * 
 * <h3>基本用法</h3>
 * <pre>
 * // 创建一个 EmbeddedChannel 并添加要测试的处理器
 * EmbeddedChannel channel = new EmbeddedChannel(
 *     new HttpRequestDecoder(),
 *     new HttpResponseEncoder(),
 *     new MyCustomHandler()
 * );
 * 
 * // 写入入站数据（模拟接收数据）
 * ByteBuf input = Unpooled.wrappedBuffer("GET / HTTP/1.1\r\n\r\n".getBytes());
 * channel.writeInbound(input);
 * 
 * // 读取并验证出站数据（处理器生成的响应）
 * HttpResponse response = channel.readOutbound();
 * assertEquals(HttpResponseStatus.OK, response.status());
 * 
 * // 最终关闭 channel 并释放资源
 * channel.finish();
 * </pre>
 * 
 * <h3>时间控制</h3>
 * <p>EmbeddedChannel 提供了操作事件循环时间的方法，这对于测试定时任务非常有用：</p>
 * <ul>
 *   <li>{@link #runPendingTasks()} - 运行所有挂起的任务</li>
 *   <li>{@link #runScheduledPendingTasks()} - 运行所有已计划的任务</li>
 *   <li>{@link #advanceTimeBy(long, TimeUnit)} - 推进事件循环时钟</li>
 *   <li>{@link #freezeTime()} - 冻结事件循环时钟</li>
 *   <li>{@link #unfreezeTime()} - 解冻事件循环时钟</li>
 * </ul>
 * 
 * <h3>状态检查</h3>
 * <p>EmbeddedChannel 允许检查内部状态：</p>
 * <ul>
 *   <li>{@link #inboundMessages()} - 获取入站消息队列</li>
 *   <li>{@link #outboundMessages()} - 获取出站消息队列</li>
 *   <li>{@link #readInbound()} - 从入站队列读取下一个消息</li>
 *   <li>{@link #readOutbound()} - 从出站队列读取下一个消息</li>
 * </ul>
 * 
 * <h3>资源管理</h3>
 * <p>使用后应调用以下方法之一清理资源：</p>
 * <ul>
 *   <li>{@link #finish()} - 完成处理并检查缓冲区是否有剩余数据</li>
 *   <li>{@link #finishAndReleaseAll()} - 完成处理并释放所有缓冲区中的消息</li>
 *   <li>{@link #releaseInbound()} - 仅释放入站缓冲区中的消息</li>
 *   <li>{@link #releaseOutbound()} - 仅释放出站缓冲区中的消息</li>
 * </ul>
 */
public class EmbeddedChannel extends AbstractChannel {

    private static final SocketAddress LOCAL_ADDRESS = new EmbeddedSocketAddress();
    private static final SocketAddress REMOTE_ADDRESS = new EmbeddedSocketAddress();

    private static final ChannelHandler[] EMPTY_HANDLERS = new ChannelHandler[0];
    private enum State { OPEN, ACTIVE, CLOSED }

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(EmbeddedChannel.class);

    private static final ChannelMetadata METADATA_NO_DISCONNECT = new ChannelMetadata(false);
    private static final ChannelMetadata METADATA_DISCONNECT = new ChannelMetadata(true);

    private final EmbeddedEventLoop loop = new EmbeddedEventLoop();
    private final ChannelFutureListener recordExceptionListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            recordException(future);
        }
    };

    private final ChannelMetadata metadata;
    private final ChannelConfig config;

    private Queue<Object> inboundMessages;
    private Queue<Object> outboundMessages;
    private Throwable lastException;
    private State state;
    private int executingStackCnt;
    private boolean cancelRemainingScheduledTasks;

    /**
     * 创建一个带有 {@link EmbeddedChannelId} 和空管道的新实例。
     */
    public EmbeddedChannel() {
        this(EMPTY_HANDLERS);
    }

    /**
     * 创建一个带有指定 ID 和空管道的新实例。
     *
     * @param channelId 用于标识此通道的 {@link ChannelId}
     */
    public EmbeddedChannel(ChannelId channelId) {
        this(channelId, EMPTY_HANDLERS);
    }

    /**
     * 创建一个新实例，其管道使用指定的处理器初始化。
     *
     * @param handlers 将添加到 {@link ChannelPipeline} 中的 {@link ChannelHandler}
     */
    public EmbeddedChannel(ChannelHandler... handlers) {
        this(EmbeddedChannelId.INSTANCE, handlers);
    }

    /**
     * 创建一个新实例，其管道使用指定的处理器初始化。
     *
     * @param hasDisconnect 如果为 {@code false}，则此 {@link Channel} 将把 {@link #disconnect()} 
     *                     委托给 {@link #close()}，否则为 {@code true}
     * @param handlers 将添加到 {@link ChannelPipeline} 中的 {@link ChannelHandler}
     */
    public EmbeddedChannel(boolean hasDisconnect, ChannelHandler... handlers) {
        this(EmbeddedChannelId.INSTANCE, hasDisconnect, handlers);
    }

    /**
     * 创建一个新实例，其管道使用指定的处理器初始化。
     *
     * @param register 如果为 {@code true}，则此 {@link Channel} 在构造函数中注册到 {@link EventLoop}。
     *                如果为 {@code false}，则用户需要调用 {@link #register()}
     * @param hasDisconnect 如果为 {@code false}，则此 {@link Channel} 将把 {@link #disconnect()}
     *                     委托给 {@link #close()}，否则为 {@code true}
     * @param handlers 将添加到 {@link ChannelPipeline} 中的 {@link ChannelHandler}
     */
    public EmbeddedChannel(boolean register, boolean hasDisconnect, ChannelHandler... handlers) {
        this(EmbeddedChannelId.INSTANCE, register, hasDisconnect, handlers);
    }

    /**
     * 创建一个新实例，其通道 ID 设置为给定 ID，管道使用指定的处理器初始化。
     *
     * @param channelId 用于标识此通道的 {@link ChannelId}
     * @param handlers 将添加到 {@link ChannelPipeline} 中的 {@link ChannelHandler}
     */
    public EmbeddedChannel(ChannelId channelId, ChannelHandler... handlers) {
        this(channelId, false, handlers);
    }

    /**
     * 创建一个新实例，其通道 ID 设置为给定 ID，管道使用指定的处理器初始化。
     *
     * @param channelId 用于标识此通道的 {@link ChannelId}
     * @param hasDisconnect 如果为 {@code false}，则此 {@link Channel} 将把 {@link #disconnect()}
     *                     委托给 {@link #close()}，否则为 {@code true}
     * @param handlers 将添加到 {@link ChannelPipeline} 中的 {@link ChannelHandler}
     */
    public EmbeddedChannel(ChannelId channelId, boolean hasDisconnect, ChannelHandler... handlers) {
        this(channelId, true, hasDisconnect, handlers);
    }

    /**
     * 创建一个新实例，其通道 ID 设置为给定 ID，管道使用指定的处理器初始化。
     *
     * @param channelId 用于标识此通道的 {@link ChannelId}
     * @param register 如果为 {@code true}，则此 {@link Channel} 在构造函数中注册到 {@link EventLoop}。
     *                如果为 {@code false}，则用户需要调用 {@link #register()}
     * @param hasDisconnect 如果为 {@code false}，则此 {@link Channel} 将把 {@link #disconnect()}
     *                     委托给 {@link #close()}，否则为 {@code true}
     * @param handlers 将添加到 {@link ChannelPipeline} 中的 {@link ChannelHandler}
     */
    public EmbeddedChannel(ChannelId channelId, boolean register, boolean hasDisconnect,
                           ChannelHandler... handlers) {
        this(null, channelId, register, hasDisconnect, handlers);
    }

    /**
     * 创建一个新实例，其通道 ID 设置为给定 ID，管道使用指定的处理器初始化。
     *
     * @param parent 此 {@link EmbeddedChannel} 的父 {@link Channel}
     * @param channelId 用于标识此通道的 {@link ChannelId}
     * @param register 如果为 {@code true}，则此 {@link Channel} 在构造函数中注册到 {@link EventLoop}。
     *                如果为 {@code false}，则用户需要调用 {@link #register()}
     * @param hasDisconnect 如果为 {@code false}，则此 {@link Channel} 将把 {@link #disconnect()}
     *                     委托给 {@link #close()}，否则为 {@code true}
     * @param handlers 将添加到 {@link ChannelPipeline} 中的 {@link ChannelHandler}
     */
    public EmbeddedChannel(Channel parent, ChannelId channelId, boolean register, boolean hasDisconnect,
                           final ChannelHandler... handlers) {
        super(parent, channelId);
        metadata = metadata(hasDisconnect);
        config = new DefaultChannelConfig(this);
        setup(register, handlers);
    }

    /**
     * 创建一个新实例，其通道 ID 设置为给定 ID，管道使用指定的处理器初始化。
     *
     * @param channelId 用于标识此通道的 {@link ChannelId}
     * @param hasDisconnect 如果为 {@code false}，则此 {@link Channel} 将把 {@link #disconnect()}
     *                     委托给 {@link #close()}，否则为 {@code true}
     * @param config 将由 {@link #config()} 返回的 {@link ChannelConfig}
     * @param handlers 将添加到 {@link ChannelPipeline} 中的 {@link ChannelHandler}
     */
    public EmbeddedChannel(ChannelId channelId, boolean hasDisconnect, final ChannelConfig config,
                           final ChannelHandler... handlers) {
        super(null, channelId);
        metadata = metadata(hasDisconnect);
        this.config = ObjectUtil.checkNotNull(config, "config");
        setup(true, handlers);
    }

    private static ChannelMetadata metadata(boolean hasDisconnect) {
        return hasDisconnect ? METADATA_DISCONNECT : METADATA_NO_DISCONNECT;
    }

    private void setup(boolean register, final ChannelHandler... handlers) {
        ObjectUtil.checkNotNull(handlers, "handlers");
        ChannelPipeline p = pipeline();
        p.addLast(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                for (ChannelHandler h: handlers) {
                    if (h == null) {
                        break;
                    }
                    pipeline.addLast(h);
                }
            }
        });
        if (register) {
            ChannelFuture future = loop.register(this);
            assert future.isDone();
        }
    }

    /**
     * 在此 {@link Channel} 的 {@link EventLoop} 上注册这个通道。
     * 
     * @throws Exception 如果注册过程中发生错误
     */
    public void register() throws Exception {
        ChannelFuture future = loop.register(this);
        assert future.isDone();
        Throwable cause = future.cause();
        if (cause != null) {
            PlatformDependent.throwException(cause);
        }
    }

    @Override
    protected final DefaultChannelPipeline newChannelPipeline() {
        return new EmbeddedChannelPipeline(this);
    }

    @Override
    public ChannelMetadata metadata() {
        return metadata;
    }

    @Override
    public ChannelConfig config() {
        return config;
    }

    @Override
    public boolean isOpen() {
        return state != State.CLOSED;
    }

    @Override
    public boolean isActive() {
        return state == State.ACTIVE;
    }

    /**
     * 返回包含此 {@link Channel} 接收的所有 {@link Object} 的 {@link Queue}。
     * 
     * @return 包含所有入站消息的队列
     */
    public Queue<Object> inboundMessages() {
        if (inboundMessages == null) {
            inboundMessages = new ArrayDeque<Object>();
        }
        return inboundMessages;
    }

    /**
     * @deprecated 使用 {@link #inboundMessages()}
     */
    @Deprecated
    public Queue<Object> lastInboundBuffer() {
        return inboundMessages();
    }

    /**
     * 返回包含此 {@link Channel} 写入的所有 {@link Object} 的 {@link Queue}。
     * 
     * @return 包含所有出站消息的队列
     */
    public Queue<Object> outboundMessages() {
        if (outboundMessages == null) {
            outboundMessages = new ArrayDeque<Object>();
        }
        return outboundMessages;
    }

    /**
     * @deprecated 使用 {@link #outboundMessages()}
     */
    @Deprecated
    public Queue<Object> lastOutboundBuffer() {
        return outboundMessages();
    }

    /**
     * 从此 {@link Channel} 读取接收到的数据。
     * 
     * @param <T> 期望的消息类型
     * @return 下一个入站消息，如果没有可读数据则返回 {@code null}
     */
    @SuppressWarnings("unchecked")
    public <T> T readInbound() {
        T message = (T) poll(inboundMessages);
        if (message != null) {
            ReferenceCountUtil.touch(message, "Caller of readInbound() will handle the message from this point");
        }
        return message;
    }

    /**
     * 从出站队列读取数据。如果没有可读内容，则可能返回 {@code null}。
     * 
     * @param <T> 期望的消息类型
     * @return 下一个出站消息，如果没有可读数据则返回 {@code null}
     */
    @SuppressWarnings("unchecked")
    public <T> T readOutbound() {
        T message =  (T) poll(outboundMessages);
        if (message != null) {
            ReferenceCountUtil.touch(message, "Caller of readOutbound() will handle the message from this point.");
        }
        return message;
    }

    /**
     * 向此 {@link Channel} 的入站队列写入消息。
     * 
     * <p>这个方法用于模拟通道接收到数据的情况。消息将通过通道管道中的所有入站处理器。</p>
     *
     * @param msgs 要写入的消息
     * @return {@code true} 如果写入操作确实向入站缓冲区添加了内容
     */
    public boolean writeInbound(Object... msgs) {
        ensureOpen();
        if (msgs.length == 0) {
            return isNotEmpty(inboundMessages);
        }

        executingStackCnt++;
        try {
            ChannelPipeline p = pipeline();
            for (Object m : msgs) {
                p.fireChannelRead(m);
            }

            flushInbound(false, voidPromise());
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
        return isNotEmpty(inboundMessages);
    }

    /**
     * 向此 {@link Channel} 的入站写入一条消息，但不刷新它。
     * 此方法在概念上等同于 {@link #write(Object)}。
     *
     * @param msg 要写入的消息
     * @return 表示写入操作结果的 {@link ChannelFuture}
     * 
     * @see #writeOneOutbound(Object)
     */
    public ChannelFuture writeOneInbound(Object msg) {
        return writeOneInbound(msg, newPromise());
    }

    /**
     * 向此 {@link Channel} 的入站写入一条消息，但不刷新它。
     * 此方法在概念上等同于 {@link #write(Object, ChannelPromise)}。
     *
     * @param msg 要写入的消息
     * @param promise 将通知写入操作结果的 {@link ChannelPromise}
     * @return 表示写入操作结果的 {@link ChannelFuture}
     * 
     * @see #writeOneOutbound(Object, ChannelPromise)
     */
    public ChannelFuture writeOneInbound(Object msg, ChannelPromise promise) {
        executingStackCnt++;
        try {
            if (checkOpen(true)) {
                pipeline().fireChannelRead(msg);
            }
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
        return checkException(promise);
    }

    /**
     * 刷新此 {@link Channel} 的入站队列。此方法在概念上等同于 {@link #flush()}。
     *
     * @return 此 {@link EmbeddedChannel} 实例，支持方法链调用
     * 
     * @see #flushOutbound()
     */
    public EmbeddedChannel flushInbound() {
        flushInbound(true, voidPromise());
        return this;
    }

    private ChannelFuture flushInbound(boolean recordException, ChannelPromise promise) {
        executingStackCnt++;
        try {
            if (checkOpen(recordException)) {
                pipeline().fireChannelReadComplete();
                runPendingTasks();
            }
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }

      return checkException(promise);
    }

    /**
     * 向此 {@link Channel} 的出站写入消息。
     * 
     * <p>这个方法用于模拟通道发送数据的情况。消息将通过通道管道中的所有出站处理器。</p>
     *
     * @param msgs 要写入的消息
     * @return 如果写入操作确实向出站缓冲区添加了内容，则返回 {@code true}
     */
    public boolean writeOutbound(Object... msgs) {
        ensureOpen();
        if (msgs.length == 0) {
            return isNotEmpty(outboundMessages);
        }

        executingStackCnt++;
        RecyclableArrayList futures = RecyclableArrayList.newInstance(msgs.length);
        try {
            try {
                for (Object m : msgs) {
                    if (m == null) {
                        break;
                    }
                    futures.add(write(m));
                }

                flushOutbound0();

                int size = futures.size();
                for (int i = 0; i < size; i++) {
                    ChannelFuture future = (ChannelFuture) futures.get(i);
                    if (future.isDone()) {
                        recordException(future);
                    } else {
                        // The write may be delayed to run later by runPendingTasks()
                        future.addListener(recordExceptionListener);
                    }
                }
            } finally {
                executingStackCnt--;
                maybeRunPendingTasks();
            }
            checkException();
            return isNotEmpty(outboundMessages);
        } finally {
            futures.recycle();
        }
    }

    /**
     * 向此 {@link Channel} 的出站写入一条消息，但不刷新它。
     * 此方法在概念上等同于 {@link #write(Object)}。
     *
     * @param msg 要写入的消息
     * @return 表示写入操作结果的 {@link ChannelFuture}
     * 
     * @see #writeOneInbound(Object)
     */
    public ChannelFuture writeOneOutbound(Object msg) {
        return writeOneOutbound(msg, newPromise());
    }

    /**
     * 向此 {@link Channel} 的出站写入一条消息，但不刷新它。
     * 此方法在概念上等同于 {@link #write(Object, ChannelPromise)}。
     *
     * @param msg 要写入的消息
     * @param promise 将通知写入操作结果的 {@link ChannelPromise}
     * @return 表示写入操作结果的 {@link ChannelFuture}
     * 
     * @see #writeOneInbound(Object, ChannelPromise)
     */
    public ChannelFuture writeOneOutbound(Object msg, ChannelPromise promise) {
        executingStackCnt++;
        try {
            if (checkOpen(true)) {
                return write(msg, promise);
            }
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }

        return checkException(promise);
    }

    /**
     * 刷新此 {@link Channel} 的出站队列。此方法在概念上等同于 {@link #flush()}。
     *
     * @return 此 {@link EmbeddedChannel} 实例，支持方法链调用
     * 
     * @see #flushInbound()
     */
    public EmbeddedChannel flushOutbound() {
        executingStackCnt++;
        try {
            if (checkOpen(true)) {
                flushOutbound0();
            }
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
        checkException(voidPromise());
        return this;
    }

    /**
     * 执行出站刷新操作的内部实现。
     * 首先运行所有挂起的任务，然后调用 {@link #flush()}。
     */
    private void flushOutbound0() {
        // We need to call runPendingTasks first as a ChannelOutboundHandler may used eventloop.execute(...) to
        // delay the write on the next eventloop run.
        runPendingTasks();

        flush();
    }

    /**
     * 将此 {@link Channel} 标记为已完成。任何进一步尝试向其写入数据都将失败。
     *
     * @return 如果任何使用的缓冲区中还有内容可读，则返回 {@code true}
     */
    public boolean finish() {
        return finish(false);
    }

    /**
     * 将此 {@link Channel} 标记为已完成，并释放入站和出站缓冲区中的所有挂起消息。
     * 任何进一步尝试向其写入数据都将失败。
     *
     * @return 如果任何使用的缓冲区中还有内容可读，则返回 {@code true}
     */
    public boolean finishAndReleaseAll() {
        return finish(true);
    }

    /**
     * 将此 {@link Channel} 标记为已完成。任何进一步尝试向其写入数据都将失败。
     *
     * @param releaseAll 如果为 {@code true}，则释放入站和出站缓冲区中的所有挂起消息。
     * @return 如果任何使用的缓冲区中还有内容可读，则返回 {@code true}
     */
    private boolean finish(boolean releaseAll) {
        executingStackCnt++;
        try {
            close();
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
        try {
            checkException();
            return isNotEmpty(inboundMessages) || isNotEmpty(outboundMessages);
        } finally {
            if (releaseAll) {
                releaseAll(inboundMessages);
                releaseAll(outboundMessages);
            }
        }
    }

    /**
     * 释放所有缓冲的入站消息，并返回 {@code true} 如果入站缓冲区中有任何消息，否则返回 {@code false}。
     * 
     * @return {@code true} 如果有任何消息被释放，否则 {@code false}
     */
    public boolean releaseInbound() {
        return releaseAll(inboundMessages);
    }

    /**
     * 释放所有缓冲的出站消息，并返回 {@code true} 如果出站缓冲区中有任何消息，否则返回 {@code false}。
     * 
     * @return {@code true} 如果有任何消息被释放，否则 {@code false}
     */
    public boolean releaseOutbound() {
        return releaseAll(outboundMessages);
    }

    /**
     * 释放队列中的所有对象并返回队列是否非空。
     *
     * @param queue 要释放的队列
     * @return 如果队列不为空，则返回 {@code true}
     */
    private static boolean releaseAll(Queue<Object> queue) {
        if (isNotEmpty(queue)) {
            for (;;) {
                Object msg = queue.poll();
                if (msg == null) {
                    break;
                }
                ReferenceCountUtil.release(msg);
            }
            return true;
        }
        return false;
    }

    @Override
    public final ChannelFuture close() {
        return close(newPromise());
    }

    @Override
    public final ChannelFuture disconnect() {
        return disconnect(newPromise());
    }

    @Override
    public final ChannelFuture close(ChannelPromise promise) {
        // We need to call runPendingTasks() before calling super.close() as there may be something in the queue
        // that needs to be run before the actual close takes place.
        executingStackCnt++;
        ChannelFuture future;
        try {
            runPendingTasks();
            future = super.close(promise);

            cancelRemainingScheduledTasks = true;
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
        return future;
    }

    @Override
    public final ChannelFuture disconnect(ChannelPromise promise) {
        executingStackCnt++;
        ChannelFuture future;
        try {
            future = super.disconnect(promise);

            if (!metadata.hasDisconnect()) {
                cancelRemainingScheduledTasks = true;
            }
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
        return future;
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress) {
        executingStackCnt++;
        try {
            return super.bind(localAddress);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        executingStackCnt++;
        try {
            return super.connect(remoteAddress);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        executingStackCnt++;
        try {
            return super.connect(remoteAddress, localAddress);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture deregister() {
        executingStackCnt++;
        try {
            return super.deregister();
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public Channel flush() {
        executingStackCnt++;
        try {
            return super.flush();
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        executingStackCnt++;
        try {
            return super.bind(localAddress, promise);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        executingStackCnt++;
        try {
            return super.connect(remoteAddress, promise);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        executingStackCnt++;
        try {
            return super.connect(remoteAddress, localAddress, promise);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture deregister(ChannelPromise promise) {
        executingStackCnt++;
        try {
            return super.deregister(promise);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public Channel read() {
        executingStackCnt++;
        try {
            return super.read();
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture write(Object msg) {
        executingStackCnt++;
        try {
            return super.write(msg);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        executingStackCnt++;
        try {
            return super.write(msg, promise);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        executingStackCnt++;
        try {
            return super.writeAndFlush(msg);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        executingStackCnt++;
        try {
            return super.writeAndFlush(msg, promise);
        } finally {
            executingStackCnt--;
            maybeRunPendingTasks();
        }
    }

    private static boolean isNotEmpty(Queue<Object> queue) {
        return queue != null && !queue.isEmpty();
    }

    private static Object poll(Queue<Object> queue) {
        return queue != null ? queue.poll() : null;
    }

    /**
     * 当执行堆栈计数为0时运行挂起的任务。
     * 这确保了在多层嵌套的操作完成后才执行挂起的任务。
     */
    private void maybeRunPendingTasks() {
        if (executingStackCnt == 0) {
            runPendingTasks();

            if (cancelRemainingScheduledTasks) {
                // Cancel all scheduled tasks that are left.
                embeddedEventLoop().cancelScheduledTasks();
            }
        }
    }

    /**
     * 运行 {@link EventLoop} 中此 {@link Channel} 的所有挂起任务（包括计划任务）。
     * 
     * <p>这个方法可以用来确保所有提交给通道的 EventLoop 的任务都得到执行，这对于测试异步操作很有用。</p>
     */
    public void runPendingTasks() {
        try {
            embeddedEventLoop().runTasks();
        } catch (Exception e) {
            recordException(e);
        }

        try {
            embeddedEventLoop().runScheduledTasks();
        } catch (Exception e) {
            recordException(e);
        }
    }

    /**
     * 检查此通道是否有任何待处理的任务，这些任务将通过调用 {@link #runPendingTasks()} 执行。
     * 这包括普通任务和截止日期已过期的计划任务。如果此方法返回 {@code false}，
     * 则调用 {@link #runPendingTasks()} 将不执行任何操作。
     *
     * @return 如果有任何待处理的任务，则为 {@code true}，否则为 {@code false}
     */
    public boolean hasPendingTasks() {
        return embeddedEventLoop().hasPendingNormalTasks() ||
                embeddedEventLoop().nextScheduledTask() == 0;
    }

    /**
     * 运行 {@link EventLoop} 中此 {@link Channel} 的所有挂起的计划任务，
     * 并返回下一个计划任务准备运行的 {@code 纳秒}。如果没有其他任务被计划，则返回 {@code -1}。
     * 
     * @return 下一个计划任务准备运行的时间（以纳秒为单位），如果没有计划任务，则返回 {@code -1}
     */
    public long runScheduledPendingTasks() {
        try {
            return embeddedEventLoop().runScheduledTasks();
        } catch (Exception e) {
            recordException(e);
            return embeddedEventLoop().nextScheduledTask();
        }
    }

    /**
     * 记录操作完成时可能出现的异常。
     *
     * @param future 要检查的 {@link ChannelFuture}
     */
    private void recordException(ChannelFuture future) {
        if (!future.isSuccess()) {
            recordException(future.cause());
        }
    }

    /**
     * 记录异常，保存第一个出现的异常，并记录后续异常。
     *
     * @param cause 要记录的异常
     */
    private void recordException(Throwable cause) {
        if (lastException == null) {
            lastException = cause;
        } else {
            logger.warn(
                    "More than one exception was raised. " +
                            "Will report only the first one and log others.", cause);
        }
    }

    /**
     * 将此通道的事件循环的时钟前进给定的持续时间。
     * 任何计划的任务都将提前执行给定的时间（但仍然需要调用 {@link #runScheduledPendingTasks()}）。
     * 
     * @param duration 要前进的时间量
     * @param unit 时间单位
     */
    public void advanceTimeBy(long duration, TimeUnit unit) {
        embeddedEventLoop().advanceTimeBy(unit.toNanos(duration));
    }

    /**
     * 冻结此通道的事件循环的时钟。
     * 任何尚未到期的计划任务都不会在未来的 {@link #runScheduledPendingTasks()} 调用中运行。
     * 当事件循环被冻结时，仍然可以手动 {@link #advanceTimeBy(long, TimeUnit) 前进时间}，
     * 以便计划的任务执行。
     */
    public void freezeTime() {
        embeddedEventLoop().freezeTime();
    }

    /**
     * 解冻已 {@link #freezeTime() 冻结} 的事件循环。
     * 时间将从 {@link #freezeTime()} 停止的点继续：如果一个任务被计划在十分钟后执行，
     * 并且调用了 {@link #freezeTime()}，那么在再次调用此方法后的十分钟内它将运行
     * （假设没有 {@link #advanceTimeBy(long, TimeUnit)} 调用，并且假设在那时使用
     * {@link #runScheduledPendingTasks()} 运行待处理的计划任务）。
     */
    public void unfreezeTime() {
        embeddedEventLoop().unfreezeTime();
    }

    /**
     * Checks for the presence of an {@link Exception}.
     */
    private ChannelFuture checkException(ChannelPromise promise) {
      Throwable t = lastException;
      if (t != null) {
        lastException = null;

        if (promise.isVoid()) {
            PlatformDependent.throwException(t);
        }

        return promise.setFailure(t);
      }

      return promise.setSuccess();
    }

    /**
     * 检查是否存在任何 {@link Throwable}，如果存在则重新抛出它。
     * 
     * <p>这个方法可以用来确保在 EmbeddedChannel 操作期间没有发生任何异常。
     * 如果有异常发生，它将被重新抛出，以便调用代码可以处理它。</p>
     * 
     * @throws RuntimeException 如果在通道操作期间捕获到异常
     */
    public void checkException() {
      checkException(voidPromise());
    }

    /**
     * Returns {@code true} if the {@link Channel} is open and records optionally
     * an {@link Exception} if it isn't.
     */
    private boolean checkOpen(boolean recordException) {
        if (!isOpen()) {
          if (recordException) {
              recordException(new ClosedChannelException());
          }
          return false;
      }

      return true;
    }

    private EmbeddedEventLoop embeddedEventLoop() {
        if (isRegistered()) {
            return (EmbeddedEventLoop) super.eventLoop();
        }

        return loop;
    }

    /**
     * 确保 {@link Channel} 是打开的，如果未打开则抛出异常。
     * 
     * @throws RuntimeException 如果通道已关闭
     */
    protected final void ensureOpen() {
        if (!checkOpen(true)) {
            checkException();
        }
    }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        return loop instanceof EmbeddedEventLoop;
    }

    @Override
    protected SocketAddress localAddress0() {
        return isActive()? LOCAL_ADDRESS : null;
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return isActive()? REMOTE_ADDRESS : null;
    }

    @Override
    protected void doRegister() throws Exception {
        state = State.ACTIVE;
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        // NOOP
    }

    @Override
    protected void doDisconnect() throws Exception {
        if (!metadata.hasDisconnect()) {
            doClose();
        }
    }

    @Override
    protected void doClose() throws Exception {
        state = State.CLOSED;
    }

    @Override
    protected void doBeginRead() throws Exception {
        // NOOP
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
        return new EmbeddedUnsafe();
    }

    @Override
    public Unsafe unsafe() {
        return ((EmbeddedUnsafe) super.unsafe()).wrapped;
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        for (;;) {
            Object msg = in.current();
            if (msg == null) {
                break;
            }

            ReferenceCountUtil.retain(msg);
            handleOutboundMessage(msg);
            in.remove();
        }
    }

    /**
     * 处理每个出站消息。
     *
     * @param msg 要处理的出站消息
     * @see #doWrite(ChannelOutboundBuffer)
     */
    protected void handleOutboundMessage(Object msg) {
        outboundMessages().add(msg);
    }

    /**
     * 处理每个入站消息。
     * 
     * @param msg 要处理的入站消息
     */
    protected void handleInboundMessage(Object msg) {
        inboundMessages().add(msg);
    }

    /**
     * EmbeddedChannel 使用的 Unsafe 实现，为通道操作提供安全访问。
     */
    private final class EmbeddedUnsafe extends AbstractUnsafe {

        /**
         * 委托给 EmbeddedUnsafe 实例的包装器，确保在每个可能改变通道状态
         * 并可能计划任务以供稍后执行的操作之后调用 runPendingTasks()。
         */
        final Unsafe wrapped = new Unsafe() {
            @Override
            public RecvByteBufAllocator.Handle recvBufAllocHandle() {
                return EmbeddedUnsafe.this.recvBufAllocHandle();
            }

            @Override
            public SocketAddress localAddress() {
                return EmbeddedUnsafe.this.localAddress();
            }

            @Override
            public SocketAddress remoteAddress() {
                return EmbeddedUnsafe.this.remoteAddress();
            }

            @Override
            public void register(EventLoop eventLoop, ChannelPromise promise) {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.register(eventLoop, promise);
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public void bind(SocketAddress localAddress, ChannelPromise promise) {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.bind(localAddress, promise);
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.connect(remoteAddress, localAddress, promise);
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public void disconnect(ChannelPromise promise) {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.disconnect(promise);
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public void close(ChannelPromise promise) {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.close(promise);
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public void closeForcibly() {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.closeForcibly();
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public void deregister(ChannelPromise promise) {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.deregister(promise);
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public void beginRead() {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.beginRead();
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public void write(Object msg, ChannelPromise promise) {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.write(msg, promise);
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public void flush() {
                executingStackCnt++;
                try {
                    EmbeddedUnsafe.this.flush();
                } finally {
                    executingStackCnt--;
                    maybeRunPendingTasks();
                }
            }

            @Override
            public ChannelPromise voidPromise() {
                return EmbeddedUnsafe.this.voidPromise();
            }

            @Override
            public ChannelOutboundBuffer outboundBuffer() {
                return EmbeddedUnsafe.this.outboundBuffer();
            }
        };

        /**
         * 实现连接操作，对于嵌入式通道，直接设置成功状态。
         */
        @Override
        public void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            safeSetSuccess(promise);
        }
    }

    /**
     * {@link EmbeddedChannel} 使用的特殊管道实现，用于处理未处理的入站异常和消息。
     */
    private final class EmbeddedChannelPipeline extends DefaultChannelPipeline {
        /**
         * 创建新的嵌入式通道管道。
         *
         * @param channel 关联的通道
         */
        EmbeddedChannelPipeline(EmbeddedChannel channel) {
            super(channel);
        }

        /**
         * 处理未处理的入站异常，将异常记录到 EmbeddedChannel 中。
         *
         * @param cause 未处理的异常
         */
        @Override
        protected void onUnhandledInboundException(Throwable cause) {
            recordException(cause);
        }

        /**
         * 处理未处理的入站消息，将消息添加到入站消息队列中。
         *
         * @param ctx 处理上下文
         * @param msg 未处理的消息
         */
        @Override
        protected void onUnhandledInboundMessage(ChannelHandlerContext ctx, Object msg) {
            handleInboundMessage(msg);
        }
    }
}
