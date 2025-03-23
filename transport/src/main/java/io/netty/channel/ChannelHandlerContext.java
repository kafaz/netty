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

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.concurrent.EventExecutor;

/**
 * 使 ChannelHandler 能够与其 ChannelPipeline 和其他处理器进行交互的上下文对象。
 * 处理器可以通过该上下文对象通知 ChannelPipeline 中的下一个 ChannelHandler，
 * 也可以动态地修改它所属的 ChannelPipeline。
 *
 * <h3>通知机制</h3>
 * 
 * 你可以通过调用这里提供的各种方法来通知同一 ChannelPipeline 中的最近的处理器。
 * 请参考 ChannelPipeline 来理解事件是如何流转的。
 *
 * <h3>修改管道</h3>
 *
 * 你可以通过调用 pipeline() 方法获取处理器所属的 ChannelPipeline。
 * 在实际应用中，可以在运行时动态地在管道中插入、删除或替换处理器。
 *
 * <h3>上下文对象的后续使用</h3>
 *
 * 你可以保存 ChannelHandlerContext 以供后续使用，比如在处理器方法之外触发事件，
 * 甚至可以在不同的线程中使用。例如：
 * <pre>
 * public class MyHandler extends ChannelDuplexHandler {
 *     private ChannelHandlerContext ctx;
 *
 *     public void beforeAdd(ChannelHandlerContext ctx) {
 *         this.ctx = ctx;
 *     }
 *
 *     public void login(String username, password) {
 *         ctx.write(new LoginMessage(username, password));
 *     }
 *     ...
 * }
 * </pre>
 *
 * <h3>存储状态信息</h3>
 *
 * 通过 attr(AttributeKey) 方法，你可以存储和访问与 ChannelHandler/Channel 
 * 及其上下文相关的状态信息。请参考 ChannelHandler 来了解管理状态信息的各种推荐方式。
 *
 * <h3>处理器可以有多个上下文</h3>
 *
 * 请注意，一个 ChannelHandler 实例可以被添加到多个 ChannelPipeline 中。
 * 这意味着单个 ChannelHandler 实例可以有多个 ChannelHandlerContext，
 * 如果它被添加到一个或多个 ChannelPipeline 中多次，则可能会使用不同的 ChannelHandlerContext 调用该实例。
 * 另外注意，如果一个 ChannelHandler 需要添加到多个 ChannelPipeline 中，
 * 应该用 @Sharable 注解标记。
 */
public interface ChannelHandlerContext extends AttributeMap, ChannelInboundInvoker, ChannelOutboundInvoker {

    /**
     * 返回绑定到此 ChannelHandlerContext 的 Channel
     */
    Channel channel();

    /**
     * 返回用于执行任意任务的 EventExecutor
     */
    EventExecutor executor();

    /**
     * 返回 ChannelHandlerContext 的唯一名称。
     * 这个名称在将 ChannelHandler 添加到 ChannelPipeline 时使用。
     * 该名称也可以用于从 ChannelPipeline 中访问已注册的 ChannelHandler。
     */
    String name();

    /**
     * 返回绑定到此 ChannelHandlerContext 的 ChannelHandler
     */
    ChannelHandler handler();

    /**
     * 如果属于此上下文的 ChannelHandler 已从 ChannelPipeline 中移除，则返回 true。
     * 注意：此方法只能在 EventLoop 中调用。
     */
    boolean isRemoved();

    /**
     * 返回分配的 ChannelPipeline
     */
    ChannelPipeline pipeline();

    /**
     * 返回分配的 ByteBufAllocator，用于分配 ByteBuf
     */
    ByteBufAllocator alloc();

    // 以下是事件触发方法，用于在管道中传播各种事件
    ChannelHandlerContext fireChannelRegistered();    // 触发 Channel 注册事件
    ChannelHandlerContext fireChannelUnregistered();  // 触发 Channel 注销事件
    ChannelHandlerContext fireChannelActive();        // 触发 Channel 活跃事件
    ChannelHandlerContext fireChannelInactive();      // 触发 Channel 非活跃事件
    ChannelHandlerContext fireExceptionCaught(Throwable cause);  // 触发异常捕获事件
    ChannelHandlerContext fireUserEventTriggered(Object evt);    // 触发用户自定义事件
    ChannelHandlerContext fireChannelRead(Object msg);           // 触发消息读取事件
    ChannelHandlerContext fireChannelReadComplete();             // 触发消息读取完成事件
    ChannelHandlerContext fireChannelWritabilityChanged();       // 触发可写状态变化事件
    
    /**
     * @deprecated 请使用 Channel#attr(AttributeKey)
     */
    @Deprecated
    @Override
    <T> Attribute<T> attr(AttributeKey<T> key);

    /**
     * @deprecated 请使用 Channel#hasAttr(AttributeKey)
     */
    @Deprecated
    @Override
    <T> boolean hasAttr(AttributeKey<T> key);
}
