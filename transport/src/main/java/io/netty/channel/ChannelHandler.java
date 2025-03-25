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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * 处理I/O事件或拦截I/O操作，并将其转发给ChannelPipeline中的下一个处理器。
 *
 * <h3>子类型</h3>
 * <p>
 * ChannelHandler接口本身并没有提供太多方法，但通常需要实现其中一个子类型：
 * <ul>
 * <li>{@link ChannelInboundHandler} - 用于处理入站I/O事件</li>
 * <li>{@link ChannelOutboundHandler} - 用于处理出站I/O操作</li>
 * </ul>
 * </p>
 * <p>
 * 为了使用方便，框架提供了以下适配器类：
 * <ul>
 * <li>{@link ChannelInboundHandlerAdapter} - 用于处理入站I/O事件</li>
 * <li>{@link ChannelOutboundHandlerAdapter} - 用于处理出站I/O操作</li>
 * <li>{@link ChannelDuplexHandler} - 用于同时处理入站和出站事件</li>
 * </ul>
 * </p>
 * <p>
 * 更多信息请参考各个子类型的文档。
 * </p>
 *
 * <h3>上下文对象</h3>
 * <p>
 * 每个ChannelHandler都会被提供一个{@link ChannelHandlerContext}对象。
 * ChannelHandler应该通过这个上下文对象与其所属的ChannelPipeline进行交互。
 * 使用上下文对象，ChannelHandler可以：
 * - 传递事件到上游或下游
 * - 动态修改管道
 * - 存储处理器特定的信息（使用{@link AttributeKey}）
 *
 * <h3>状态管理</h3>
 *
 * ChannelHandler经常需要存储一些状态信息。最简单和推荐的方式是使用成员变量：
 * <pre>
 * public interface Message {
 *     // 你的方法
 * }
 *
 * public class DataServerHandler extends {@link SimpleChannelInboundHandler}&lt;Message&gt; {
 *
 *     <b>private boolean loggedIn;</b>
 *
 *     {@code @Override}
 *     public void channelRead0({@link ChannelHandlerContext} ctx, Message message) {
 *         if (message instanceof LoginMessage) {
 *             authenticate((LoginMessage) message);
 *             <b>loggedIn = true;</b>
 *         } else (message instanceof GetDataMessage) {
 *             if (<b>loggedIn</b>) {
 *                 ctx.writeAndFlush(fetchSecret((GetDataMessage) message));
 *             } else {
 *                 fail();
 *             }
 *         }
 *     }
 *     ...
 * }
 * </pre>
 * 
 * 由于处理器实例的状态变量是专门用于一个连接的，因此必须为每个新通道创建一个新的处理器实例，
 * 以避免未经身份验证的客户端获取机密信息的竞态条件：
 * <pre>
 * // 为每个通道创建新的处理器实例
 * // 参见 {@link ChannelInitializer#initChannel(Channel)}
 * public class DataServerInitializer extends {@link ChannelInitializer}&lt;{@link Channel}&gt; {
 *     {@code @Override}
 *     public void initChannel({@link Channel} channel) {
 *         channel.pipeline().addLast("handler", <b>new DataServerHandler()</b>);
 *     }
 * }
 * </pre>
 *
 * <h4>使用{@link AttributeKey}</h4>
 *
 * 尽管推荐使用成员变量来存储处理器的状态，但有时候你可能不想创建太多处理器实例。
 * 在这种情况下，可以使用{@link ChannelHandlerContext}提供的{@link AttributeKey}：
 * <pre>
 * {@code @Sharable}
 * public class DataServerHandler extends {@link SimpleChannelInboundHandler}&lt;Message&gt; {
 *     private final {@link AttributeKey}&lt;{@link Boolean}&gt; auth =
 *           {@link AttributeKey#valueOf(String) AttributeKey.valueOf("auth")};
 *
 *     {@code @Override}
 *     public void channelRead({@link ChannelHandlerContext} ctx, Message message) {
 *         {@link Attribute}&lt;{@link Boolean}&gt; attr = ctx.attr(auth);
 *         // ... 处理逻辑
 *     }
 * }
 * </pre>
 *
 * <h4>{@code @Sharable}注解</h4>
 * <p>
 * 如果一个{@link ChannelHandler}被{@code @Sharable}注解标注，
 * 表示你可以创建该处理器的一个实例，并将其添加到多个{@link ChannelPipeline}中而不会产生竞态条件。
 * <p>
 * 如果没有指定此注解，则每次将处理器添加到管道时都必须创建一个新的处理器实例，
 * 因为它具有非共享状态（如成员变量）。
 * <p>
 * 此注解仅用于文档目的，类似于JCIP注解。
 */
public interface ChannelHandler {

    /**
     * 在ChannelHandler被添加到实际的上下文并准备好处理事件后调用。
     */
    void handlerAdded(ChannelHandlerContext ctx) throws Exception;

    /**
     * 在ChannelHandler从实际的上下文中移除后调用，此时它将不再处理事件。
     */
    void handlerRemoved(ChannelHandlerContext ctx) throws Exception;

    /**
     * 当发生异常时调用。
     *
     * @deprecated 如果要处理此事件，应该实现{@link ChannelInboundHandler}
     * 并在那里实现相应的方法。
     */
    @Deprecated
    void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception;

    /**
     * 表示被注解的{@link ChannelHandler}的同一个实例可以被安全地添加到
     * 多个{@link ChannelPipeline}中而不会产生竞态条件。
     * <p>
     * 如果没有指定此注解，则每次添加到管道时都必须创建新的处理器实例，
     * 因为它具有非共享状态（如成员变量）。
     * <p>
     * 此注解仅用于文档目的，类似于JCIP注解。
     */
    @Inherited
    @Documented
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Sharable {
        // 无值
    }
}