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
package io.netty.example.echo;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.example.util.ServerUtil;
import io.netty.handler.ssl.SslContext;

/**
 * Echo客户端类
 * 当连接建立时发送一条消息，并将从服务器收到的任何数据回显。
 * 简单来说，Echo客户端通过向服务器发送第一条消息来启动客户端和服务器之间的数据来回传输。
 */
public final class EchoClient {

    // 服务器主机地址，默认为本地地址127.0.0.1
    static final String HOST = System.getProperty("host", "127.0.0.1");
    // 服务器端口，默认为8007
    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));
    // 发送消息的大小，默认为256字节
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256"));

    public static void main(String[] args) throws Exception {
        // 配置SSL上下文
        final SslContext sslCtx = ServerUtil.buildSslContext();

        // 创建EventLoopGroup，用于处理客户端的事件和IO
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            // 创建客户端启动引导类
            Bootstrap b = new Bootstrap();
            b.group(group)  // 设置EventLoopGroup
             // 设置要使用的Channel类型为NIO客户端Socket通道
             .channel(NioSocketChannel.class)
             // 设置TCP无延迟选项
             .option(ChannelOption.TCP_NODELAY, true)
             // 设置处理器
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     // 获取Channel的ChannelPipeline
                     ChannelPipeline p = ch.pipeline();
                     // 如果配置了SSL，添加SSL处理器
                     if (sslCtx != null) {
                         p.addLast(sslCtx.newHandler(ch.alloc(), HOST, PORT));
                     }
                     // 可选：添加日志处理器，用于调试
                     //p.addLast(new LoggingHandler(LogLevel.INFO));
                     // 添加Echo客户端处理器，处理实际的业务逻辑
                     p.addLast(new EchoClientHandler());
                 }
             });

            // 启动客户端
            // 连接到服务器，并等待连接完成
            ChannelFuture f = b.connect(HOST, PORT).sync();

            // 等待，直到连接关闭
            // 这里会阻塞等待，直到客户端Channel关闭
            f.channel().closeFuture().sync();
        } finally {
            // 优雅关闭EventLoopGroup
            // 释放所有资源，并关闭所有线程
            group.shutdownGracefully();
        }
    }
}
