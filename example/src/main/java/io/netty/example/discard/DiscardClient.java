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
package io.netty.example.discard;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.example.util.ServerUtil;
import io.netty.handler.ssl.SslContext;

/**
 * Keeps sending random data to the specified address.
 * 持续向指定地址发送随机数据的客户端实现。
 */
public final class DiscardClient {

    static final String HOST = System.getProperty("host", "127.0.0.1"); // 服务器主机地址，默认为本地回环地址
    static final int PORT = Integer.parseInt(System.getProperty("port", "8009")); // 服务器端口，默认为8009
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256")); // 发送数据的大小，默认为256字节

    public static void main(String[] args) throws Exception {
        // 配置SSL上下文
        // SslContext用于创建SSL处理器，支持安全的网络通信
        final SslContext sslCtx = ServerUtil.buildSslContext();

        // 创建EventLoopGroup，用于处理I/O操作的多线程事件循环器
        // NioEventLoopGroup是基于NIO的实现，用于创建处理事件的线程组
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            // Bootstrap是客户端启动辅助类，用于配置客户端参数并启动客户端
            Bootstrap b = new Bootstrap();
            b.group(group) // 设置EventLoopGroup，用于处理客户端事件和IO
                    .channel(NioSocketChannel.class) // 指定Channel类型为NioSocketChannel，适用于TCP客户端
                    .handler(new ChannelInitializer<SocketChannel>() { // 配置Channel的处理器
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            // 获取ChannelPipeline，它是处理器的链，用于处理或拦截入站/出站事件和操作
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                // 如果SSL上下文存在，添加SSL处理器到管道
                                p.addLast(sslCtx.newHandler(ch.alloc(), HOST, PORT));
                            }
                            // 添加自定义的客户端处理器，用于实际的数据发送逻辑
                            p.addLast(new DiscardClientHandler());
                        }
                    });

            // 尝试连接到服务器
            // connect()创建连接，sync()等待连接完成
            ChannelFuture f = b.connect(HOST, PORT).sync();

            // 等待连接关闭
            // closeFuture()获取通道关闭的future，sync()等待通道关闭
            f.channel().closeFuture().sync();
        } finally {
            // 优雅地关闭EventLoopGroup，释放所有资源
            group.shutdownGracefully();
        }
    }
}
