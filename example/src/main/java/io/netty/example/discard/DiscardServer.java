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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.example.util.ServerUtil;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;

/**
 * Discards any incoming data.
 */
public final class DiscardServer {

    static final int PORT = Integer.parseInt(System.getProperty("port", "8009"));

    public static void main(String[] args) throws Exception {
        // 配置 SSL，以实现安全通信，防止数据在传输过程中被窃听或篡改
        final SslContext sslCtx = ServerUtil.buildSslContext();

        // 创建两个事件循环组：
        // bossGroup 用于监听和接收客户端的连接请求，通常只需要一个线程即可；
        // workerGroup 用于处理已经建立连接的客户端的数据读写操作，线程数通常根据系统资源自动配置。
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            // ServerBootstrap 是 Netty 的辅助类，用于简化服务器的启动配置
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup) // 设置两个事件循环组，bossGroup 管理连接，workerGroup 处理数据
                    .channel(NioServerSocketChannel.class) // 指定使用基于 NIO 的服务器通道，支持非阻塞 I/O
                    .handler(new LoggingHandler(LogLevel.INFO)) // 添加日志处理器，用于记录服务器启动及事件日志，便于调试
                    .childHandler(new ChannelInitializer<SocketChannel>() { // 为每个新建立的连接初始化通道流水线
                        @Override
                        public void initChannel(SocketChannel ch) {
                            // 获取该连接对应的通道流水线，流水线可以按顺序处理接收到的数据
                            ChannelPipeline p = ch.pipeline();
                            // 如果启用了 SSL，则在通道流水线中添加 SSL 处理器，实现数据的加密和解密
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc()));
                            }
                            // 添加自定义的处理器 DiscardServerHandler，其作用是丢弃所有接收到的数据
                            p.addLast(new DiscardServerHandler());
                        }
                    });

            // 绑定指定端口并启动服务器，sync() 方法确保操作完成再继续执行
            ChannelFuture f = b.bind(PORT).sync();

            // 通过等待服务器通道关闭来保持服务器一直运行，实际部署时可以通过其他信号来关闭
            f.channel().closeFuture().sync();
        } finally {
            // 优雅地关闭工作线程组，确保所有资源都被释放
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
