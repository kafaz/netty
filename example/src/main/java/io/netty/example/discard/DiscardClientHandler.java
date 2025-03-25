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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Handles a client-side channel.
 */
public class DiscardClientHandler extends SimpleChannelInboundHandler<Object> {

    private ByteBuf content; // 用于存储发送到服务器的数据缓冲区
    private ChannelHandlerContext ctx; // 保存通道处理上下文以便后续使用

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 当与服务器的连接建立完成后调用此方法
        this.ctx = ctx;

        // 初始化消息内容：创建指定大小的直接内存缓冲区并填充0值
        // directBuffer创建的是堆外内存，避免了JVM堆内存与native内存之间的数据拷贝，提高性能
        content = ctx.alloc().directBuffer(DiscardClient.SIZE).writeZero(DiscardClient.SIZE);

        // 连接建立后立即开始发送数据，触发客户端向服务器发送流量
        generateTraffic();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 当与服务器的连接断开时调用此方法
        // 释放之前分配的ByteBuf资源，防止内存泄漏
        content.release();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 当从服务器接收到消息时调用此方法
        // 在丢弃协议中，服务器不应发送任何数据，但如果接收到也会被忽略
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 当处理过程中发生异常时调用
        // 打印异常栈信息并关闭连接
        cause.printStackTrace();
        ctx.close();
    }

    long counter; // 可用于统计已发送的消息数量（当前未使用）

    private void generateTraffic() {
        // 生成网络流量的核心方法
        // 将缓冲区内容写入并刷新到网络中，同时保留原缓冲区内容以便重复使用
        // retainedDuplicate()创建缓冲区的复制并增加引用计数，防止被释放
        ctx.writeAndFlush(content.retainedDuplicate()).addListener(trafficGenerator);
    }

    private final ChannelFutureListener trafficGenerator = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
            // 当数据发送操作完成时触发此监听器
            if (future.isSuccess()) {
                // 如果发送成功，则继续发送更多数据，形成连续不断的流量
                generateTraffic();
            } else {
                // 如果发送失败，打印异常并关闭连接
                future.cause().printStackTrace();
                future.channel().close();
            }
        }
    };
}
