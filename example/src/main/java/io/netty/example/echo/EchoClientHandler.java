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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler implementation for the echo client.  It initiates the ping-pong
 * traffic between the echo client and server by sending the first message to
 * the server.
 */
public class EchoClientHandler extends ChannelInboundHandlerAdapter {

    private final ByteBuf firstMessage;

    /**
     * Creates a client-side handler.
     */
    public EchoClientHandler() {
        firstMessage = Unpooled.buffer(EchoClient.SIZE);
        for (int i = 0; i < firstMessage.capacity(); i ++) {
            firstMessage.writeByte((byte) i);
        }
    }

    /**
     * 当Channel变为活跃状态时被调用
     * - 在TCP连接建立后触发
     * - 这是客户端向服务器发送第一条消息的最佳时机
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 当连接建立时，立即发送第一条消息
        // writeAndFlush = write + flush 组合操作
        // - write: 将数据写入缓冲区
        // - flush: 将缓冲区数据刷新到网络端
        ctx.writeAndFlush(firstMessage);
    }

    /**
     * 当从服务器接收到新的数据时被调用
     * - 每收到一个消息包就会调用一次
     * - msg参数包含了接收到的数据，默认类型是ByteBuf
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Echo客户端的核心逻辑：将收到的数据写回服务器
        // 注意这里只是写入缓冲区，没有flush
        ctx.write(msg);
    }

    /**
     * 当一次数据读取完成后被调用
     * - 一次网络读取可能会触发多次channelRead
     * - channelReadComplete标志着一次读取操作的结束
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        // 刷新所有待写入的数据到网络端
        // 配合上面的write操作，确保数据确实被发送出去
        ctx.flush();
    }

    /**
     * 当处理过程中发生异常时被调用
     * - 用于处理所有的I/O异常和业务逻辑异常
     * - 通常在这里进行异常日志记录和连接清理
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 打印异常堆栈信息
        cause.printStackTrace();
        // 关闭连接
        // 这是一个很好的实践，因为在发生异常时应该及时释放资源
        ctx.close();
    }
}
