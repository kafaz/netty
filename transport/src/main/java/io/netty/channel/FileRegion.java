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

import io.netty.util.ReferenceCounted;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

/**
 * 文件区域的抽象，可通过支持<a href="https://en.wikipedia.org/wiki/Zero-copy">零拷贝文件传输</a>
 * 技术的 {@link Channel} 进行发送。
 * 
 * <p>
 * 零拷贝是一种优化数据传输的技术，它避免了在数据传输过程中将数据从内核空间复制到用户空间的额外步骤。
 * 在传统的文件传输中，数据需要经过以下路径：
 * <ol>
 *   <li>从磁盘读取到内核缓冲区</li>
 *   <li>从内核缓冲区复制到用户空间缓冲区</li>
 *   <li>从用户空间缓冲区复制回内核中的 socket 缓冲区</li>
 *   <li>从 socket 缓冲区发送到网络接口</li>
 * </ol>
 * </p>
 * 
 * <p>
 * 而使用零拷贝技术（如 Java NIO 中的 {@link FileChannel#transferTo} 方法），可以直接将数据从文件系统缓存传输到网络接口，
 * 从而避免了中间的两次复制操作，大大提高了传输效率并减少了 CPU 的使用。
 * </p>
 * 
 * <p>
 * 在 Netty 中，{@link FileRegion} 通常与 {@link FileChannel} 一起使用，
 * 作为高效传输大型文件的首选机制。
 * </p>
 * 
 * <p>
 * 使用示例：
 * <pre>
 * // 打开文件通道
 * RandomAccessFile raf = new RandomAccessFile("large_file.iso", "r");
 * FileChannel fileChannel = raf.getChannel();
 * 
 * // 创建 FileRegion
 * FileRegion region = new DefaultFileRegion(fileChannel, 0, fileChannel.size());
 * 
 * // 通过 Netty Channel 发送
 * channel.writeAndFlush(region).addListener(new ChannelFutureListener() {
 *     public void operationComplete(ChannelFuture future) {
 *         if (future.isSuccess()) {
 *             System.out.println("File transferred successfully");
 *         } else {
 *             System.err.println("File transfer failed: " + future.cause());
 *         }
 *         fileChannel.close();
 *         raf.close();
 *     }
 * });
 * </pre>
 * </p>
 *
 * <h3>升级您的 JDK / JRE</h3>
 *
 * {@link FileChannel#transferTo(long, long, WritableByteChannel)} 在旧版本的 Sun JDK 
 * 及其衍生版本中至少有四个已知的 bug。
 * 如果您打算使用零拷贝文件传输，请升级您的 JDK 到 1.6.0_18 或更高版本。
 * <ul>
 * <li><a href="https://bugs.java.com/bugdatabase/view_bug.do?bug_id=5103988">5103988</a>
 *   - FileChannel.transferTo() 在 EAGAIN 情况下应返回 -1 而不是抛出 IOException</li>
 * <li><a href="https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6253145">6253145</a>
 *   - 在 Linux 上当 FileChannel.transferTo(), 超过 2GB 边界时失败</li>
 * <li><a href="https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6427312">6427312</a>
 *   - FileChannel.transferTo() 抛出 IOException "system call interrupted"</li>
 * <li><a href="https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6524172">6470086</a>
 *   - FileChannel.transferTo(2147483647, 1, channel) 导致 "Value too large" 异常</li>
 * </ul>
 *
 * <h3>检查您的操作系统和 JDK / JRE</h3>
 *
 * 如果您的操作系统（或 JDK / JRE）不支持零拷贝文件传输，使用 {@link FileRegion} 发送文件可能会失败或性能较差。
 * 例如，在 Windows 系统上发送大文件可能表现不佳。
 *
 * <h3>不是所有传输层都支持</h3>
 * 
 * <p>
 * 零拷贝传输通常只在 NIO 传输层中工作良好。OIO（旧的阻塞 I/O）传输层不支持零拷贝优化。
 * 此外，某些特殊的传输层（如 SCTP、UDT）可能对 FileRegion 的支持有限。
 * </p>
 * 
 * <p>
 * 在使用 {@link FileRegion} 之前，请确保您使用的传输层支持零拷贝操作，否则您可能会发现使用常规的
 * {@link io.netty.buffer.ByteBuf} 装载文件内容更加可靠（尽管效率较低）。
 * </p>
 */
public interface FileRegion extends ReferenceCounted {

    /**
     * 返回文件传输开始的偏移量。
     * 
     * <p>
     * 该方法返回原始文件中的起始位置（以字节为单位），从该位置开始进行文件传输。
     * 此偏移量通常在创建 {@link FileRegion} 实例时指定。
     * </p>
     * 
     * @return 文件中的起始偏移量（以字节为单位）
     */
    long position();

    /**
     * 返回已经传输的字节数。
     *
     * @deprecated 请使用 {@link #transferred()} 代替。
     */
    @Deprecated
    long transfered();

    /**
     * 返回已经传输的字节数。
     * 
     * <p>
     * 此方法返回当前 {@link FileRegion} 已成功传输的字节总数。每次调用 {@link #transferTo(WritableByteChannel, long)}
     * 方法成功传输数据后，此值都会相应增加。
     * </p>
     * 
     * <p>
     * 此值对于跟踪传输进度很有用，特别是在大文件传输过程中。
     * </p>
     *
     * @return 已成功传输的字节数
     */
    long transferred();

    /**
     * 返回要传输的字节总数。
     * 
     * <p>
     * 此方法返回当前 {@link FileRegion} 要传输的总字节数。这个值通常在创建 FileRegion 实例时指定，
     * 表示从 {@link #position()} 开始要传输的字节数。
     * </p>
     * 
     * <p>
     * 要计算剩余要传输的字节数，可以使用：{@code count() - transferred()}
     * </p>
     *
     * @return 要传输的总字节数
     */
    long count();

    /**
     * 将此文件区域的内容传输到指定的通道。
     * 
     * <p>
     * 此方法尝试使用零拷贝技术将文件数据直接传输到目标通道。如果底层实现和操作系统支持，
     * 这将避免不必要的数据复制，从而提高性能。
     * </p>
     * 
     * <p>
     * 注意：此方法可能不会一次性传输所有请求的字节，因此调用者可能需要在循环中多次调用此方法，
     * 直到所有数据都被传输完成（即 {@code transferred() == count()}）。
     * </p>
     *
     * @param target    传输的目标通道
     * @param position  相对于文件区域起始位置的偏移量。例如，{@code 0} 将使传输从
     *                  {@link #position()} 字节处开始，而 {@code count() - 1} 将只传输区域的最后一个字节。
     *                  
     * @return 此次操作成功传输的字节数
     * @throws IOException 如果传输过程中发生 I/O 错误
     */
    long transferTo(WritableByteChannel target, long position) throws IOException;

    /**
     * 增加此对象的引用计数并返回此实例。
     * 
     * <p>
     * 此方法是 {@link ReferenceCounted} 接口的一部分，用于资源管理。每次调用此方法，
     * 对象的引用计数都会增加 1。
     * </p>
     *
     * @return 此 {@link FileRegion} 实例
     */
    @Override
    FileRegion retain();

    /**
     * 将此对象的引用计数增加指定的增量并返回此实例。
     * 
     * <p>
     * 此方法允许一次性增加多个引用计数，而不必多次调用 {@link #retain()}。
     * </p>
     *
     * @param increment 要增加的引用计数值
     * @return 此 {@link FileRegion} 实例
     */
    @Override
    FileRegion retain(int increment);

    /**
     * 记录对此对象的访问并返回此实例。
     * 
     * <p>
     * 此方法可用于调试目的，以跟踪对象的访问模式。它不会更改引用计数。
     * </p>
     *
     * @return 此 {@link FileRegion} 实例
     */
    @Override
    FileRegion touch();

    /**
     * 记录对此对象的访问，添加自定义提示信息，并返回此实例。
     * 
     * <p>
     * 此方法允许在记录访问时添加自定义信息，有助于更详细的调试。
     * </p>
     *
     * @param hint 与此访问相关联的提示对象
     * @return 此 {@link FileRegion} 实例
     */
    @Override
    FileRegion touch(Object hint);
}
