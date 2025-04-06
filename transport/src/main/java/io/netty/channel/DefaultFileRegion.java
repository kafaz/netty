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

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

/**
 * {@link FileRegion}接口的默认实现，用于从{@link FileChannel}或{@link File}传输数据。
 *
 * <p>
 * 该实现支持零拷贝文件传输，这是一种高效的数据传输技术，可以避免在传输过程中不必要的内存复制操作。
 * 它直接利用操作系统提供的机制（如sendfile系统调用）将文件数据从文件系统缓存传输到目标通道，
 * 而无需将数据复制到用户空间缓冲区。
 * </p>
 * 
 * <p>
 * 主要适用场景：
 * <ul>
 *   <li>网络文件服务器</li>
 *   <li>大文件传输</li>
 *   <li>静态内容分发</li>
 * </ul>
 * </p>
 * 
 * <p>
 * 使用示例：
 * <pre>
 * // 从FileChannel创建
 * RandomAccessFile raf = new RandomAccessFile("large_file.dat", "r");
 * FileChannel fileChannel = raf.getChannel();
 * FileRegion region = new DefaultFileRegion(fileChannel, 0, fileChannel.size());
 * 
 * // 或从File创建
 * File file = new File("large_file.dat");
 * FileRegion region = new DefaultFileRegion(file, 0, file.length());
 * 
 * // 发送文件
 * channel.writeAndFlush(region);
 * </pre>
 * </p>
 *
 * <p>注意：当{@link #refCnt()}返回{@code 0}时，{@link FileChannel}将被自动关闭。</p>
 */
public class DefaultFileRegion extends AbstractReferenceCounted implements FileRegion {

    // 用于记录日志的内部日志器
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultFileRegion.class);
    // 文件对象，用于延迟打开的情况
    private final File f;
    // 文件传输的起始位置
    private final long position;
    // 要传输的字节总数
    private final long count;
    // 已经传输的字节数
    private long transferred;
    // 文件通道，用于实际的文件传输操作
    private FileChannel file;

    /**
     * 创建一个新的DefaultFileRegion实例，基于已打开的FileChannel。
     * 
     * <p>
     * 这个构造函数接受一个已经打开的FileChannel，并直接使用它进行文件传输。
     * 当引用计数降为0时，提供的FileChannel会被自动关闭。
     * </p>
     *
     * @param fileChannel  要传输的{@link FileChannel}
     * @param position     传输开始的位置（字节偏移量）
     * @param count        要传输的字节数
     * @throws NullPointerException 如果fileChannel为null
     * @throws IllegalArgumentException 如果position或count为负数
     */
    public DefaultFileRegion(FileChannel fileChannel, long position, long count) {
        // 检查fileChannel是否为null，如果是则抛出NullPointerException
        this.file = ObjectUtil.checkNotNull(fileChannel, "fileChannel");
        // 检查position是否为非负数
        this.position = checkPositiveOrZero(position, "position");
        // 检查count是否为非负数
        this.count = checkPositiveOrZero(count, "count");
        // 没有关联的File对象
        this.f = null;
    }

    /**
     * 创建一个新的DefaultFileRegion实例，基于文件对象，支持延迟打开。
     * 
     * <p>
     * 这个构造函数接受一个File对象，但不会立即打开文件。文件会在以下情况被打开：
     * <ul>
     *   <li>第一次调用{@link #transferTo(WritableByteChannel, long)}时</li>
     *   <li>显式调用{@link #open()}方法时</li>
     * </ul>
     * 延迟打开可以减少文件描述符的使用，特别是当创建了FileRegion但可能不会立即使用时。
     * </p>
     *
     * @param file      要传输的{@link File}
     * @param position  传输开始的位置（字节偏移量）
     * @param count     要传输的字节数
     * @throws NullPointerException 如果file为null
     * @throws IllegalArgumentException 如果position或count为负数
     */
    public DefaultFileRegion(File file, long position, long count) {
        // 检查file是否为null，如果是则抛出NullPointerException
        this.f = ObjectUtil.checkNotNull(file, "file");
        // 检查position是否为非负数
        this.position = checkPositiveOrZero(position, "position");
        // 检查count是否为非负数
        this.count = checkPositiveOrZero(count, "count");
        // file字段初始为null，表示文件尚未打开
    }

    /**
     * 检查FileRegion是否具有打开的文件描述符。
     * 
     * <p>
     * 如果FileRegion是通过File构造函数创建的，并且尚未调用{@link #open()}方法，
     * 或者文件通道已关闭，此方法将返回false。
     * </p>
     * 
     * @return 如果FileRegion有打开的文件描述符，则返回{@code true}
     */
    public boolean isOpen() {
        return file != null;
    }

    /**
     * 如果尚未打开，显式打开底层文件描述符。
     * 
     * <p>
     * 此方法用于提前打开文件，而不是等到第一次传输时才打开。
     * 只有当FileRegion是通过File构造函数创建的，并且引用计数大于0时，此方法才会执行打开操作。
     * 如果FileRegion已经打开，此方法不会执行任何操作。
     * </p>
     * 
     * @throws IOException 如果打开文件时发生I/O错误
     */
    public void open() throws IOException {
        if (!isOpen() && refCnt() > 0) {
            // 只有当这个DefaultFileRegion尚未被释放时才打开文件
            // 使用RandomAccessFile以只读模式打开文件，并获取其通道
            file = new RandomAccessFile(f, "r").getChannel();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * 返回传输开始的位置（字节偏移量）。这个值在构造时指定，不会改变。
     * </p>
     */
    @Override
    public long position() {
        return position;
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * 返回要传输的总字节数。这个值在构造时指定，不会改变。
     * </p>
     */
    @Override
    public long count() {
        return count;
    }

    /**
     * {@inheritDoc}
     * 
     * @deprecated 使用 {@link #transferred()} 代替。
     */
    @Deprecated
    @Override
    public long transfered() {
        return transferred;
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * 返回已传输的字节数。这个值会在每次成功传输后更新。
     * </p>
     */
    @Override
    public long transferred() {
        return transferred;
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * 将文件内容传输到指定的通道。此方法利用{@link FileChannel#transferTo}实现零拷贝传输。
     * 注意，此方法可能不会一次性传输所有请求的字节，调用者应在循环中调用此方法直到所有数据都被传输。
     * </p>
     *
     * @param target    传输的目标通道
     * @param position  相对于文件区域起始位置的偏移量
     * @return          此次操作传输的字节数
     * @throws IOException 如果传输过程中发生I/O错误
     * @throws IllegalArgumentException 如果position参数超出有效范围
     * @throws IllegalReferenceCountException 如果引用计数为0
     */
    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        // 计算剩余要传输的字节数
        long count = this.count - position;
        if (count < 0 || position < 0) {
            // 如果位置参数无效，抛出异常
            throw new IllegalArgumentException(
                    "position out of range: " + position +
                    " (expected: 0 - " + (this.count - 1) + ')');
        }
        if (count == 0) {
            // 如果没有字节要传输，直接返回0
            return 0L;
        }
        if (refCnt() == 0) {
            // 如果引用计数为0（已释放），抛出异常
            throw new IllegalReferenceCountException(0);
        }
        // 调用open确保文件通道已初始化。如果已经调用过，这是一个空操作。
        open();

        // 执行实际的零拷贝传输操作
        long written = file.transferTo(this.position + position, count, target);
        if (written > 0) {
            // 成功传输了数据，更新总传输字节数
            transferred += written;
        } else if (written == 0) {
            // 如果传输的数据量为0，我们需要检查请求的字节数是否大于
            // 实际文件本身，因为文件可能在磁盘上被截断了。
            //
            // 参见 https://github.com/netty/netty/issues/8868
            validate(this, position);
        }
        return written;
    }

    /**
     * 释放此FileRegion持有的资源，主要是关闭文件通道。
     * 
     * <p>
     * 此方法在引用计数降为0时被自动调用，用于清理资源。
     * 它会关闭底层的FileChannel，如果关闭过程中发生异常，会记录警告日志但不会抛出异常。
     * </p>
     */
    @Override
    protected void deallocate() {
        FileChannel file = this.file;

        if (file == null) {
            // 文件已关闭或从未打开，直接返回
            return;
        }
        // 清除引用，防止重复关闭
        this.file = null;

        try {
            // 关闭文件通道
            file.close();
        } catch (IOException e) {
            // 记录关闭文件失败的警告日志，但不抛出异常
            logger.warn("Failed to close a file.", e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * 增加引用计数并返回此实例。
     * </p>
     */
    @Override
    public FileRegion retain() {
        super.retain();
        return this;
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * 将引用计数增加指定的增量并返回此实例。
     * </p>
     */
    @Override
    public FileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * 记录对此对象的访问并返回此实例。
     * </p>
     */
    @Override
    public FileRegion touch() {
        return this;
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * 记录对此对象的访问，添加自定义提示信息，并返回此实例。
     * </p>
     */
    @Override
    public FileRegion touch(Object hint) {
        return this;
    }

    /**
     * 验证文件区域的有效性，确保请求的数据范围不超出实际文件大小。
     * 
     * <p>
     * 此方法在传输返回0字节时调用，用于检查是否因为文件被截断而导致要请求的数据范围超出了
     * 实际文件大小。如果是这种情况，将抛出IOException。
     * </p>
     * 
     * @param region    要验证的DefaultFileRegion实例
     * @param position  相对于文件区域起始位置的偏移量
     * @throws IOException 如果请求的数据范围超出了实际文件大小
     */
    static void validate(DefaultFileRegion region, long position) throws IOException {
        // 如果传输的数据量为0，我们需要检查请求的字节数是否大于
        // 实际文件本身，因为文件可能在磁盘上被截断了。
        //
        // 参见 https://github.com/netty/netty/issues/8868
        long size = region.file.size(); // 获取文件的实际大小
        long count = region.count - position; // 计算需要传输的字节数
        if (region.position + count + position > size) {
            // 如果要传输的总范围超出了文件大小，抛出IOException
            throw new IOException("Underlying file size " + size + " smaller then requested count " + region.count);
        }
    }
}
