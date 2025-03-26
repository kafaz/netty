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
package io.netty.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCounted;

/**
 * 一个可以随机和顺序访问的零个或多个字节（八位字节）序列。
 * 这个接口为一个或多个原始字节数组（{@code byte[]}）和{@linkplain ByteBuffer NIO缓冲区}提供了一个抽象视图。
 *
 * <h3>创建缓冲区</h3>
 *
 * 建议使用{@link Unpooled}中的辅助方法来创建新的缓冲区，而不是调用单个实现的构造函数。
 *
 * <h3>随机访问索引</h3>
 *
 * 就像普通的原始字节数组一样，{@link ByteBuf}使用
 * <a href="https://en.wikipedia.org/wiki/Zero-based_numbering">零基索引</a>。
 * 这意味着第一个字节的索引总是{@code 0}，最后一个字节的索引总是{@link #capacity() capacity - 1}。
 * 例如，要遍历缓冲区的所有字节，无论其内部实现如何，都可以执行以下操作：
 *
 * <pre>
 * {@link ByteBuf} buffer = ...;
 * for (int i = 0; i &lt; buffer.capacity(); i ++) {
 *     byte b = buffer.getByte(i);
 *     System.out.println((char) b);
 * }
 * </pre>
 *
 * <h3>顺序访问索引</h3>
 *
 * {@link ByteBuf}提供了两个指针变量来支持顺序读写操作 -
 * {@link #readerIndex() readerIndex}用于读操作，{@link #writerIndex()
 * writerIndex}用于写操作。
 * 下图显示了缓冲区如何被这两个指针分成三个区域：
 *
 * <pre>
 *      +-------------------+------------------+------------------+
 *      | 可丢弃字节        |  可读字节        |  可写字节        |
 *      |                   |    (内容)        |                  |
 *      +-------------------+------------------+------------------+
 *      |                   |                  |                  |
 *      0      <=      readerIndex   <=   writerIndex    <=    capacity
 * </pre>
 *
 * <h4>可读字节（实际内容）</h4>
 *
 * 这个段是存储实际数据的地方。任何以{@code read}或{@code skip}开头的操作都会
 * 获取或跳过当前{@link #readerIndex() readerIndex}处的数据，并将其增加已读取的字节数。
 * 如果读操作的参数也是{@link ByteBuf}且未指定目标索引，则指定缓冲区的
 * {@link #writerIndex() writerIndex}也会一起增加。
 * <p>
 * 如果没有足够的内容剩余，将抛出{@link IndexOutOfBoundsException}。
 * 新分配、包装或复制的缓冲区的{@link #readerIndex() readerIndex}默认值为{@code 0}。
 *
 * <pre>
 * // 遍历缓冲区的可读字节
 * {@link ByteBuf} buffer = ...;
 * while (buffer.isReadable()) {
 *     System.out.println(buffer.readByte());
 * }
 * </pre>
 *
 * <h4>可写字节</h4>
 *
 * 这个段是一个需要填充的未定义空间。任何以{@code write}开头的操作都会
 * 在当前{@link #writerIndex() writerIndex}处写入数据，并将其增加已写入的字节数。
 * 如果写操作的参数也是{@link ByteBuf}，且未指定源索引，则指定缓冲区的
 * {@link #readerIndex() readerIndex}也会一起增加。
 * <p>
 * 如果没有足够的可写字节剩余，将抛出{@link IndexOutOfBoundsException}。
 * 新分配缓冲区的{@link #writerIndex() writerIndex}默认值为{@code 0}。
 * 包装或复制缓冲区的{@link #writerIndex() writerIndex}默认值为缓冲区的{@link #capacity()
 * capacity}。
 *
 * <pre>
 * // 用随机整数填充缓冲区的可写字节
 * {@link ByteBuf} buffer = ...;
 * while (buffer.maxWritableBytes() >= 4) {
 *     buffer.writeInt(random.nextInt());
 * }
 * </pre>
 *
 * <h4>可丢弃字节</h4>
 *
 * 这个段包含已经被读操作读取过的字节。最初，这个段的大小为{@code 0}，
 * 但随着读操作的执行，其大小会增加到{@link #writerIndex() writerIndex}。
 * 可以通过调用{@link #discardReadBytes()}来丢弃已读取的字节，以回收未使用的区域，如下图所示：
 *
 * <pre>
 *  调用discardReadBytes()之前
 *
 *      +-------------------+------------------+------------------+
 *      | 可丢弃字节        |  可读字节        |  可写字节        |
 *      +-------------------+------------------+------------------+
 *      |                   |                  |                  |
 *      0      <=      readerIndex   <=   writerIndex    <=    capacity
 *
 *
 *  调用discardReadBytes()之后
 *
 *      +------------------+--------------------------------------+
 *      |  可读字节        |    可写字节（获得更多空间）           |
 *      +------------------+--------------------------------------+
 *      |                  |                                      |
 * readerIndex (0) <= writerIndex (减少)        <=        capacity
 * </pre>
 *
 * 请注意，调用{@link #discardReadBytes()}后，不保证可写字节的内容。
 * 在大多数情况下，可写字节不会被移动，甚至可能被完全不同的数据填充，
 * 这取决于底层缓冲区的实现。
 *
 * <h4>清除缓冲区索引</h4>
 *
 * 可以通过调用{@link #clear()}将{@link #readerIndex() readerIndex}和
 * {@link #writerIndex() writerIndex}都设置为{@code 0}。
 * 它不会清除缓冲区内容（例如填充{@code 0}），而只是清除两个指针。
 * 请注意，此操作的语义与{@link ByteBuffer#clear()}不同。
 *
 * <pre>
 *  调用clear()之前
 *
 *      +-------------------+------------------+------------------+
 *      | 可丢弃字节        |  可读字节        |  可写字节        |
 *      +-------------------+------------------+------------------+
 *      |                   |                  |                  |
 *      0      <=      readerIndex   <=   writerIndex    <=    capacity
 *
 *
 *  调用clear()之后
 *
 *      +---------------------------------------------------------+
 *      |             可写字节（获得更多空间）                      |
 *      +---------------------------------------------------------+
 *      |                                                         |
 *      0 = readerIndex = writerIndex            <=            capacity
 * </pre>
 *
 * <h3>搜索操作</h3>
 *
 * 对于简单的单字节搜索，使用{@link #indexOf(int, int, byte)}和{@link #bytesBefore(int, int, byte)}。
 * {@link #bytesBefore(byte)}在处理{@code NUL}终止的字符串时特别有用。
 * 对于复杂的搜索，使用带有{@link ByteProcessor}实现的{@link #forEachByte(int, int, ByteProcessor)}。
 *
 * <h3>标记和重置</h3>
 *
 * 每个缓冲区都有两个标记索引。一个用于存储{@link #readerIndex() readerIndex}，
 * 另一个用于存储{@link #writerIndex() writerIndex}。
 * 你可以通过调用reset方法随时重新定位这两个索引之一。
 * 它的工作方式类似于{@link InputStream}中的mark和reset方法，但没有{@code readlimit}。
 *
 * <h3>派生缓冲区</h3>
 *
 * 你可以通过调用以下方法之一来创建现有缓冲区的视图：
 * <ul>
 * <li>{@link #duplicate()}</li>
 * <li>{@link #slice()}</li>
 * <li>{@link #slice(int, int)}</li>
 * <li>{@link #readSlice(int)}</li>
 * <li>{@link #retainedDuplicate()}</li>
 * <li>{@link #retainedSlice()}</li>
 * <li>{@link #retainedSlice(int, int)}</li>
 * <li>{@link #readRetainedSlice(int)}</li>
 * </ul>
 * 派生缓冲区将有独立的{@link #readerIndex() readerIndex}、
 * {@link #writerIndex() writerIndex}和标记索引，同时它共享其他内部数据表示，
 * 就像NIO缓冲区一样。
 * <p>
 * 如果需要现有缓冲区的完全新副本，请改用{@link #copy()}方法。
 *
 * <h4>非保留和保留的派生缓冲区</h4>
 *
 * 注意，{@link #duplicate()}、{@link #slice()}、{@link #slice(int, int)}和{@link #readSlice(int)}
 * 不会在返回的派生缓冲区上调用{@link #retain()}，因此其引用计数不会增加。
 * 如果你需要创建一个引用计数增加的派生缓冲区，请考虑使用{@link #retainedDuplicate()}、
 * {@link #retainedSlice()}、{@link #retainedSlice(int, int)}和{@link #readRetainedSlice(int)}，
 * 这些方法可能返回产生更少垃圾的缓冲区实现。
 *
 * <h3>转换为现有的JDK类型</h3>
 *
 * <h4>字节数组</h4>
 *
 * 如果{@link ByteBuf}由字节数组（即{@code byte[]}）支持，
 * 你可以通过{@link #array()}方法直接访问它。
 * 要确定缓冲区是否由字节数组支持，应使用{@link #hasArray()}。
 *
 * <h4>NIO缓冲区</h4>
 *
 * 如果{@link ByteBuf}可以转换为共享其内容的NIO {@link ByteBuffer}
 * （即视图缓冲区），你可以通过{@link #nioBuffer()}方法获取它。
 * 要确定缓冲区是否可以转换为NIO缓冲区，使用{@link #nioBufferCount()}。
 *
 * <h4>字符串</h4>
 *
 * 各种{@link #toString(Charset)}方法将{@link ByteBuf}转换为{@link String}。
 * 请注意，{@link #toString()}不是转换方法。
 *
 * <h4>I/O流</h4>
 *
 * 请参考{@link ByteBufInputStream}和{@link ByteBufOutputStream}。
 */
public abstract class ByteBuf implements ReferenceCounted, Comparable<ByteBuf>, ByteBufConvertible {

    /**
     * Returns the number of bytes (octets) this buffer can contain.
     */
    public abstract int capacity();

    /**
     * Adjusts the capacity of this buffer. If the {@code newCapacity} is less than
     * the current
     * capacity, the content of this buffer is truncated. If the {@code newCapacity}
     * is greater
     * than the current capacity, the buffer is appended with unspecified data whose
     * length is
     * {@code (newCapacity - currentCapacity)}.
     *
     * @throws IllegalArgumentException if the {@code newCapacity} is greater than
     *                                  {@link #maxCapacity()}
     */
    public abstract ByteBuf capacity(int newCapacity);

    /**
     * Returns the maximum allowed capacity of this buffer. This value provides an
     * upper
     * bound on {@link #capacity()}.
     */
    public abstract int maxCapacity();

    /**
     * Returns the {@link ByteBufAllocator} which created this buffer.
     */
    public abstract ByteBufAllocator alloc();

    /**
     * Returns the <a href="https://en.wikipedia.org/wiki/Endianness">endianness</a>
     * of this buffer.
     *
     * @deprecated use the Little Endian accessors, e.g. {@code getShortLE},
     *             {@code getIntLE}
     *             instead of creating a buffer with swapped {@code e7668ndianness}.
     */
    @Deprecated
    public abstract ByteOrder order();

    /**
     * Returns a buffer with the specified {@code endianness} which shares the whole
     * region,
     * indexes, and marks of this buffer. Modifying the content, the indexes, or the
     * marks of the
     * returned buffer or this buffer affects each other's content, indexes, and
     * marks. If the
     * specified {@code endianness} is identical to this buffer's byte order, this
     * method can
     * return {@code this}. This method does not modify {@code readerIndex} or
     * {@code writerIndex}
     * of this buffer.
     *
     * @deprecated use the Little Endian accessors, e.g. {@code getShortLE},
     *             {@code getIntLE}
     *             instead of creating a buffer with swapped {@code endianness}.
     */
    @Deprecated
    public abstract ByteBuf order(ByteOrder endianness);

    /**
     * unwrap方法 - 获取被包装缓冲区的底层缓冲区
     * 
     * @return 如果当前缓冲区是包装缓冲区，则返回底层缓冲区实例；
     *         如果不是包装缓冲区，则返回null
     */
    public abstract ByteBuf unwrap();

    /**
     * Returns {@code true} if and only if this buffer is backed by an
     * NIO direct buffer.
     */
    public abstract boolean isDirect();

    /**
     * Returns {@code true} if and only if this buffer is read-only.
     */
    public abstract boolean isReadOnly();

    /**
     * Returns a read-only version of this buffer.
     */
    public abstract ByteBuf asReadOnly();

    /**
     * Returns the {@code readerIndex} of this buffer.
     */
    public abstract int readerIndex();

    /**
     * Sets the {@code readerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code readerIndex} is
     *                                   less than {@code 0} or
     *                                   greater than {@code this.writerIndex}
     */
    public abstract ByteBuf readerIndex(int readerIndex);

    /**
     * Returns the {@code writerIndex} of this buffer.
     */
    public abstract int writerIndex();

    /**
     * Sets the {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code writerIndex} is
     *                                   less than {@code this.readerIndex} or
     *                                   greater than {@code this.capacity}
     */
    public abstract ByteBuf writerIndex(int writerIndex);

    /**
     * Sets the {@code readerIndex} and {@code writerIndex} of this buffer
     * in one shot. This method is useful when you have to worry about the
     * invocation order of {@link #readerIndex(int)} and {@link #writerIndex(int)}
     * methods. For example, the following code will fail:
     *
     * <pre>
     * // Create a buffer whose readerIndex, writerIndex and capacity are
     * // 0, 0 and 8 respectively.
     * {@link ByteBuf} buf = {@link Unpooled}.buffer(8);
     *
     * // IndexOutOfBoundsException is thrown because the specified
     * // readerIndex (2) cannot be greater than the current writerIndex (0).
     * buf.readerIndex(2);
     * buf.writerIndex(4);
     * </pre>
     *
     * The following code will also fail:
     *
     * <pre>
     * // Create a buffer whose readerIndex, writerIndex and capacity are
     * // 0, 8 and 8 respectively.
     * {@link ByteBuf} buf = {@link Unpooled}.wrappedBuffer(new byte[8]);
     *
     * // readerIndex becomes 8.
     * buf.readLong();
     *
     * // IndexOutOfBoundsException is thrown because the specified
     * // writerIndex (4) cannot be less than the current readerIndex (8).
     * buf.writerIndex(4);
     * buf.readerIndex(2);
     * </pre>
     *
     * By contrast, this method guarantees that it never
     * throws an {@link IndexOutOfBoundsException} as long as the specified
     * indexes meet basic constraints, regardless what the current index
     * values of the buffer are:
     *
     * <pre>
     * // No matter what the current state of the buffer is, the following
     * // call always succeeds as long as the capacity of the buffer is not
     * // less than 4.
     * buf.setIndex(2, 4);
     * </pre>
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code readerIndex} is
     *                                   less than 0,
     *                                   if the specified {@code writerIndex} is
     *                                   less than the specified
     *                                   {@code readerIndex} or if the specified
     *                                   {@code writerIndex} is
     *                                   greater than {@code this.capacity}
     */
    public abstract ByteBuf setIndex(int readerIndex, int writerIndex);

    /**
     * Returns the number of readable bytes which is equal to
     * {@code (this.writerIndex - this.readerIndex)}.
     */
    public abstract int readableBytes();

    /**
     * Returns the number of writable bytes which is equal to
     * {@code (this.capacity - this.writerIndex)}.
     */
    public abstract int writableBytes();

    /**
     * Returns the maximum possible number of writable bytes, which is equal to
     * {@code (this.maxCapacity - this.writerIndex)}.
     */
    public abstract int maxWritableBytes();

    /**
     * Returns the maximum number of bytes which can be written for certain without
     * involving
     * an internal reallocation or data-copy. The returned value will be &ge;
     * {@link #writableBytes()}
     * and &le; {@link #maxWritableBytes()}.
     */
    public int maxFastWritableBytes() {
        return writableBytes();
    }

    /**
     * Returns {@code true}
     * if and only if {@code (this.writerIndex - this.readerIndex)} is greater
     * than {@code 0}.
     */
    public abstract boolean isReadable();

    /**
     * Returns {@code true} if and only if this buffer contains equal to or more
     * than the specified number of elements.
     */
    public abstract boolean isReadable(int size);

    /**
     * Returns {@code true}
     * if and only if {@code (this.capacity - this.writerIndex)} is greater
     * than {@code 0}.
     */
    public abstract boolean isWritable();

    /**
     * Returns {@code true} if and only if this buffer has enough room to allow
     * writing the specified number of
     * elements.
     */
    public abstract boolean isWritable(int size);

    /**
     * Sets the {@code readerIndex} and {@code writerIndex} of this buffer to
     * {@code 0}.
     * This method is identical to {@link #setIndex(int, int) setIndex(0, 0)}.
     * <p>
     * Please note that the behavior of this method is different
     * from that of NIO buffer, which sets the {@code limit} to
     * the {@code capacity} of the buffer.
     */
    public abstract ByteBuf clear();

    /**
     * Marks the current {@code readerIndex} in this buffer. You can
     * reposition the current {@code readerIndex} to the marked
     * {@code readerIndex} by calling {@link #resetReaderIndex()}.
     * The initial value of the marked {@code readerIndex} is {@code 0}.
     */
    public abstract ByteBuf markReaderIndex();

    /**
     * Repositions the current {@code readerIndex} to the marked
     * {@code readerIndex} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the current {@code writerIndex} is less
     *                                   than the marked
     *                                   {@code readerIndex}
     */
    public abstract ByteBuf resetReaderIndex();

    /**
     * Marks the current {@code writerIndex} in this buffer. You can
     * reposition the current {@code writerIndex} to the marked
     * {@code writerIndex} by calling {@link #resetWriterIndex()}.
     * The initial value of the marked {@code writerIndex} is {@code 0}.
     */
    public abstract ByteBuf markWriterIndex();

    /**
     * Repositions the current {@code writerIndex} to the marked
     * {@code writerIndex} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the current {@code readerIndex} is
     *                                   greater than the marked
     *                                   {@code writerIndex}
     */
    public abstract ByteBuf resetWriterIndex();

    /**
     * Discards the bytes between the 0th index and {@code readerIndex}.
     * It moves the bytes between {@code readerIndex} and {@code writerIndex}
     * to the 0th index, and sets {@code readerIndex} and {@code writerIndex}
     * to {@code 0} and {@code oldWriterIndex - oldReaderIndex} respectively.
     * <p>
     * Please refer to the class documentation for more detailed explanation.
     */
    public abstract ByteBuf discardReadBytes();

    /**
     * Similar to {@link ByteBuf#discardReadBytes()} except that this method might
     * discard
     * some, all, or none of read bytes depending on its internal implementation to
     * reduce
     * overall memory bandwidth consumption at the cost of potentially additional
     * memory
     * consumption.
     */
    public abstract ByteBuf discardSomeReadBytes();

    /**
     * Expands the buffer {@link #capacity()} to make sure the number of
     * {@linkplain #writableBytes() writable bytes} is equal to or greater than the
     * specified value. If there are enough writable bytes in this buffer, this
     * method
     * returns with no side effect.
     *
     * @param minWritableBytes
     *                         the expected minimum number of writable bytes
     * @throws IndexOutOfBoundsException
     *                                   if {@link #writerIndex()} +
     *                                   {@code minWritableBytes} &gt;
     *                                   {@link #maxCapacity()}.
     * @see #capacity(int)
     */
    public abstract ByteBuf ensureWritable(int minWritableBytes);

    /**
     * Expands the buffer {@link #capacity()} to make sure the number of
     * {@linkplain #writableBytes() writable bytes} is equal to or greater than the
     * specified value. Unlike {@link #ensureWritable(int)}, this method returns a
     * status code.
     *
     * @param minWritableBytes
     *                         the expected minimum number of writable bytes
     * @param force
     *                         When {@link #writerIndex()} +
     *                         {@code minWritableBytes} &gt; {@link #maxCapacity()}:
     *                         <ul>
     *                         <li>{@code true} - the capacity of the buffer is
     *                         expanded to {@link #maxCapacity()}</li>
     *                         <li>{@code false} - the capacity of the buffer is
     *                         unchanged</li>
     *                         </ul>
     * @return {@code 0} if the buffer has enough writable bytes, and its capacity
     *         is unchanged.
     *         {@code 1} if the buffer does not have enough bytes, and its capacity
     *         is unchanged.
     *         {@code 2} if the buffer has enough writable bytes, and its capacity
     *         has been increased.
     *         {@code 3} if the buffer does not have enough bytes, but its capacity
     *         has been
     *         increased to its maximum.
     */
    public abstract int ensureWritable(int minWritableBytes, boolean force);

    /**
     * Gets a boolean at the specified absolute (@code index) in this buffer.
     * This method does not modify the {@code readerIndex} or {@code writerIndex}
     * of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 1} is greater than
     *                                   {@code this.capacity}
     */
    public abstract boolean getBoolean(int index);

    /**
     * Gets a byte at the specified absolute {@code index} in this buffer.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 1} is greater than
     *                                   {@code this.capacity}
     */
    public abstract byte getByte(int index);

    /**
     * Gets an unsigned byte at the specified absolute {@code index} in this
     * buffer. This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 1} is greater than
     *                                   {@code this.capacity}
     */
    public abstract short getUnsignedByte(int index);

    /**
     * Gets a 16-bit short integer at the specified absolute {@code index} in
     * this buffer. This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 2} is greater than
     *                                   {@code this.capacity}
     */
    public abstract short getShort(int index);

    /**
     * Gets a 16-bit short integer at the specified absolute {@code index} in
     * this buffer in Little Endian Byte Order. This method does not modify
     * {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 2} is greater than
     *                                   {@code this.capacity}
     */
    public abstract short getShortLE(int index);

    /**
     * Gets an unsigned 16-bit short integer at the specified absolute
     * {@code index} in this buffer. This method does not modify
     * {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 2} is greater than
     *                                   {@code this.capacity}
     */
    public abstract int getUnsignedShort(int index);

    /**
     * Gets an unsigned 16-bit short integer at the specified absolute
     * {@code index} in this buffer in Little Endian Byte Order.
     * This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 2} is greater than
     *                                   {@code this.capacity}
     */
    public abstract int getUnsignedShortLE(int index);

    /**
     * Gets a 24-bit medium integer at the specified absolute {@code index} in
     * this buffer. This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 3} is greater than
     *                                   {@code this.capacity}
     */
    public abstract int getMedium(int index);

    /**
     * Gets a 24-bit medium integer at the specified absolute {@code index} in
     * this buffer in the Little Endian Byte Order. This method does not
     * modify {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 3} is greater than
     *                                   {@code this.capacity}
     */
    public abstract int getMediumLE(int index);

    /**
     * Gets an unsigned 24-bit medium integer at the specified absolute
     * {@code index} in this buffer. This method does not modify
     * {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 3} is greater than
     *                                   {@code this.capacity}
     */
    public abstract int getUnsignedMedium(int index);

    /**
     * Gets an unsigned 24-bit medium integer at the specified absolute
     * {@code index} in this buffer in Little Endian Byte Order.
     * This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 3} is greater than
     *                                   {@code this.capacity}
     */
    public abstract int getUnsignedMediumLE(int index);

    /**
     * Gets a 32-bit integer at the specified absolute {@code index} in
     * this buffer. This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public abstract int getInt(int index);

    /**
     * Gets a 32-bit integer at the specified absolute {@code index} in
     * this buffer with Little Endian Byte Order. This method does not
     * modify {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public abstract int getIntLE(int index);

    /**
     * Gets an unsigned 32-bit integer at the specified absolute {@code index}
     * in this buffer. This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public abstract long getUnsignedInt(int index);

    /**
     * Gets an unsigned 32-bit integer at the specified absolute {@code index}
     * in this buffer in Little Endian Byte Order. This method does not
     * modify {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public abstract long getUnsignedIntLE(int index);

    /**
     * Gets a 64-bit long integer at the specified absolute {@code index} in
     * this buffer. This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 8} is greater than
     *                                   {@code this.capacity}
     */
    public abstract long getLong(int index);

    /**
     * Gets a 64-bit long integer at the specified absolute {@code index} in
     * this buffer in Little Endian Byte Order. This method does not
     * modify {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 8} is greater than
     *                                   {@code this.capacity}
     */
    public abstract long getLongLE(int index);

    /**
     * Gets a 2-byte UTF-16 character at the specified absolute
     * {@code index} in this buffer. This method does not modify
     * {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 2} is greater than
     *                                   {@code this.capacity}
     */
    public abstract char getChar(int index);

    /**
     * Gets a 32-bit floating point number at the specified absolute
     * {@code index} in this buffer. This method does not modify
     * {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public abstract float getFloat(int index);

    /**
     * Gets a 32-bit floating point number at the specified absolute
     * {@code index} in this buffer in Little Endian Byte Order.
     * This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public float getFloatLE(int index) {
        return Float.intBitsToFloat(getIntLE(index));
    }

    /**
     * Gets a 64-bit floating point number at the specified absolute
     * {@code index} in this buffer. This method does not modify
     * {@code readerIndex} or {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 8} is greater than
     *                                   {@code this.capacity}
     */
    public abstract double getDouble(int index);

    /**
     * Gets a 64-bit floating point number at the specified absolute
     * {@code index} in this buffer in Little Endian Byte Order.
     * This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 8} is greater than
     *                                   {@code this.capacity}
     */
    public double getDoubleLE(int index) {
        return Double.longBitsToDouble(getLongLE(index));
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index} until the destination becomes
     * non-writable. This method is basically same with
     * {@link #getBytes(int, ByteBuf, int, int)}, except that this
     * method increases the {@code writerIndex} of the destination by the
     * number of the transferred bytes while
     * {@link #getBytes(int, ByteBuf, int, int)} does not.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * the source buffer (i.e. {@code this}).
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + dst.writableBytes} is
     *                                   greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf getBytes(int index, ByteBuf dst);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}. This method is basically same
     * with {@link #getBytes(int, ByteBuf, int, int)}, except that this
     * method increases the {@code writerIndex} of the destination by the
     * number of the transferred bytes while
     * {@link #getBytes(int, ByteBuf, int, int)} does not.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * the source buffer (i.e. {@code this}).
     *
     * @param length the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0},
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}, or
     *                                   if {@code length} is greater than
     *                                   {@code dst.writableBytes}
     */
    public abstract ByteBuf getBytes(int index, ByteBuf dst, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex}
     * of both the source (i.e. {@code this}) and the destination.
     *
     * @param dstIndex the first index of the destination
     * @param length   the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0},
     *                                   if the specified {@code dstIndex} is less
     *                                   than {@code 0},
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}, or
     *                                   if {@code dstIndex + length} is greater
     *                                   than
     *                                   {@code dst.capacity}
     */
    public abstract ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + dst.length} is greater
     *                                   than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf getBytes(int index, byte[] dst);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex}
     * of this buffer.
     *
     * @param dstIndex the first index of the destination
     * @param length   the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0},
     *                                   if the specified {@code dstIndex} is less
     *                                   than {@code 0},
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}, or
     *                                   if {@code dstIndex + length} is greater
     *                                   than
     *                                   {@code dst.length}
     */
    public abstract ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index} until the destination's position
     * reaches its limit.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer while the destination's {@code position} will be increased.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + dst.remaining()} is
     *                                   greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf getBytes(int index, ByteBuffer dst);

    /**
     * Transfers this buffer's data to the specified stream starting at the
     * specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @param length the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}
     * @throws IOException
     *                                   if the specified stream threw an exception
     *                                   during I/O
     */
    public abstract ByteBuf getBytes(int index, OutputStream out, int length) throws IOException;

    /**
     * Transfers this buffer's data to the specified channel starting at the
     * specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @param length the maximum number of bytes to transfer
     *
     * @return the actual number of bytes written out to the specified channel
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}
     * @throws IOException
     *                                   if the specified channel threw an exception
     *                                   during I/O
     */
    public abstract int getBytes(int index, GatheringByteChannel out, int length) throws IOException;

    /**
     * Transfers this buffer's data starting at the specified absolute {@code index}
     * to the specified channel starting at the given file position.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer. This method does not modify the channel's position.
     *
     * @param position the file position at which the transfer is to begin
     * @param length   the maximum number of bytes to transfer
     *
     * @return the actual number of bytes written out to the specified channel
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}
     * @throws IOException
     *                                   if the specified channel threw an exception
     *                                   during I/O
     */
    public abstract int getBytes(int index, FileChannel out, long position, int length) throws IOException;

    /**
     * Gets a {@link CharSequence} with the given length at the given index.
     *
     * @param length  the length to read
     * @param charset that should be used
     * @return the sequence
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     */
    public abstract CharSequence getCharSequence(int index, int length, Charset charset);

    /**
     * Sets the specified boolean at the specified absolute {@code index} in this
     * buffer.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 1} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setBoolean(int index, boolean value);

    /**
     * Sets the specified byte at the specified absolute {@code index} in this
     * buffer. The 24 high-order bits of the specified value are ignored.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 1} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setByte(int index, int value);

    /**
     * Sets the specified 16-bit short integer at the specified absolute
     * {@code index} in this buffer. The 16 high-order bits of the specified
     * value are ignored.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 2} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setShort(int index, int value);

    /**
     * Sets the specified 16-bit short integer at the specified absolute
     * {@code index} in this buffer with the Little Endian Byte Order.
     * The 16 high-order bits of the specified value are ignored.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 2} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setShortLE(int index, int value);

    /**
     * Sets the specified 24-bit medium integer at the specified absolute
     * {@code index} in this buffer. Please note that the most significant
     * byte is ignored in the specified value.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 3} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setMedium(int index, int value);

    /**
     * Sets the specified 24-bit medium integer at the specified absolute
     * {@code index} in this buffer in the Little Endian Byte Order.
     * Please note that the most significant byte is ignored in the
     * specified value.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 3} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setMediumLE(int index, int value);

    /**
     * Sets the specified 32-bit integer at the specified absolute
     * {@code index} in this buffer.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setInt(int index, int value);

    /**
     * Sets the specified 32-bit integer at the specified absolute
     * {@code index} in this buffer with Little Endian byte order
     * .
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setIntLE(int index, int value);

    /**
     * Sets the specified 64-bit long integer at the specified absolute
     * {@code index} in this buffer.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 8} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setLong(int index, long value);

    /**
     * Sets the specified 64-bit long integer at the specified absolute
     * {@code index} in this buffer in Little Endian Byte Order.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 8} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setLongLE(int index, long value);

    /**
     * Sets the specified 2-byte UTF-16 character at the specified absolute
     * {@code index} in this buffer.
     * The 16 high-order bits of the specified value are ignored.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 2} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setChar(int index, int value);

    /**
     * Sets the specified 32-bit floating-point number at the specified
     * absolute {@code index} in this buffer.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setFloat(int index, float value);

    /**
     * Sets the specified 32-bit floating point number at the specified
     * absolute {@code index} in this buffer in Little Endian Byte Order.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 4} is greater than
     *                                   {@code this.capacity}
     */
    public ByteBuf setFloatLE(int index, float value) {
        return setIntLE(index, Float.floatToRawIntBits(value));
    }

    /**
     * Sets the specified 64-bit floating point number at the specified
     * absolute {@code index} in this buffer.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 8} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setDouble(int index, double value);

    /**
     * Sets the specified 64-bit floating point number at the specified
     * absolute {@code index} in this buffer in Little Endian Byte Order.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   {@code index + 8} is greater than
     *                                   {@code this.capacity}
     */
    public ByteBuf setDoubleLE(int index, double value) {
        return setLongLE(index, Double.doubleToRawLongBits(value));
    }

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index} until the source buffer becomes
     * unreadable. This method is basically same with
     * {@link #setBytes(int, ByteBuf, int, int)}, except that this
     * method increases the {@code readerIndex} of the source buffer by
     * the number of the transferred bytes while
     * {@link #setBytes(int, ByteBuf, int, int)} does not.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer (i.e. {@code this}).
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + src.readableBytes} is
     *                                   greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setBytes(int index, ByteBuf src);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index}. This method is basically same
     * with {@link #setBytes(int, ByteBuf, int, int)}, except that this
     * method increases the {@code readerIndex} of the source buffer by
     * the number of the transferred bytes while
     * {@link #setBytes(int, ByteBuf, int, int)} does not.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer (i.e. {@code this}).
     *
     * @param length the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0},
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}, or
     *                                   if {@code length} is greater than
     *                                   {@code src.readableBytes}
     */
    public abstract ByteBuf setBytes(int index, ByteBuf src, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex}
     * of both the source (i.e. {@code this}) and the destination.
     *
     * @param srcIndex the first index of the source
     * @param length   the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0},
     *                                   if the specified {@code srcIndex} is less
     *                                   than {@code 0},
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}, or
     *                                   if {@code srcIndex + length} is greater
     *                                   than
     *                                   {@code src.capacity}
     */
    public abstract ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length);

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + src.length} is greater
     *                                   than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setBytes(int index, byte[] src);

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0},
     *                                   if the specified {@code srcIndex} is less
     *                                   than {@code 0},
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}, or
     *                                   if {@code srcIndex + length} is greater
     *                                   than {@code src.length}
     */
    public abstract ByteBuf setBytes(int index, byte[] src, int srcIndex, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index} until the source buffer's position
     * reaches its limit.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + src.remaining()} is
     *                                   greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setBytes(int index, ByteBuffer src);

    /**
     * Transfers the content of the specified source stream to this buffer
     * starting at the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @param length the number of bytes to transfer
     *
     * @return the actual number of bytes read in from the specified channel.
     *         {@code -1} if the specified {@link InputStream} reached EOF.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}
     * @throws IOException
     *                                   if the specified stream threw an exception
     *                                   during I/O
     */
    public abstract int setBytes(int index, InputStream in, int length) throws IOException;

    /**
     * Transfers the content of the specified source channel to this buffer
     * starting at the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @param length the maximum number of bytes to transfer
     *
     * @return the actual number of bytes read in from the specified channel.
     *         {@code -1} if the specified channel is closed or it reached EOF.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}
     * @throws IOException
     *                                   if the specified channel threw an exception
     *                                   during I/O
     */
    public abstract int setBytes(int index, ScatteringByteChannel in, int length) throws IOException;

    /**
     * Transfers the content of the specified source channel starting at the given
     * file position
     * to this buffer starting at the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer. This method does not modify the channel's position.
     *
     * @param position the file position at which the transfer is to begin
     * @param length   the maximum number of bytes to transfer
     *
     * @return the actual number of bytes read in from the specified channel.
     *         {@code -1} if the specified channel is closed or it reached EOF.
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}
     * @throws IOException
     *                                   if the specified channel threw an exception
     *                                   during I/O
     */
    public abstract int setBytes(int index, FileChannel in, long position, int length) throws IOException;

    /**
     * Fills this buffer with <tt>NUL (0x00)</tt> starting at the specified
     * absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @param length the number of <tt>NUL</tt>s to write to the buffer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf setZero(int index, int length);

    /**
     * Writes the specified {@link CharSequence} at the given {@code index}.
     * The {@code writerIndex} is not modified by this method.
     *
     * @param index    on which the sequence should be written
     * @param sequence to write
     * @param charset  that should be used.
     * @return the written number of bytes.
     * @throws IndexOutOfBoundsException
     *                                   if the sequence at the given index would be
     *                                   out of bounds of the buffer capacity
     */
    public abstract int setCharSequence(int index, CharSequence sequence, Charset charset);

    /**
     * Gets a boolean at the current {@code readerIndex} and increases
     * the {@code readerIndex} by {@code 1} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 1}
     */
    public abstract boolean readBoolean();

    /**
     * Gets a byte at the current {@code readerIndex} and increases
     * the {@code readerIndex} by {@code 1} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 1}
     */
    public abstract byte readByte();

    /**
     * Gets an unsigned byte at the current {@code readerIndex} and increases
     * the {@code readerIndex} by {@code 1} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 1}
     */
    public abstract short readUnsignedByte();

    /**
     * Gets a 16-bit short integer at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 2} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 2}
     */
    public abstract short readShort();

    /**
     * Gets a 16-bit short integer at the current {@code readerIndex}
     * in the Little Endian Byte Order and increases the {@code readerIndex}
     * by {@code 2} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 2}
     */
    public abstract short readShortLE();

    /**
     * Gets an unsigned 16-bit short integer at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 2} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 2}
     */
    public abstract int readUnsignedShort();

    /**
     * Gets an unsigned 16-bit short integer at the current {@code readerIndex}
     * in the Little Endian Byte Order and increases the {@code readerIndex}
     * by {@code 2} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 2}
     */
    public abstract int readUnsignedShortLE();

    /**
     * Gets a 24-bit medium integer at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 3} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 3}
     */
    public abstract int readMedium();

    /**
     * Gets a 24-bit medium integer at the current {@code readerIndex}
     * in the Little Endian Byte Order and increases the
     * {@code readerIndex} by {@code 3} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 3}
     */
    public abstract int readMediumLE();

    /**
     * Gets an unsigned 24-bit medium integer at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 3} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 3}
     */
    public abstract int readUnsignedMedium();

    /**
     * Gets an unsigned 24-bit medium integer at the current {@code readerIndex}
     * in the Little Endian Byte Order and increases the {@code readerIndex}
     * by {@code 3} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 3}
     */
    public abstract int readUnsignedMediumLE();

    /**
     * Gets a 32-bit integer at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 4}
     */
    public abstract int readInt();

    /**
     * Gets a 32-bit integer at the current {@code readerIndex}
     * in the Little Endian Byte Order and increases the {@code readerIndex}
     * by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 4}
     */
    public abstract int readIntLE();

    /**
     * Gets an unsigned 32-bit integer at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 4}
     */
    public abstract long readUnsignedInt();

    /**
     * Gets an unsigned 32-bit integer at the current {@code readerIndex}
     * in the Little Endian Byte Order and increases the {@code readerIndex}
     * by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 4}
     */
    public abstract long readUnsignedIntLE();

    /**
     * Gets a 64-bit integer at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 8} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 8}
     */
    public abstract long readLong();

    /**
     * Gets a 64-bit integer at the current {@code readerIndex}
     * in the Little Endian Byte Order and increases the {@code readerIndex}
     * by {@code 8} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 8}
     */
    public abstract long readLongLE();

    /**
     * Gets a 2-byte UTF-16 character at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 2} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 2}
     */
    public abstract char readChar();

    /**
     * Gets a 32-bit floating point number at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 4}
     */
    public abstract float readFloat();

    /**
     * Gets a 32-bit floating point number at the current {@code readerIndex}
     * in Little Endian Byte Order and increases the {@code readerIndex}
     * by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 4}
     */
    public float readFloatLE() {
        return Float.intBitsToFloat(readIntLE());
    }

    /**
     * Gets a 64-bit floating point number at the current {@code readerIndex}
     * and increases the {@code readerIndex} by {@code 8} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 8}
     */
    public abstract double readDouble();

    /**
     * Gets a 64-bit floating point number at the current {@code readerIndex}
     * in Little Endian Byte Order and increases the {@code readerIndex}
     * by {@code 8} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code this.readableBytes} is less than
     *                                   {@code 8}
     */
    public double readDoubleLE() {
        return Double.longBitsToDouble(readLongLE());
    }

    /**
     * Transfers this buffer's data to a newly created buffer starting at
     * the current {@code readerIndex} and increases the {@code readerIndex}
     * by the number of the transferred bytes (= {@code length}).
     * The returned buffer's {@code readerIndex} and {@code writerIndex} are
     * {@code 0} and {@code length} respectively.
     *
     * @param length the number of bytes to transfer
     *
     * @return the newly created buffer which contains the transferred bytes
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     */
    public abstract ByteBuf readBytes(int length);

    /**
     * Returns a new slice of this buffer's sub-region starting at the current
     * {@code readerIndex} and increases the {@code readerIndex} by the size
     * of the new slice (= {@code length}).
     * <p>
     * Also be aware that this method will NOT call {@link #retain()} and so the
     * reference count will NOT be increased.
     *
     * @param length the size of the new slice
     *
     * @return the newly created slice
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     */
    public abstract ByteBuf readSlice(int length);

    /**
     * Returns a new retained slice of this buffer's sub-region starting at the
     * current
     * {@code readerIndex} and increases the {@code readerIndex} by the size
     * of the new slice (= {@code length}).
     * <p>
     * Note that this method returns a {@linkplain #retain() retained} buffer unlike
     * {@link #readSlice(int)}.
     * This method behaves similarly to {@code readSlice(...).retain()} except that
     * this method may return
     * a buffer implementation that produces less garbage.
     *
     * @param length the size of the new slice
     *
     * @return the newly created slice
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     */
    public abstract ByteBuf readRetainedSlice(int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code readerIndex} until the destination becomes
     * non-writable, and increases the {@code readerIndex} by the number of the
     * transferred bytes. This method is basically same with
     * {@link #readBytes(ByteBuf, int, int)}, except that this method
     * increases the {@code writerIndex} of the destination by the number of
     * the transferred bytes while {@link #readBytes(ByteBuf, int, int)}
     * does not.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code dst.writableBytes} is greater
     *                                   than
     *                                   {@code this.readableBytes}
     */
    public abstract ByteBuf readBytes(ByteBuf dst);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code readerIndex} and increases the {@code readerIndex}
     * by the number of the transferred bytes (= {@code length}). This method
     * is basically same with {@link #readBytes(ByteBuf, int, int)},
     * except that this method increases the {@code writerIndex} of the
     * destination by the number of the transferred bytes (= {@code length})
     * while {@link #readBytes(ByteBuf, int, int)} does not.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes} or
     *                                   if {@code length} is greater than
     *                                   {@code dst.writableBytes}
     */
    public abstract ByteBuf readBytes(ByteBuf dst, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code readerIndex} and increases the {@code readerIndex}
     * by the number of the transferred bytes (= {@code length}).
     *
     * @param dstIndex the first index of the destination
     * @param length   the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code dstIndex} is less
     *                                   than {@code 0},
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}, or
     *                                   if {@code dstIndex + length} is greater
     *                                   than
     *                                   {@code dst.capacity}
     */
    public abstract ByteBuf readBytes(ByteBuf dst, int dstIndex, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code readerIndex} and increases the {@code readerIndex}
     * by the number of the transferred bytes (= {@code dst.length}).
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code dst.length} is greater than
     *                                   {@code this.readableBytes}
     */
    public abstract ByteBuf readBytes(byte[] dst);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code readerIndex} and increases the {@code readerIndex}
     * by the number of the transferred bytes (= {@code length}).
     *
     * @param dstIndex the first index of the destination
     * @param length   the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code dstIndex} is less
     *                                   than {@code 0},
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}, or
     *                                   if {@code dstIndex + length} is greater
     *                                   than {@code dst.length}
     */
    public abstract ByteBuf readBytes(byte[] dst, int dstIndex, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code readerIndex} until the destination's position
     * reaches its limit, and increases the {@code readerIndex} by the
     * number of the transferred bytes.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code dst.remaining()} is greater than
     *                                   {@code this.readableBytes}
     */
    public abstract ByteBuf readBytes(ByteBuffer dst);

    /**
     * Transfers this buffer's data to the specified stream starting at the
     * current {@code readerIndex}.
     *
     * @param length the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     * @throws IOException
     *                                   if the specified stream threw an exception
     *                                   during I/O
     */
    public abstract ByteBuf readBytes(OutputStream out, int length) throws IOException;

    /**
     * Transfers this buffer's data to the specified stream starting at the
     * current {@code readerIndex}.
     *
     * @param length the maximum number of bytes to transfer
     *
     * @return the actual number of bytes written out to the specified channel
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     * @throws IOException
     *                                   if the specified channel threw an exception
     *                                   during I/O
     */
    public abstract int readBytes(GatheringByteChannel out, int length) throws IOException;

    /**
     * Gets a {@link CharSequence} with the given length at the current
     * {@code readerIndex}
     * and increases the {@code readerIndex} by the given length.
     *
     * @param length  the length to read
     * @param charset that should be used
     * @return the sequence
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     */
    public abstract CharSequence readCharSequence(int length, Charset charset);

    /**
     * Transfers this buffer's data starting at the current {@code readerIndex}
     * to the specified channel starting at the given file position.
     * This method does not modify the channel's position.
     *
     * @param position the file position at which the transfer is to begin
     * @param length   the maximum number of bytes to transfer
     *
     * @return the actual number of bytes written out to the specified channel
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     * @throws IOException
     *                                   if the specified channel threw an exception
     *                                   during I/O
     */
    public abstract int readBytes(FileChannel out, long position, int length) throws IOException;

    /**
     * Increases the current {@code readerIndex} by the specified
     * {@code length} in this buffer.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     */
    public abstract ByteBuf skipBytes(int length);

    /**
     * Sets the specified boolean at the current {@code writerIndex}
     * and increases the {@code writerIndex} by {@code 1} in this buffer.
     * If {@code this.writableBytes} is less than {@code 1},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeBoolean(boolean value);

    /**
     * Sets the specified byte at the current {@code writerIndex}
     * and increases the {@code writerIndex} by {@code 1} in this buffer.
     * The 24 high-order bits of the specified value are ignored.
     * If {@code this.writableBytes} is less than {@code 1},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeByte(int value);

    /**
     * Sets the specified 16-bit short integer at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 2}
     * in this buffer. The 16 high-order bits of the specified value are ignored.
     * If {@code this.writableBytes} is less than {@code 2},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeShort(int value);

    /**
     * Sets the specified 16-bit short integer in the Little Endian Byte
     * Order at the current {@code writerIndex} and increases the
     * {@code writerIndex} by {@code 2} in this buffer.
     * The 16 high-order bits of the specified value are ignored.
     * If {@code this.writableBytes} is less than {@code 2},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeShortLE(int value);

    /**
     * Sets the specified 24-bit medium integer at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 3}
     * in this buffer.
     * If {@code this.writableBytes} is less than {@code 3},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeMedium(int value);

    /**
     * Sets the specified 24-bit medium integer at the current
     * {@code writerIndex} in the Little Endian Byte Order and
     * increases the {@code writerIndex} by {@code 3} in this
     * buffer.
     * If {@code this.writableBytes} is less than {@code 3},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeMediumLE(int value);

    /**
     * Sets the specified 32-bit integer at the current {@code writerIndex}
     * and increases the {@code writerIndex} by {@code 4} in this buffer.
     * If {@code this.writableBytes} is less than {@code 4},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeInt(int value);

    /**
     * Sets the specified 32-bit integer at the current {@code writerIndex}
     * in the Little Endian Byte Order and increases the {@code writerIndex}
     * by {@code 4} in this buffer.
     * If {@code this.writableBytes} is less than {@code 4},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeIntLE(int value);

    /**
     * Sets the specified 64-bit long integer at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 8}
     * in this buffer.
     * If {@code this.writableBytes} is less than {@code 8},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeLong(long value);

    /**
     * Sets the specified 64-bit long integer at the current
     * {@code writerIndex} in the Little Endian Byte Order and
     * increases the {@code writerIndex} by {@code 8}
     * in this buffer.
     * If {@code this.writableBytes} is less than {@code 8},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeLongLE(long value);

    /**
     * Sets the specified 2-byte UTF-16 character at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 2}
     * in this buffer. The 16 high-order bits of the specified value are ignored.
     * If {@code this.writableBytes} is less than {@code 2},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeChar(int value);

    /**
     * Sets the specified 32-bit floating point number at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 4}
     * in this buffer.
     * If {@code this.writableBytes} is less than {@code 4},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeFloat(float value);

    /**
     * Sets the specified 32-bit floating point number at the current
     * {@code writerIndex} in Little Endian Byte Order and increases
     * the {@code writerIndex} by {@code 4} in this buffer.
     * If {@code this.writableBytes} is less than {@code 4},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public ByteBuf writeFloatLE(float value) {
        return writeIntLE(Float.floatToRawIntBits(value));
    }

    /**
     * Sets the specified 64-bit floating point number at the current
     * {@code writerIndex} and increases the {@code writerIndex} by {@code 8}
     * in this buffer.
     * If {@code this.writableBytes} is less than {@code 8},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeDouble(double value);

    /**
     * Sets the specified 64-bit floating point number at the current
     * {@code writerIndex} in Little Endian Byte Order and increases
     * the {@code writerIndex} by {@code 8} in this buffer.
     * If {@code this.writableBytes} is less than {@code 8},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public ByteBuf writeDoubleLE(double value) {
        return writeLongLE(Double.doubleToRawLongBits(value));
    }

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writerIndex} until the source buffer becomes
     * unreadable, and increases the {@code writerIndex} by the number of
     * the transferred bytes. This method is basically same with
     * {@link #writeBytes(ByteBuf, int, int)}, except that this method
     * increases the {@code readerIndex} of the source buffer by the number of
     * the transferred bytes while {@link #writeBytes(ByteBuf, int, int)}
     * does not.
     * If {@code this.writableBytes} is less than {@code src.readableBytes},
     * {@link #ensureWritable(int)} will be called in an attempt to expand
     * capacity to accommodate.
     */
    public abstract ByteBuf writeBytes(ByteBuf src);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writerIndex} and increases the {@code writerIndex}
     * by the number of the transferred bytes (= {@code length}). This method
     * is basically same with {@link #writeBytes(ByteBuf, int, int)},
     * except that this method increases the {@code readerIndex} of the source
     * buffer by the number of the transferred bytes (= {@code length}) while
     * {@link #writeBytes(ByteBuf, int, int)} does not.
     * If {@code this.writableBytes} is less than {@code length},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     *
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if {@code length} is greater then
     *                                   {@code src.readableBytes}
     */
    public abstract ByteBuf writeBytes(ByteBuf src, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writerIndex} and increases the {@code writerIndex}
     * by the number of the transferred bytes (= {@code length}).
     * If {@code this.writableBytes} is less than {@code length},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     *
     * @param srcIndex the first index of the source
     * @param length   the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code srcIndex} is less
     *                                   than {@code 0}, or
     *                                   if {@code srcIndex + length} is greater
     *                                   than {@code src.capacity}
     */
    public abstract ByteBuf writeBytes(ByteBuf src, int srcIndex, int length);

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the current {@code writerIndex} and increases the {@code writerIndex}
     * by the number of the transferred bytes (= {@code src.length}).
     * If {@code this.writableBytes} is less than {@code src.length},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     */
    public abstract ByteBuf writeBytes(byte[] src);

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the current {@code writerIndex} and increases the {@code writerIndex}
     * by the number of the transferred bytes (= {@code length}).
     * If {@code this.writableBytes} is less than {@code length},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     *
     * @param srcIndex the first index of the source
     * @param length   the number of bytes to transfer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code srcIndex} is less
     *                                   than {@code 0}, or
     *                                   if {@code srcIndex + length} is greater
     *                                   than {@code src.length}
     */
    public abstract ByteBuf writeBytes(byte[] src, int srcIndex, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the current {@code writerIndex} until the source buffer's position
     * reaches its limit, and increases the {@code writerIndex} by the
     * number of the transferred bytes.
     * If {@code this.writableBytes} is less than {@code src.remaining()},
     * {@link #ensureWritable(int)} will be called in an attempt to expand
     * capacity to accommodate.
     */
    public abstract ByteBuf writeBytes(ByteBuffer src);

    /**
     * Transfers the content of the specified stream to this buffer
     * starting at the current {@code writerIndex} and increases the
     * {@code writerIndex} by the number of the transferred bytes.
     * If {@code this.writableBytes} is less than {@code length},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     *
     * @param length the number of bytes to transfer
     *
     * @return the actual number of bytes read in from the specified channel.
     *         {@code -1} if the specified {@link InputStream} reached EOF.
     *
     * @throws IOException if the specified stream threw an exception during I/O
     */
    public abstract int writeBytes(InputStream in, int length) throws IOException;

    /**
     * Transfers the content of the specified channel to this buffer
     * starting at the current {@code writerIndex} and increases the
     * {@code writerIndex} by the number of the transferred bytes.
     * If {@code this.writableBytes} is less than {@code length},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     *
     * @param length the maximum number of bytes to transfer
     *
     * @return the actual number of bytes read in from the specified channel.
     *         {@code -1} if the specified channel is closed or it reached EOF.
     *
     * @throws IOException
     *                     if the specified channel threw an exception during I/O
     */
    public abstract int writeBytes(ScatteringByteChannel in, int length) throws IOException;

    /**
     * Transfers the content of the specified channel starting at the given file
     * position
     * to this buffer starting at the specified absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer. This method does not modify the channel's position.
     * If {@code this.writableBytes} is less than {@code length},
     * {@link #ensureWritable(int)}
     * will be called in an attempt to expand capacity to accommodate.
     *
     * @param position the file position at which the transfer is to begin
     * @param length   the maximum number of bytes to transfer
     *
     * @return the actual number of bytes read in from the specified channel.
     *         {@code -1} if the specified channel is closed or it reached EOF.
     *
     * @throws IOException
     *                     if the specified channel threw an exception during I/O
     */
    public abstract int writeBytes(FileChannel in, long position, int length) throws IOException;

    /**
     * Fills this buffer with <tt>NUL (0x00)</tt> starting at the specified
     * absolute {@code index}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @param length the number of <tt>NUL</tt>s to write to the buffer
     *
     * @throws IndexOutOfBoundsException
     *                                   if the specified {@code index} is less than
     *                                   {@code 0} or
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}
     */
    public abstract ByteBuf writeZero(int length);

    /**
     * Writes the specified {@link CharSequence} at the current {@code writerIndex}
     * and increases
     * the {@code writerIndex} by the written bytes.
     * in this buffer.
     * If {@code this.writableBytes} is not large enough to write the whole
     * sequence,
     * {@link #ensureWritable(int)} will be called in an attempt to expand capacity
     * to accommodate.
     *
     * @param sequence to write
     * @param charset  that should be used
     * @return the written number of bytes
     */
    public abstract int writeCharSequence(CharSequence sequence, Charset charset);

    /**
     * Locates the first occurrence of the specified {@code value} in this
     * buffer. The search takes place from the specified {@code fromIndex}
     * (inclusive) to the specified {@code toIndex} (exclusive).
     * <p>
     * If {@code fromIndex} is greater than {@code toIndex}, the search is
     * performed in a reversed order from {@code fromIndex} (exclusive)
     * down to {@code toIndex} (inclusive).
     * <p>
     * Note that the lower index is always included and higher always excluded.
     * <p>
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @return the absolute index of the first occurrence if found.
     *         {@code -1} otherwise.
     */
    public abstract int indexOf(int fromIndex, int toIndex, byte value);

    /**
     * Locates the first occurrence of the specified {@code value} in this
     * buffer. The search takes place from the current {@code readerIndex}
     * (inclusive) to the current {@code writerIndex} (exclusive).
     * <p>
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @return the number of bytes between the current {@code readerIndex}
     *         and the first occurrence if found. {@code -1} otherwise.
     */
    public abstract int bytesBefore(byte value);

    /**
     * Locates the first occurrence of the specified {@code value} in this
     * buffer. The search starts from the current {@code readerIndex}
     * (inclusive) and lasts for the specified {@code length}.
     * <p>
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @return the number of bytes between the current {@code readerIndex}
     *         and the first occurrence if found. {@code -1} otherwise.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code length} is greater than
     *                                   {@code this.readableBytes}
     */
    public abstract int bytesBefore(int length, byte value);

    /**
     * Locates the first occurrence of the specified {@code value} in this
     * buffer. The search starts from the specified {@code index} (inclusive)
     * and lasts for the specified {@code length}.
     * <p>
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @return the number of bytes between the specified {@code index}
     *         and the first occurrence if found. {@code -1} otherwise.
     *
     * @throws IndexOutOfBoundsException
     *                                   if {@code index + length} is greater than
     *                                   {@code this.capacity}
     */
    public abstract int bytesBefore(int index, int length, byte value);

    /**
     * Iterates over the readable bytes of this buffer with the specified
     * {@code processor} in ascending order.
     *
     * @return {@code -1} if the processor iterated to or beyond the end of the
     *         readable bytes.
     *         The last-visited index If the {@link ByteProcessor#process(byte)}
     *         returned {@code false}.
     */
    public abstract int forEachByte(ByteProcessor processor);

    /**
     * Iterates over the specified area of this buffer with the specified
     * {@code processor} in ascending order.
     * (i.e. {@code index}, {@code (index + 1)}, .. {@code (index + length - 1)})
     *
     * @return {@code -1} if the processor iterated to or beyond the end of the
     *         specified area.
     *         The last-visited index If the {@link ByteProcessor#process(byte)}
     *         returned {@code false}.
     */
    public abstract int forEachByte(int index, int length, ByteProcessor processor);

    /**
     * Iterates over the readable bytes of this buffer with the specified
     * {@code processor} in descending order.
     *
     * @return {@code -1} if the processor iterated to or beyond the beginning of
     *         the readable bytes.
     *         The last-visited index If the {@link ByteProcessor#process(byte)}
     *         returned {@code false}.
     */
    public abstract int forEachByteDesc(ByteProcessor processor);

    /**
     * Iterates over the specified area of this buffer with the specified
     * {@code processor} in descending order.
     * (i.e. {@code (index + length - 1)}, {@code (index + length - 2)}, ...
     * {@code index})
     *
     *
     * @return {@code -1} if the processor iterated to or beyond the beginning of
     *         the specified area.
     *         The last-visited index If the {@link ByteProcessor#process(byte)}
     *         returned {@code false}.
     */
    public abstract int forEachByteDesc(int index, int length, ByteProcessor processor);

    /**
     * Returns a copy of this buffer's readable bytes. Modifying the content
     * of the returned buffer or this buffer does not affect each other at all.
     * This method is identical to
     * {@code buf.copy(buf.readerIndex(), buf.readableBytes())}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     */
    public abstract ByteBuf copy();

    /**
     * Returns a copy of this buffer's sub-region. Modifying the content of
     * the returned buffer or this buffer does not affect each other at all.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     */
    public abstract ByteBuf copy(int index, int length);

    /**
     * Returns a slice of this buffer's readable bytes. Modifying the content
     * of the returned buffer or this buffer affects each other's content
     * while they maintain separate indexes and marks. This method is
     * identical to {@code buf.slice(buf.readerIndex(), buf.readableBytes())}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * <p>
     * Also be aware that this method will NOT call {@link #retain()} and so the
     * reference count will NOT be increased.
     */
    public abstract ByteBuf slice();

    /**
     * Returns a retained slice of this buffer's readable bytes. Modifying the
     * content
     * of the returned buffer or this buffer affects each other's content
     * while they maintain separate indexes and marks. This method is
     * identical to {@code buf.slice(buf.readerIndex(), buf.readableBytes())}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * <p>
     * Note that this method returns a {@linkplain #retain() retained} buffer unlike
     * {@link #slice()}.
     * This method behaves similarly to {@code slice().retain()} except that this
     * method may return
     * a buffer implementation that produces less garbage.
     */
    public abstract ByteBuf retainedSlice();

    /**
     * Returns a slice of this buffer's sub-region. Modifying the content of
     * the returned buffer or this buffer affects each other's content while
     * they maintain separate indexes and marks.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * <p>
     * Also be aware that this method will NOT call {@link #retain()} and so the
     * reference count will NOT be increased.
     */
    public abstract ByteBuf slice(int index, int length);

    /**
     * Returns a retained slice of this buffer's sub-region. Modifying the content
     * of
     * the returned buffer or this buffer affects each other's content while
     * they maintain separate indexes and marks.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * <p>
     * Note that this method returns a {@linkplain #retain() retained} buffer unlike
     * {@link #slice(int, int)}.
     * This method behaves similarly to {@code slice(...).retain()} except that this
     * method may return
     * a buffer implementation that produces less garbage.
     */
    public abstract ByteBuf retainedSlice(int index, int length);

    /**
     * Returns a buffer which shares the whole region of this buffer.
     * Modifying the content of the returned buffer or this buffer affects
     * each other's content while they maintain separate indexes and marks.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * <p>
     * The reader and writer marks will not be duplicated. Also be aware that this
     * method will
     * NOT call {@link #retain()} and so the reference count will NOT be increased.
     * 
     * @return A buffer whose readable content is equivalent to the buffer returned
     *         by {@link #slice()}.
     *         However this buffer will share the capacity of the underlying buffer,
     *         and therefore allows access to all of the
     *         underlying content if necessary.
     */
    public abstract ByteBuf duplicate();

    /**
     * Returns a retained buffer which shares the whole region of this buffer.
     * Modifying the content of the returned buffer or this buffer affects
     * each other's content while they maintain separate indexes and marks.
     * This method is identical to {@code buf.slice(0, buf.capacity())}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * <p>
     * Note that this method returns a {@linkplain #retain() retained} buffer unlike
     * {@link #slice(int, int)}.
     * This method behaves similarly to {@code duplicate().retain()} except that
     * this method may return
     * a buffer implementation that produces less garbage.
     */
    public abstract ByteBuf retainedDuplicate();

    /**
     * Returns the maximum number of NIO {@link ByteBuffer}s that consist this
     * buffer. Note that {@link #nioBuffers()}
     * or {@link #nioBuffers(int, int)} might return a less number of
     * {@link ByteBuffer}s.
     *
     * @return {@code -1} if this buffer has no underlying {@link ByteBuffer}.
     *         the number of the underlying {@link ByteBuffer}s if this buffer has
     *         at least one underlying
     *         {@link ByteBuffer}. Note that this method does not return {@code 0}
     *         to avoid confusion.
     *
     * @see #nioBuffer()
     * @see #nioBuffer(int, int)
     * @see #nioBuffers()
     * @see #nioBuffers(int, int)
     */
    public abstract int nioBufferCount();

    /**
     * Exposes this buffer's readable bytes as an NIO {@link ByteBuffer}. The
     * returned buffer
     * either share or contains the copied content of this buffer, while changing
     * the position
     * and limit of the returned NIO buffer does not affect the indexes and marks of
     * this buffer.
     * This method is identical to
     * {@code buf.nioBuffer(buf.readerIndex(), buf.readableBytes())}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * Please note that the returned NIO buffer will not see the changes of this
     * buffer if this buffer
     * is a dynamic buffer and it adjusted its capacity.
     *
     * @throws UnsupportedOperationException
     *                                       if this buffer cannot create a
     *                                       {@link ByteBuffer} that shares the
     *                                       content with itself
     *
     * @see #nioBufferCount()
     * @see #nioBuffers()
     * @see #nioBuffers(int, int)
     */
    public abstract ByteBuffer nioBuffer();

    /**
     * 将此缓冲区的子区域暴露为NIO {@link ByteBuffer}。
     * 返回的缓冲区要么共享，要么包含此缓冲区内容的副本。
     * 更改返回的NIO缓冲区的位置和限制不会影响此缓冲区的索引和标记。
     * 此方法不会修改此缓冲区的{@code readerIndex}或{@code writerIndex}。
     * 请注意，如果此缓冲区是动态缓冲区并且调整了其容量，
     * 则返回的NIO缓冲区将不会看到此缓冲区的更改。
     *
     * @throws UnsupportedOperationException
     *                                       如果此缓冲区无法创建一个与自身共享内容的
     *                                       {@link ByteBuffer}
     *
     * @see #nioBufferCount()
     * @see #nioBuffers()
     * @see #nioBuffers(int, int)
     */
    public abstract ByteBuffer nioBuffer(int index, int length);

    /**
     * Internal use only: Exposes the internal NIO buffer.
     */
    public abstract ByteBuffer internalNioBuffer(int index, int length);

    /**
     * Exposes this buffer's readable bytes as an NIO {@link ByteBuffer}'s. The
     * returned buffer
     * either share or contains the copied content of this buffer, while changing
     * the position
     * and limit of the returned NIO buffer does not affect the indexes and marks of
     * this buffer.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * Please note that the returned NIO buffer will not see the changes of this
     * buffer if this buffer
     * is a dynamic buffer and it adjusted its capacity.
     *
     *
     * @throws UnsupportedOperationException
     *                                       if this buffer cannot create a
     *                                       {@link ByteBuffer} that shares the
     *                                       content with itself
     *
     * @see #nioBufferCount()
     * @see #nioBuffer()
     * @see #nioBuffer(int, int)
     */
    public abstract ByteBuffer[] nioBuffers();

    /**
     * Exposes this buffer's bytes as an NIO {@link ByteBuffer}'s for the specified
     * index and length
     * The returned buffer either share or contains the copied content of this
     * buffer, while changing
     * the position and limit of the returned NIO buffer does not affect the indexes
     * and marks of this buffer.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer. Please note that the
     * returned NIO buffer will not see the changes of this buffer if this buffer is
     * a dynamic
     * buffer and it adjusted its capacity.
     *
     * @throws UnsupportedOperationException
     *                                       if this buffer cannot create a
     *                                       {@link ByteBuffer} that shares the
     *                                       content with itself
     *
     * @see #nioBufferCount()
     * @see #nioBuffer()
     * @see #nioBuffer(int, int)
     */
    public abstract ByteBuffer[] nioBuffers(int index, int length);

    /**
     * Returns {@code true} if and only if this buffer has a backing byte array.
     * If this method returns true, you can safely call {@link #array()} and
     * {@link #arrayOffset()}.
     */
    public abstract boolean hasArray();

    /**
     * Returns the backing byte array of this buffer.
     *
     * @throws UnsupportedOperationException
     *                                       if there no accessible backing byte
     *                                       array
     */
    public abstract byte[] array();

    /**
     * Returns the offset of the first byte within the backing byte array of
     * this buffer.
     *
     * @throws UnsupportedOperationException
     *                                       if there no accessible backing byte
     *                                       array
     */
    public abstract int arrayOffset();

    /**
     * Returns {@code true} if and only if this buffer has a reference to the
     * low-level memory address that points
     * to the backing data.
     */
    public abstract boolean hasMemoryAddress();

    /**
     * Returns the low-level memory address that point to the first byte of ths
     * backing data.
     *
     * @throws UnsupportedOperationException
     *                                       if this buffer does not support
     *                                       accessing the low-level memory address
     */
    public abstract long memoryAddress();

    /**
     * Returns {@code true} if this {@link ByteBuf} implementation is backed by a
     * single memory region.
     * Composite buffer implementations must return false even if they currently
     * hold &le; 1 components.
     * For buffers that return {@code true}, it's guaranteed that a successful call
     * to {@link #discardReadBytes()}
     * will increase the value of {@link #maxFastWritableBytes()} by the current
     * {@code readerIndex}.
     * <p>
     * This method will return {@code false} by default, and a {@code false} return
     * value does not necessarily
     * mean that the implementation is composite or that it is <i>not</i> backed by
     * a single memory region.
     */
    public boolean isContiguous() {
        return false;
    }

    /**
     * A {@code ByteBuf} can turn into itself.
     * 
     * @return This {@code ByteBuf} instance.
     */
    @Override
    public ByteBuf asByteBuf() {
        return this;
    }

    /**
     * Decodes this buffer's readable bytes into a string with the specified
     * character set name. This method is identical to
     * {@code buf.toString(buf.readerIndex(), buf.readableBytes(), charsetName)}.
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     *
     * @throws UnsupportedCharsetException
     *                                     if the specified character set name is
     *                                     not supported by the
     *                                     current VM
     */
    public abstract String toString(Charset charset);

    /**
     * Decodes this buffer's sub-region into a string with the specified
     * character set. This method does not modify {@code readerIndex} or
     * {@code writerIndex} of this buffer.
     */
    public abstract String toString(int index, int length, Charset charset);

    /**
     * Returns a hash code which was calculated from the content of this
     * buffer. If there's a byte array which is
     * {@linkplain #equals(Object) equal to} this array, both arrays should
     * return the same value.
     */
    @Override
    public abstract int hashCode();

    /**
     * Determines if the content of the specified buffer is identical to the
     * content of this array. 'Identical' here means:
     * <ul>
     * <li>the size of the contents of the two buffers are same and</li>
     * <li>every single byte of the content of the two buffers are same.</li>
     * </ul>
     * Please note that it does not compare {@link #readerIndex()} nor
     * {@link #writerIndex()}. This method also returns {@code false} for
     * {@code null} and an object which is not an instance of
     * {@link ByteBuf} type.
     */
    @Override
    public abstract boolean equals(Object obj);

    /**
     * Compares the content of the specified buffer to the content of this
     * buffer. Comparison is performed in the same manner with the string
     * comparison functions of various languages such as {@code strcmp},
     * {@code memcmp} and {@link String#compareTo(String)}.
     */
    @Override
    public abstract int compareTo(ByteBuf buffer);

    /**
     * Returns the string representation of this buffer. This method does not
     * necessarily return the whole content of the buffer but returns
     * the values of the key properties such as {@link #readerIndex()},
     * {@link #writerIndex()} and {@link #capacity()}.
     */
    @Override
    public abstract String toString();

    @Override
    public abstract ByteBuf retain(int increment);

    @Override
    public abstract ByteBuf retain();

    @Override
    public abstract ByteBuf touch();

    @Override
    public abstract ByteBuf touch(Object hint);

    /**
     * Used internally by {@link AbstractByteBuf#ensureAccessible()} to try to guard
     * against using the buffer after it was released (best-effort).
     */
    boolean isAccessible() {
        return refCnt() != 0;
    }
}
