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

import static io.netty.util.internal.MathUtil.isOutOfBounds;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

import io.netty.util.AsciiString;
import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * A skeletal implementation of a buffer.
 */
public abstract class AbstractByteBuf extends ByteBuf {
    // 获取当前类的日志记录器实例
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractByteBuf.class);

    // 旧版本的可访问性检查属性名（用于向后兼容）
    private static final String LEGACY_PROP_CHECK_ACCESSIBLE = "io.netty.buffer.bytebuf.checkAccessible";

    // 新版本的可访问性检查属性名
    private static final String PROP_CHECK_ACCESSIBLE = "io.netty.buffer.checkAccessible";

    // 是否进行可访问性检查的标志，从CompositeByteBuf中访问
    static final boolean checkAccessible;

    // 边界检查属性名
    private static final String PROP_CHECK_BOUNDS = "io.netty.buffer.checkBounds";

    // 是否进行边界检查的标志
    private static final boolean checkBounds;

    // 静态初始化块，用于初始化检查标志
    // question: 为什么用户需要手动设置checkAccessible?
    /*
     * answser:
     * 总结设置checkAccessible的原因：
     * 性能优化：关闭检查可以提升性能
     * 环境适配：不同环境可能需要不同的配置
     * 特殊需求：某些场景下用户完全控制ByteBuf使用
     * 调试便利：开发测试时可能需要关闭检查
     * 生产调优：根据实际负载调整配置
     */
    /*
     * question: checkAccessible的作用是什么?
     * answser:
     * --------------------------------
     * 安全性保证---
     * 防止访问已释放的缓冲区
     * 避免内存泄漏
     * 确保多线程安全
     * 资源管理---
     * 帮助正确管理ByteBuf的生命周期
     * 确保资源及时释放
     * 防止资源泄露
     * 调试支持---
     * 帮助发现潜在问题
     * 提供清晰的错误信息
     * 便于问题定位
     * 性能考虑---
     * 可以通过配置关闭检查
     * 在高性能场景下优化
     * 平衡安全性和性能
     * 最佳实践---
     * 开发环境建议开启
     * 生产环境根据需求配置
     * 确保代码质量
     */
    static {
        // 优先检查新版本的属性名，如果存在则使用新版本
        if (SystemPropertyUtil.contains(PROP_CHECK_ACCESSIBLE)) {
            checkAccessible = SystemPropertyUtil.getBoolean(PROP_CHECK_ACCESSIBLE, true);
        } else {
            // 如果新版本不存在，则使用旧版本的属性名
            checkAccessible = SystemPropertyUtil.getBoolean(LEGACY_PROP_CHECK_ACCESSIBLE, true);
        }

        // 初始化边界检查标志
        checkBounds = SystemPropertyUtil.getBoolean(PROP_CHECK_BOUNDS, true);

        // 如果日志级别为DEBUG，则输出配置信息
        if (logger.isDebugEnabled()) {
            logger.debug("-D{}: {}", PROP_CHECK_ACCESSIBLE, checkAccessible);
            logger.debug("-D{}: {}", PROP_CHECK_BOUNDS, checkBounds);
        }
    }

    // 创建ByteBuf的资源泄漏检测器实例
    static final ResourceLeakDetector<ByteBuf> leakDetector = ResourceLeakDetectorFactory.instance()
            .newResourceLeakDetector(ByteBuf.class);

    // 读索引位置
    int readerIndex;

    // 写索引位置
    int writerIndex;

    // 标记的读索引位置（用于临时保存读位置）
    // tags: 优化
    private int markedReaderIndex;

    // 标记的写索引位置（用于临时保存写位置）
    // tags: 优化
    private int markedWriterIndex;

    // 最大容量限制
    private int maxCapacity;

    protected AbstractByteBuf(int maxCapacity) {
        checkPositiveOrZero(maxCapacity, "maxCapacity");
        this.maxCapacity = maxCapacity;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @SuppressWarnings("deprecation")
    @Override
    public ByteBuf asReadOnly() {
        if (isReadOnly()) {
            return this;
        }
        return Unpooled.unmodifiableBuffer(this);
    }

    @Override
    public int maxCapacity() {
        return maxCapacity;
    }

    protected final void maxCapacity(int maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    @Override
    public int readerIndex() {
        return readerIndex;
    }

    private static void checkIndexBounds(final int readerIndex, final int writerIndex, final int capacity) {
        if (readerIndex < 0 || readerIndex > writerIndex || writerIndex > capacity) {
            throw new IndexOutOfBoundsException(String.format(
                    "readerIndex: %d, writerIndex: %d (expected: 0 <= readerIndex <= writerIndex <= capacity(%d))",
                    readerIndex, writerIndex, capacity));
        }
    }

    @Override
    public ByteBuf readerIndex(int readerIndex) {
        if (checkBounds) {
            checkIndexBounds(readerIndex, writerIndex, capacity());
        }
        this.readerIndex = readerIndex;
        return this;
    }

    @Override
    public int writerIndex() {
        return writerIndex;
    }

    @Override
    public ByteBuf writerIndex(int writerIndex) {
        if (checkBounds) {
            checkIndexBounds(readerIndex, writerIndex, capacity());
        }
        this.writerIndex = writerIndex;
        return this;
    }

    @Override
    public ByteBuf setIndex(int readerIndex, int writerIndex) {
        if (checkBounds) {
            checkIndexBounds(readerIndex, writerIndex, capacity());
        }
        setIndex0(readerIndex, writerIndex);
        return this;
    }

    @Override
    public ByteBuf clear() {
        readerIndex = writerIndex = 0;
        return this;
    }

    @Override
    public boolean isReadable() {
        return writerIndex > readerIndex;
    }

    @Override
    public boolean isReadable(int numBytes) {
        return writerIndex - readerIndex >= numBytes;
    }

    @Override
    public boolean isWritable() {
        return capacity() > writerIndex;
    }

    @Override
    public boolean isWritable(int numBytes) {
        return capacity() - writerIndex >= numBytes;
    }

    @Override
    public int readableBytes() {
        return writerIndex - readerIndex;
    }

    @Override
    public int writableBytes() {
        return capacity() - writerIndex;
    }

    @Override
    public int maxWritableBytes() {
        return maxCapacity() - writerIndex;
    }

    @Override
    public ByteBuf markReaderIndex() {
        markedReaderIndex = readerIndex;
        return this;
    }

    @Override
    public ByteBuf resetReaderIndex() {
        readerIndex(markedReaderIndex);
        return this;
    }

    @Override
    public ByteBuf markWriterIndex() {
        markedWriterIndex = writerIndex;
        return this;
    }

    @Override
    public ByteBuf resetWriterIndex() {
        writerIndex(markedWriterIndex);
        return this;
    }

    @Override
    public ByteBuf discardReadBytes() {
        // 如果读索引为0，说明没有可丢弃的数据
        if (readerIndex == 0) {
            // 仅检查缓冲区是否可访问（引用计数>0）
            ensureAccessible();
            // 直接返回当前缓冲区，不做任何操作
            return this;
        }

        // 如果读索引不等于写索引，说明缓冲区中还有未读数据
        if (readerIndex != writerIndex) {
            // 将未读数据（从readerIndex到writerIndex）移动到缓冲区起始位置（索引0）
            // 实际上是执行内存搬移操作，将未读数据前移
            setBytes(0, this, readerIndex, writerIndex - readerIndex);

            // 更新写索引，减去已丢弃的字节数量
            // 新的写索引 = 原写索引 - 读索引（丢弃的字节数）
            writerIndex -= readerIndex;

            // 调整标记位置，因为内存位置已经改变
            // 用于更新markReaderIndex和markWriterIndex
            adjustMarkers(readerIndex);

            // 将读索引重置为0，表示从缓冲区开始位置开始读取
            readerIndex = 0;
        } else {
            // 如果读索引等于写索引，说明所有数据都已被读取
            // 仅检查缓冲区是否可访问
            ensureAccessible();

            // 调整标记位置
            adjustMarkers(readerIndex);

            // 将读写索引都重置为0，相当于清空缓冲区
            writerIndex = readerIndex = 0;
        }

        // 返回当前缓冲区，支持方法链式调用
        return this;
    }

    @Override
    public ByteBuf discardSomeReadBytes() {
        // 如果读索引大于0，说明有已读数据可以被丢弃
        if (readerIndex > 0) {
            // 如果读索引等于写索引，表示所有数据都已被读取
            if (readerIndex == writerIndex) {
                // 确保缓冲区可访问（引用计数>0）
                ensureAccessible();
                // 调整标记位置，以适应索引的变化
                adjustMarkers(readerIndex);
                // 将读写索引都重置为0，相当于清空缓冲区
                writerIndex = readerIndex = 0;
                return this;
            }

            // 只有当读索引超过缓冲区容量的一半时，才执行内存压缩操作
            // 这是一个优化策略，避免频繁的内存复制
            if (readerIndex >= capacity() >>> 1) { // ">>> 1"相当于除以2
                // 将未读数据（从readerIndex到writerIndex）移动到缓冲区起始位置（索引0）
                setBytes(0, this, readerIndex, writerIndex - readerIndex);
                // 更新写索引，减去已丢弃的字节数量
                writerIndex -= readerIndex;
                // 调整标记位置，以适应内存布局的变化
                adjustMarkers(readerIndex);
                // 将读索引重置为0，表示从缓冲区开始位置读取
                readerIndex = 0;
                return this;
            }
        }

        // 如果没有可丢弃的数据（readerIndex=0）或者读索引未超过容量一半，
        // 则仅检查缓冲区可访问性，不做任何内存操作
        ensureAccessible();
        return this;
    }

    protected final void adjustMarkers(int decrement) {
        if (markedReaderIndex <= decrement) {
            markedReaderIndex = 0;
            if (markedWriterIndex <= decrement) {
                markedWriterIndex = 0;
            } else {
                markedWriterIndex -= decrement;
            }
        } else {
            markedReaderIndex -= decrement;
            markedWriterIndex -= decrement;
        }
    }

    // Called after a capacity reduction
    protected final void trimIndicesToCapacity(int newCapacity) {
        if (writerIndex() > newCapacity) {
            setIndex0(Math.min(readerIndex(), newCapacity), newCapacity);
        }
    }

    @Override
    public ByteBuf ensureWritable(int minWritableBytes) {
        ensureWritable0(checkPositiveOrZero(minWritableBytes, "minWritableBytes"));
        return this;
    }

    /**
     * 确保ByteBuf有足够的可写空间，必要时进行容量扩展
     * 这是一个内部方法，被public的ensureWritable方法调用
     */
    final void ensureWritable0(int minWritableBytes) {
        // 获取当前写索引位置
        final int writerIndex = writerIndex();

        // 计算需要的目标容量 = 当前写索引 + 最小可写字节数
        final int targetCapacity = writerIndex + minWritableBytes;

        // 检查目标容量是否合法（非负）且在当前容量范围内
        // 注意：这里使用非短路与(&)而不是短路与(&&)来减少分支预测失败
        // 这是性能优化的一种手段，因为这是热点路径，且targetCapacity很少溢出
        if (targetCapacity >= 0 & targetCapacity <= capacity()) {
            // 如果目标容量合法且在当前容量范围内，只需确保缓冲区可访问（引用计数>0）
            ensureAccessible();
            return;
        }

        // 如果启用了边界检查，且目标容量非法（负数或超过最大容量）
        if (checkBounds && (targetCapacity < 0 || targetCapacity > maxCapacity)) {
            // 确保缓冲区可访问
            ensureAccessible();
            // 抛出索引越界异常，提供详细错误信息
            throw new IndexOutOfBoundsException(String.format(
                    "writerIndex(%d) + minWritableBytes(%d) exceeds maxCapacity(%d): %s",
                    writerIndex, minWritableBytes, maxCapacity, this));
        }

        // 计算新的容量
        // 首先获取快速可写字节数（不需要内存复制的可写空间）
        //
        final int fastWritable = maxFastWritableBytes();

        // 决定新容量
        // 1. 如果快速可写空间足够，直接使用当前写索引+快速可写空间
        // 2. 否则，通过分配器计算合适的新容量，考虑目标容量和最大容量
        int newCapacity = fastWritable >= minWritableBytes ? writerIndex + fastWritable
                : alloc().calculateNewCapacity(targetCapacity, maxCapacity);

        // 调整缓冲区到新容量
        // 这可能涉及内存重新分配和数据复制
        capacity(newCapacity);
    }

    @Override
    public int ensureWritable(int minWritableBytes, boolean force) {
        // 确保缓冲区可访问（引用计数>0）
        ensureAccessible();

        // 检查minWritableBytes参数是否为非负数，否则抛出异常
        checkPositiveOrZero(minWritableBytes, "minWritableBytes");

        // 如果当前可写字节数已经足够，直接返回0（表示无需扩容）
        if (minWritableBytes <= writableBytes()) {
            return 0;
        }

        // 获取最大容量限制
        final int maxCapacity = maxCapacity();

        // 获取当前写索引位置
        final int writerIndex = writerIndex();

        // 如果需要的可写空间超过了从当前写索引到最大容量的剩余空间
        if (minWritableBytes > maxCapacity - writerIndex) {
            // 如果不强制扩容或者当前容量已经达到最大容量
            if (!force || capacity() == maxCapacity) {
                // 返回1，表示无法满足扩容请求
                return 1;
            }

            // 强制扩容到最大容量
            capacity(maxCapacity);

            // 返回3，表示已扩容到最大容量，但仍无法满足完整需求
            return 3;
        }

        // 获取快速可写字节数（不需要内存复制的可写空间）
        int fastWritable = maxFastWritableBytes();

        // 计算新容量：
        // 1. 如果快速可写空间足够，直接使用当前写索引+快速可写空间
        // 2. 否则，通过分配器计算合适的新容量，考虑目标容量和最大容量
        int newCapacity = fastWritable >= minWritableBytes ? writerIndex + fastWritable
                : alloc().calculateNewCapacity(writerIndex + minWritableBytes, maxCapacity);

        // 调整缓冲区到新容量
        // 这可能涉及内存重新分配和数据复制
        capacity(newCapacity);

        // 返回2，表示扩容成功，且满足需求
        return 2;
    }

    @Override
    public ByteBuf order(ByteOrder endianness) {
        if (endianness == order()) {
            return this;
        }
        ObjectUtil.checkNotNull(endianness, "endianness");
        return newSwappedByteBuf();
    }

    /**
     * Creates a new {@link SwappedByteBuf} for this {@link ByteBuf} instance.
     */
    protected SwappedByteBuf newSwappedByteBuf() {
        return new SwappedByteBuf(this);
    }

    @Override
    public byte getByte(int index) {
        checkIndex(index);
        return _getByte(index);
    }

    protected abstract byte _getByte(int index);

    @Override
    public boolean getBoolean(int index) {
        return getByte(index) != 0;
    }

    @Override
    public short getUnsignedByte(int index) {
        return (short) (getByte(index) & 0xFF);
    }

    @Override
    public short getShort(int index) {
        checkIndex(index, 2);
        return _getShort(index);
    }

    protected abstract short _getShort(int index);

    @Override
    public short getShortLE(int index) {
        checkIndex(index, 2);
        return _getShortLE(index);
    }

    protected abstract short _getShortLE(int index);

    @Override
    public int getUnsignedShort(int index) {
        return getShort(index) & 0xFFFF;
    }

    @Override
    public int getUnsignedShortLE(int index) {
        return getShortLE(index) & 0xFFFF;
    }

    @Override
    public int getUnsignedMedium(int index) {
        checkIndex(index, 3);
        return _getUnsignedMedium(index);
    }

    protected abstract int _getUnsignedMedium(int index);

    @Override
    public int getUnsignedMediumLE(int index) {
        checkIndex(index, 3);
        return _getUnsignedMediumLE(index);
    }

    protected abstract int _getUnsignedMediumLE(int index);

    @Override
    public int getMedium(int index) {
        int value = getUnsignedMedium(index);
        if ((value & 0x800000) != 0) {
            value |= 0xff000000;
        }
        return value;
    }

    @Override
    public int getMediumLE(int index) {
        int value = getUnsignedMediumLE(index);
        if ((value & 0x800000) != 0) {
            value |= 0xff000000;
        }
        return value;
    }

    @Override
    public int getInt(int index) {
        checkIndex(index, 4);
        return _getInt(index);
    }

    protected abstract int _getInt(int index);

    @Override
    public int getIntLE(int index) {
        checkIndex(index, 4);
        return _getIntLE(index);
    }

    protected abstract int _getIntLE(int index);

    @Override
    public long getUnsignedInt(int index) {
        return getInt(index) & 0xFFFFFFFFL;
    }

    @Override
    public long getUnsignedIntLE(int index) {
        return getIntLE(index) & 0xFFFFFFFFL;
    }

    @Override
    public long getLong(int index) {
        checkIndex(index, 8);
        return _getLong(index);
    }

    protected abstract long _getLong(int index);

    @Override
    public long getLongLE(int index) {
        checkIndex(index, 8);
        return _getLongLE(index);
    }

    protected abstract long _getLongLE(int index);

    @Override
    public char getChar(int index) {
        return (char) getShort(index);
    }

    @Override
    public float getFloat(int index) {
        return Float.intBitsToFloat(getInt(index));
    }

    @Override
    public double getDouble(int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst) {
        getBytes(index, dst, 0, dst.length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst) {
        getBytes(index, dst, dst.writableBytes());
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int length) {
        getBytes(index, dst, dst.writerIndex(), length);
        dst.writerIndex(dst.writerIndex() + length);
        return this;
    }

    @Override
    public CharSequence getCharSequence(int index, int length, Charset charset) {
        if (CharsetUtil.US_ASCII.equals(charset) || CharsetUtil.ISO_8859_1.equals(charset)) {
            // ByteBufUtil.getBytes(...) will return a new copy which the AsciiString uses
            // directly
            return new AsciiString(ByteBufUtil.getBytes(this, index, length, true), false);
        }
        return toString(index, length, charset);
    }

    @Override
    public CharSequence readCharSequence(int length, Charset charset) {
        CharSequence sequence = getCharSequence(readerIndex, length, charset);
        readerIndex += length;
        return sequence;
    }

    @Override
    public ByteBuf setByte(int index, int value) {
        checkIndex(index);
        _setByte(index, value);
        return this;
    }

    protected abstract void _setByte(int index, int value);

    @Override
    public ByteBuf setBoolean(int index, boolean value) {
        setByte(index, value ? 1 : 0);
        return this;
    }

    @Override
    public ByteBuf setShort(int index, int value) {
        checkIndex(index, 2);
        _setShort(index, value);
        return this;
    }

    protected abstract void _setShort(int index, int value);

    @Override
    public ByteBuf setShortLE(int index, int value) {
        checkIndex(index, 2);
        _setShortLE(index, value);
        return this;
    }

    protected abstract void _setShortLE(int index, int value);

    @Override
    public ByteBuf setChar(int index, int value) {
        setShort(index, value);
        return this;
    }

    @Override
    public ByteBuf setMedium(int index, int value) {
        checkIndex(index, 3);
        _setMedium(index, value);
        return this;
    }

    protected abstract void _setMedium(int index, int value);

    @Override
    public ByteBuf setMediumLE(int index, int value) {
        checkIndex(index, 3);
        _setMediumLE(index, value);
        return this;
    }

    protected abstract void _setMediumLE(int index, int value);

    @Override
    public ByteBuf setInt(int index, int value) {
        checkIndex(index, 4);
        _setInt(index, value);
        return this;
    }

    protected abstract void _setInt(int index, int value);

    @Override
    public ByteBuf setIntLE(int index, int value) {
        checkIndex(index, 4);
        _setIntLE(index, value);
        return this;
    }

    protected abstract void _setIntLE(int index, int value);

    @Override
    public ByteBuf setFloat(int index, float value) {
        setInt(index, Float.floatToRawIntBits(value));
        return this;
    }

    @Override
    public ByteBuf setLong(int index, long value) {
        checkIndex(index, 8);
        _setLong(index, value);
        return this;
    }

    protected abstract void _setLong(int index, long value);

    @Override
    public ByteBuf setLongLE(int index, long value) {
        checkIndex(index, 8);
        _setLongLE(index, value);
        return this;
    }

    protected abstract void _setLongLE(int index, long value);

    @Override
    public ByteBuf setDouble(int index, double value) {
        setLong(index, Double.doubleToRawLongBits(value));
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src) {
        setBytes(index, src, 0, src.length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src) {
        setBytes(index, src, src.readableBytes());
        return this;
    }

    private static void checkReadableBounds(final ByteBuf src, final int length) {
        if (length > src.readableBytes()) {
            throw new IndexOutOfBoundsException(String.format(
                    "length(%d) exceeds src.readableBytes(%d) where src is: %s", length, src.readableBytes(), src));
        }
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int length) {
        // 检查目标位置index和长度length是否在有效范围内
        // 如果超出范围会抛出IndexOutOfBoundsException
        checkIndex(index, length);

        // 检查源ByteBuf是否为null
        // 如果为null会抛出NullPointerException
        ObjectUtil.checkNotNull(src, "src");

        // 如果启用了边界检查
        if (checkBounds) {
            // 检查源ByteBuf是否有足够的可读字节
            // 如果可读字节数小于length会抛出IndexOutOfBoundsException
            checkReadableBounds(src, length);
        }

        // 执行实际的字节复制操作
        // 从源ByteBuf的readerIndex位置开始，复制length个字节
        // 到目标ByteBuf的index位置
        setBytes(index, src, src.readerIndex(), length);

        // 更新源ByteBuf的读索引，向后移动length个位置
        // 这表示这些字节已经被读取和复制
        src.readerIndex(src.readerIndex() + length);

        // 返回当前ByteBuf实例，支持链式调用
        return this;
    }

    @Override
    public ByteBuf setZero(int index, int length) {
        if (length == 0) {
            return this;
        }

        checkIndex(index, length);

        int nLong = length >>> 3;
        int nBytes = length & 7;
        for (int i = nLong; i > 0; i--) {
            _setLong(index, 0);
            index += 8;
        }
        if (nBytes == 4) {
            _setInt(index, 0);
            // Not need to update the index as we not will use it after this.
        } else if (nBytes < 4) {
            for (int i = nBytes; i > 0; i--) {
                _setByte(index, 0);
                index++;
            }
        } else {
            _setInt(index, 0);
            index += 4;
            for (int i = nBytes - 4; i > 0; i--) {
                _setByte(index, 0);
                index++;
            }
        }
        return this;
    }

    @Override
    public int setCharSequence(int index, CharSequence sequence, Charset charset) {
        return setCharSequence0(index, sequence, charset, false);
    }

    /**
     * 将字符序列写入ByteBuf的指定位置
     * 
     * @param index    写入的起始位置
     * @param sequence 要写入的字符序列
     * @param charset  字符编码
     * @param expand   是否允许自动扩容
     * @return 写入的字节数
     * 
     * @throws IndexOutOfBoundsException 如果index小于0或写入位置超出缓冲区容量
     * @throws IllegalArgumentException  如果字符序列为null
     */
    private int setCharSequence0(int index, CharSequence sequence, Charset charset, boolean expand) {
        // 针对UTF-8编码的特殊优化处理
        if (charset.equals(CharsetUtil.UTF_8)) {
            // 计算UTF-8编码下可能的最大字节数
            int length = ByteBufUtil.utf8MaxBytes(sequence);

            // 根据是否需要扩容进行不同的索引检查
            if (expand) {
                // 确保有足够的可写空间
                ensureWritable0(length);
                // 检查索引是否在有效范围内
                checkIndex0(index, length);
            } else {
                // 不扩容时检查索引
                checkIndex(index, length);
            }
            // 使用优化的UTF-8写入方法
            return ByteBufUtil.writeUtf8(this, index, length, sequence, sequence.length());
        }

        // 针对ASCII和ISO-8859-1编码的特殊优化处理
        if (charset.equals(CharsetUtil.US_ASCII) || charset.equals(CharsetUtil.ISO_8859_1)) {
            // 对于单字节编码，长度等于字符序列长度
            int length = sequence.length();

            // 根据是否需要扩容进行不同的索引检查
            if (expand) {
                // 确保有足够的可写空间
                ensureWritable0(length);
                // 检查索引是否在有效范围内
                checkIndex0(index, length);
            } else {
                // 不扩容时检查索引
                checkIndex(index, length);
            }
            // 使用优化的ASCII写入方法
            return ByteBufUtil.writeAscii(this, index, sequence, length);
        }

        // 对于其他编码，使用通用的转换方法
        // 将字符序列转换为字节数组
        byte[] bytes = sequence.toString().getBytes(charset);

        // 如果需要扩容，确保有足够的空间
        if (expand) {
            ensureWritable0(bytes.length);
            // setBytes方法会处理索引检查
        }

        // 写入字节数组
        setBytes(index, bytes);
        // 返回写入的字节数
        return bytes.length;
    }

    @Override
    public byte readByte() {
        checkReadableBytes0(1);
        int i = readerIndex;
        byte b = _getByte(i);
        readerIndex = i + 1;
        return b;
    }

    @Override
    public boolean readBoolean() {
        return readByte() != 0;
    }

    @Override
    public short readUnsignedByte() {
        return (short) (readByte() & 0xFF);
    }

    @Override
    public short readShort() {
        checkReadableBytes0(2);
        short v = _getShort(readerIndex);
        readerIndex += 2;
        return v;
    }

    @Override
    public short readShortLE() {
        checkReadableBytes0(2);
        short v = _getShortLE(readerIndex);
        readerIndex += 2;
        return v;
    }

    @Override
    public int readUnsignedShort() {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readUnsignedShortLE() {
        return readShortLE() & 0xFFFF;
    }

    @Override
    public int readMedium() {
        int value = readUnsignedMedium();
        if ((value & 0x800000) != 0) {
            value |= 0xff000000;
        }
        return value;
    }

    @Override
    public int readMediumLE() {
        int value = readUnsignedMediumLE();
        if ((value & 0x800000) != 0) {
            value |= 0xff000000;
        }
        return value;
    }

    @Override
    public int readUnsignedMedium() {
        checkReadableBytes0(3);
        int v = _getUnsignedMedium(readerIndex);
        readerIndex += 3;
        return v;
    }

    @Override
    public int readUnsignedMediumLE() {
        checkReadableBytes0(3);
        int v = _getUnsignedMediumLE(readerIndex);
        readerIndex += 3;
        return v;
    }

    @Override
    public int readInt() {
        checkReadableBytes0(4);
        int v = _getInt(readerIndex);
        readerIndex += 4;
        return v;
    }

    @Override
    public int readIntLE() {
        checkReadableBytes0(4);
        int v = _getIntLE(readerIndex);
        readerIndex += 4;
        return v;
    }

    @Override
    public long readUnsignedInt() {
        return readInt() & 0xFFFFFFFFL;
    }

    @Override
    public long readUnsignedIntLE() {
        return readIntLE() & 0xFFFFFFFFL;
    }

    @Override
    public long readLong() {
        checkReadableBytes0(8);
        long v = _getLong(readerIndex);
        readerIndex += 8;
        return v;
    }

    @Override
    public long readLongLE() {
        checkReadableBytes0(8);
        long v = _getLongLE(readerIndex);
        readerIndex += 8;
        return v;
    }

    @Override
    public char readChar() {
        return (char) readShort();
    }

    @Override
    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public ByteBuf readBytes(int length) {
        checkReadableBytes(length);
        if (length == 0) {
            return Unpooled.EMPTY_BUFFER;
        }

        ByteBuf buf = alloc().buffer(length, maxCapacity);
        buf.writeBytes(this, readerIndex, length);
        readerIndex += length;
        return buf;
    }

    @Override
    public ByteBuf readSlice(int length) {
        checkReadableBytes(length);
        ByteBuf slice = slice(readerIndex, length);
        readerIndex += length;
        return slice;
    }

    @Override
    public ByteBuf readRetainedSlice(int length) {
        checkReadableBytes(length);
        ByteBuf slice = retainedSlice(readerIndex, length);
        readerIndex += length;
        return slice;
    }

    @Override
    public ByteBuf readBytes(byte[] dst, int dstIndex, int length) {
        checkReadableBytes(length);
        getBytes(readerIndex, dst, dstIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public ByteBuf readBytes(byte[] dst) {
        readBytes(dst, 0, dst.length);
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst) {
        readBytes(dst, dst.writableBytes());
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst, int length) {
        if (checkBounds) {
            if (length > dst.writableBytes()) {
                throw new IndexOutOfBoundsException(String.format(
                        "length(%d) exceeds dst.writableBytes(%d) where dst is: %s", length, dst.writableBytes(), dst));
            }
        }
        readBytes(dst, dst.writerIndex(), length);
        dst.writerIndex(dst.writerIndex() + length);
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
        checkReadableBytes(length);
        getBytes(readerIndex, dst, dstIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuffer dst) {
        int length = dst.remaining();
        checkReadableBytes(length);
        getBytes(readerIndex, dst);
        readerIndex += length;
        return this;
    }

    @Override
    public int readBytes(GatheringByteChannel out, int length)
            throws IOException {
        checkReadableBytes(length);
        int readBytes = getBytes(readerIndex, out, length);
        readerIndex += readBytes;
        return readBytes;
    }

    @Override
    public int readBytes(FileChannel out, long position, int length)
            throws IOException {
        checkReadableBytes(length);
        int readBytes = getBytes(readerIndex, out, position, length);
        readerIndex += readBytes;
        return readBytes;
    }

    @Override
    public ByteBuf readBytes(OutputStream out, int length) throws IOException {
        checkReadableBytes(length);
        getBytes(readerIndex, out, length);
        readerIndex += length;
        return this;
    }

    @Override
    public ByteBuf skipBytes(int length) {
        checkReadableBytes(length);
        readerIndex += length;
        return this;
    }

    @Override
    public ByteBuf writeBoolean(boolean value) {
        writeByte(value ? 1 : 0);
        return this;
    }

    @Override
    public ByteBuf writeByte(int value) {
        ensureWritable0(1);
        _setByte(writerIndex++, value);
        return this;
    }

    @Override
    public ByteBuf writeShort(int value) {
        ensureWritable0(2);
        _setShort(writerIndex, value);
        writerIndex += 2;
        return this;
    }

    @Override
    public ByteBuf writeShortLE(int value) {
        ensureWritable0(2);
        _setShortLE(writerIndex, value);
        writerIndex += 2;
        return this;
    }

    @Override
    public ByteBuf writeMedium(int value) {
        ensureWritable0(3);
        _setMedium(writerIndex, value);
        writerIndex += 3;
        return this;
    }

    @Override
    public ByteBuf writeMediumLE(int value) {
        ensureWritable0(3);
        _setMediumLE(writerIndex, value);
        writerIndex += 3;
        return this;
    }

    @Override
    public ByteBuf writeInt(int value) {
        ensureWritable0(4);
        _setInt(writerIndex, value);
        writerIndex += 4;
        return this;
    }

    @Override
    public ByteBuf writeIntLE(int value) {
        ensureWritable0(4);
        _setIntLE(writerIndex, value);
        writerIndex += 4;
        return this;
    }

    @Override
    public ByteBuf writeLong(long value) {
        ensureWritable0(8);
        _setLong(writerIndex, value);
        writerIndex += 8;
        return this;
    }

    @Override
    public ByteBuf writeLongLE(long value) {
        ensureWritable0(8);
        _setLongLE(writerIndex, value);
        writerIndex += 8;
        return this;
    }

    @Override
    public ByteBuf writeChar(int value) {
        writeShort(value);
        return this;
    }

    @Override
    public ByteBuf writeFloat(float value) {
        writeInt(Float.floatToRawIntBits(value));
        return this;
    }

    @Override
    public ByteBuf writeDouble(double value) {
        writeLong(Double.doubleToRawLongBits(value));
        return this;
    }

    @Override
    public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
        ensureWritable(length);
        setBytes(writerIndex, src, srcIndex, length);
        writerIndex += length;
        return this;
    }

    @Override
    public ByteBuf writeBytes(byte[] src) {
        writeBytes(src, 0, src.length);
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuf src) {
        writeBytes(src, src.readableBytes());
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuf src, int length) {
        if (checkBounds) {
            checkReadableBounds(src, length);
        }
        writeBytes(src, src.readerIndex(), length);
        src.readerIndex(src.readerIndex() + length);
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
        ensureWritable(length);
        setBytes(writerIndex, src, srcIndex, length);
        writerIndex += length;
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuffer src) {
        int length = src.remaining();
        ensureWritable0(length);
        setBytes(writerIndex, src);
        writerIndex += length;
        return this;
    }

    @Override
    public int writeBytes(InputStream in, int length)
            throws IOException {
        ensureWritable(length);
        int writtenBytes = setBytes(writerIndex, in, length);
        if (writtenBytes > 0) {
            writerIndex += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
        ensureWritable(length);
        int writtenBytes = setBytes(writerIndex, in, length);
        if (writtenBytes > 0) {
            writerIndex += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public int writeBytes(FileChannel in, long position, int length) throws IOException {
        ensureWritable(length);
        int writtenBytes = setBytes(writerIndex, in, position, length);
        if (writtenBytes > 0) {
            writerIndex += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public ByteBuf writeZero(int length) {
        if (length == 0) {
            return this;
        }

        ensureWritable(length);
        int wIndex = writerIndex;
        checkIndex0(wIndex, length);

        int nLong = length >>> 3;
        int nBytes = length & 7;
        for (int i = nLong; i > 0; i--) {
            _setLong(wIndex, 0);
            wIndex += 8;
        }
        if (nBytes == 4) {
            _setInt(wIndex, 0);
            wIndex += 4;
        } else if (nBytes < 4) {
            for (int i = nBytes; i > 0; i--) {
                _setByte(wIndex, 0);
                wIndex++;
            }
        } else {
            _setInt(wIndex, 0);
            wIndex += 4;
            for (int i = nBytes - 4; i > 0; i--) {
                _setByte(wIndex, 0);
                wIndex++;
            }
        }
        writerIndex = wIndex;
        return this;
    }

    @Override
    public int writeCharSequence(CharSequence sequence, Charset charset) {
        int written = setCharSequence0(writerIndex, sequence, charset, true);
        writerIndex += written;
        return written;
    }

    @Override
    public ByteBuf copy() {
        return copy(readerIndex, readableBytes());
    }

    @Override
    public ByteBuf duplicate() {
        ensureAccessible();
        return new UnpooledDuplicatedByteBuf(this);
    }

    @Override
    public ByteBuf retainedDuplicate() {
        return duplicate().retain();
    }

    @Override
    public ByteBuf slice() {
        return slice(readerIndex, readableBytes());
    }

    @Override
    public ByteBuf retainedSlice() {
        return slice().retain();
    }

    @Override
    public ByteBuf slice(int index, int length) {
        ensureAccessible();
        return new UnpooledSlicedByteBuf(this, index, length);
    }

    @Override
    public ByteBuf retainedSlice(int index, int length) {
        return slice(index, length).retain();
    }

    @Override
    public ByteBuffer nioBuffer() {
        return nioBuffer(readerIndex, readableBytes());
    }

    @Override
    public ByteBuffer[] nioBuffers() {
        return nioBuffers(readerIndex, readableBytes());
    }

    @Override
    public String toString(Charset charset) {
        return toString(readerIndex, readableBytes(), charset);
    }

    @Override
    public String toString(int index, int length, Charset charset) {
        return ByteBufUtil.decodeString(this, index, length, charset);
    }

    @Override
    public int indexOf(int fromIndex, int toIndex, byte value) {
        if (fromIndex <= toIndex) {
            return ByteBufUtil.firstIndexOf(this, fromIndex, toIndex, value);
        }
        return ByteBufUtil.lastIndexOf(this, fromIndex, toIndex, value);
    }

    @Override
    public int bytesBefore(byte value) {
        return bytesBefore(readerIndex(), readableBytes(), value);
    }

    @Override
    public int bytesBefore(int length, byte value) {
        checkReadableBytes(length);
        return bytesBefore(readerIndex(), length, value);
    }

    @Override
    public int bytesBefore(int index, int length, byte value) {
        int endIndex = indexOf(index, index + length, value);
        if (endIndex < 0) {
            return -1;
        }
        return endIndex - index;
    }

    @Override
    public int forEachByte(ByteProcessor processor) {
        ensureAccessible();
        try {
            return forEachByteAsc0(readerIndex, writerIndex, processor);
        } catch (Exception e) {
            PlatformDependent.throwException(e);
            return -1;
        }
    }

    @Override
    public int forEachByte(int index, int length, ByteProcessor processor) {
        checkIndex(index, length);
        try {
            return forEachByteAsc0(index, index + length, processor);
        } catch (Exception e) {
            PlatformDependent.throwException(e);
            return -1;
        }
    }

    int forEachByteAsc0(int start, int end, ByteProcessor processor) throws Exception {
        for (; start < end; ++start) {
            if (!processor.process(_getByte(start))) {
                return start;
            }
        }

        return -1;
    }

    @Override
    public int forEachByteDesc(ByteProcessor processor) {
        ensureAccessible();
        try {
            return forEachByteDesc0(writerIndex - 1, readerIndex, processor);
        } catch (Exception e) {
            PlatformDependent.throwException(e);
            return -1;
        }
    }

    @Override
    public int forEachByteDesc(int index, int length, ByteProcessor processor) {
        checkIndex(index, length);
        try {
            return forEachByteDesc0(index + length - 1, index, processor);
        } catch (Exception e) {
            PlatformDependent.throwException(e);
            return -1;
        }
    }

    int forEachByteDesc0(int rStart, final int rEnd, ByteProcessor processor) throws Exception {
        for (; rStart >= rEnd; --rStart) {
            if (!processor.process(_getByte(rStart))) {
                return rStart;
            }
        }
        return -1;
    }

    @Override
    public int hashCode() {
        return ByteBufUtil.hashCode(this);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ByteBuf && ByteBufUtil.equals(this, (ByteBuf) o);
    }

    @Override
    public int compareTo(ByteBuf that) {
        return ByteBufUtil.compare(this, that);
    }

    @Override
    public String toString() {
        if (refCnt() == 0) {
            return StringUtil.simpleClassName(this) + "(freed)";
        }

        StringBuilder buf = new StringBuilder()
                .append(StringUtil.simpleClassName(this))
                .append("(ridx: ").append(readerIndex)
                .append(", widx: ").append(writerIndex)
                .append(", cap: ").append(capacity());
        if (maxCapacity != Integer.MAX_VALUE) {
            buf.append('/').append(maxCapacity);
        }

        ByteBuf unwrapped = unwrap();
        if (unwrapped != null) {
            buf.append(", unwrapped: ").append(unwrapped);
        }
        buf.append(')');
        return buf.toString();
    }

    protected final void checkIndex(int index) {
        checkIndex(index, 1);
    }

    protected final void checkIndex(int index, int fieldLength) {
        ensureAccessible();
        checkIndex0(index, fieldLength);
    }

    private static void checkRangeBounds(final String indexName, final int index,
            final int fieldLength, final int capacity) {
        if (isOutOfBounds(index, fieldLength, capacity)) {
            throw new IndexOutOfBoundsException(String.format(
                    "%s: %d, length: %d (expected: range(0, %d))", indexName, index, fieldLength, capacity));
        }
    }

    final void checkIndex0(int index, int fieldLength) {
        if (checkBounds) {
            checkRangeBounds("index", index, fieldLength, capacity());
        }
    }

    protected final void checkSrcIndex(int index, int length, int srcIndex, int srcCapacity) {
        checkIndex(index, length);
        if (checkBounds) {
            checkRangeBounds("srcIndex", srcIndex, length, srcCapacity);
        }
    }

    protected final void checkDstIndex(int index, int length, int dstIndex, int dstCapacity) {
        checkIndex(index, length);
        if (checkBounds) {
            checkRangeBounds("dstIndex", dstIndex, length, dstCapacity);
        }
    }

    protected final void checkDstIndex(int length, int dstIndex, int dstCapacity) {
        checkReadableBytes(length);
        if (checkBounds) {
            checkRangeBounds("dstIndex", dstIndex, length, dstCapacity);
        }
    }

    /**
     * Throws an {@link IndexOutOfBoundsException} if the current
     * {@linkplain #readableBytes() readable bytes} of this buffer is less
     * than the specified value.
     */
    protected final void checkReadableBytes(int minimumReadableBytes) {
        checkReadableBytes0(checkPositiveOrZero(minimumReadableBytes, "minimumReadableBytes"));
    }

    protected final void checkNewCapacity(int newCapacity) {
        ensureAccessible();
        if (checkBounds && (newCapacity < 0 || newCapacity > maxCapacity())) {
            throw new IllegalArgumentException("newCapacity: " + newCapacity +
                    " (expected: 0-" + maxCapacity() + ')');
        }
    }

    private void checkReadableBytes0(int minimumReadableBytes) {
        ensureAccessible();
        if (checkBounds && readerIndex > writerIndex - minimumReadableBytes) {
            throw new IndexOutOfBoundsException(String.format(
                    "readerIndex(%d) + length(%d) exceeds writerIndex(%d): %s",
                    readerIndex, minimumReadableBytes, writerIndex, this));
        }
    }

    /**
     * Should be called by every method that tries to access the buffers content to
     * check
     * if the buffer was released before.
     */
    protected final void ensureAccessible() {
        if (checkAccessible && !isAccessible()) {
            throw new IllegalReferenceCountException(0);
        }
    }

    final void setIndex0(int readerIndex, int writerIndex) {
        this.readerIndex = readerIndex;
        this.writerIndex = writerIndex;
    }

    final void discardMarks() {
        markedReaderIndex = markedWriterIndex = 0;
    }
}
