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

import io.netty.util.Recycler.EnhancedHandle;
import io.netty.util.internal.ObjectPool.Handle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * 池化的ByteBuf抽象基类
 * 
 * <p>此类是Netty内存池系统的核心组件，实现了高效的内存分配与回收机制。相比于非池化的ByteBuf，
 * 它能够显著减少内存分配和垃圾回收的开销，提高高并发场景下的性能。</p>
 * 
 * <p>主要特性：</p>
 * <ul>
 *   <li><b>内存池化</b> - 预分配内存并在多个请求间复用</li>
 *   <li><b>引用计数</b> - 通过引用计数进行内存管理</li>
 *   <li><b>智能扩容</b> - 根据使用模式自动调整容量</li>
 *   <li><b>线程本地缓存</b> - 通过线程本地缓存减少线程竞争</li>
 *   <li><b>对象回收</b> - 利用对象池避免频繁创建实例</li>
 * </ul>
 * 
 * <p>工作原理：</p>
 * <ol>
 *   <li>从内存池（PoolArena）中分配预定大小的内存块</li>
 *   <li>通过引用计数跟踪内存使用情况</li>
 *   <li>当引用计数为0时，将内存返回到池中而不是释放</li>
 *   <li>使用线程本地缓存加速频繁使用的内存块分配</li>
 * </ol>
 * 
 * <p>适用场景：</p>
 * <ul>
 *   <li>高并发网络应用</li>
 *   <li>需要频繁创建和销毁ByteBuf的场景</li>
 *   <li>对内存使用效率和GC性能有较高要求的应用</li>
 * </ul>
 * 
 * <p>通用泛型参数T表示底层内存的类型，可以是直接内存（DirectBuffer）或堆内存（byte[]）</p>
 *
 * @param <T> 底层内存类型，通常是byte[]用于堆缓冲区或ByteBuffer用于直接缓冲区
 */
abstract class PooledByteBuf<T> extends AbstractReferenceCountedByteBuf {

    /**
     * 对象回收处理器
     * 用于在ByteBuf不再使用时将其回收到对象池中而不是被垃圾回收
     * 这减少了对象创建和销毁的开销
     */
    private final EnhancedHandle<PooledByteBuf<T>> recyclerHandle;

    /**
     * 当前ByteBuf所属的内存块
     * 包含分配的内存区域和管理信息
     */
    protected PoolChunk<T> chunk;
    
    /**
     * 在内存块内的句柄值
     * 用于标识内存块内的特定区域
     * 在释放内存时使用此句柄找到对应区域
     */
    protected long handle;
    
    /**
     * 实际的内存引用
     * 可能是堆内存(byte[])或直接内存(ByteBuffer)
     */
    protected T memory;
    
    /**
     * 在内存中的起始偏移量
     * 表示当前ByteBuf在整块内存中的起始位置
     */
    protected int offset;
    
    /**
     * 当前分配的内存长度
     * 决定了ByteBuf的容量
     */
    protected int length;
    
    /**
     * 最大可用内存长度
     * 通常大于等于length，用于优化扩容操作
     */
    int maxLength;
    
    /**
     * 线程本地缓存
     * 用于加速频繁使用的小块内存的分配和释放
     */
    PoolThreadCache cache;
    
    /**
     * 临时NIO ByteBuffer
     * 用于操作需要ByteBuffer的场景，避免重复创建
     */
    ByteBuffer tmpNioBuf;
    
    /**
     * ByteBuf的分配器
     * 用于再分配和容量调整
     */
    private ByteBufAllocator allocator;

    /**
     * 构造一个新的池化ByteBuf
     * 
     * @param recyclerHandle 对象回收处理器，用于在释放后回收对象
     * @param maxCapacity 此ByteBuf允许的最大容量
     */
    @SuppressWarnings("unchecked")
    protected PooledByteBuf(Handle<? extends PooledByteBuf<T>> recyclerHandle, int maxCapacity) {
        super(maxCapacity);
        this.recyclerHandle = (EnhancedHandle<PooledByteBuf<T>>) recyclerHandle;
    }

    /**
     * 初始化池化的ByteBuf
     * 设置内存块、偏移量、长度等必要参数
     * 
     * @param chunk 内存块，包含实际的内存
     * @param nioBuffer 可选的NIO缓冲区包装
     * @param handle 在内存块中的句柄值
     * @param offset 在内存中的起始偏移量
     * @param length 当前分配的内存长度
     * @param maxLength 最大可用内存长度
     * @param cache 线程本地缓存，用于后续内存操作
     */
    void init(PoolChunk<T> chunk, ByteBuffer nioBuffer,
              long handle, int offset, int length, int maxLength, PoolThreadCache cache) {
        init0(chunk, nioBuffer, handle, offset, length, maxLength, cache);
    }

    /**
     * 初始化非池化的ByteBuf
     * 用于特殊场景下的非池化分配
     * 
     * @param chunk 内存块
     * @param length 内存长度
     */
    void initUnpooled(PoolChunk<T> chunk, int length) {
        init0(chunk, null, 0, 0, length, length, null);
    }

    /**
     * 实际的初始化实现
     * 设置所有必要的内部字段
     * 
     * @param chunk 内存块
     * @param nioBuffer NIO缓冲区
     * @param handle 内存句柄
     * @param offset 偏移量
     * @param length 长度
     * @param maxLength 最大长度
     * @param cache 线程缓存
     */
    private void init0(PoolChunk<T> chunk, ByteBuffer nioBuffer,
                       long handle, int offset, int length, int maxLength, PoolThreadCache cache) {
        // 验证参数合法性
        assert handle >= 0;
        assert chunk != null;
        // 确保小页面分配的是合适大小的缓冲区
        assert !PoolChunk.isSubpage(handle) ||
                chunk.arena.sizeClass.size2SizeIdx(maxLength) <= chunk.arena.sizeClass.smallMaxSizeIdx:
                "Allocated small sub-page handle for a buffer size that isn't \"small.\"";

        // 增加内存块的引用计数，防止提前释放
        chunk.incrementPinnedMemory(maxLength);
        this.chunk = chunk;
        memory = chunk.memory;
        tmpNioBuf = nioBuffer;
        allocator = chunk.arena.parent;
        this.cache = cache;
        this.handle = handle;
        this.offset = offset;
        this.length = length;
        this.maxLength = maxLength;
    }

    /**
     * 重用此ByteBuf对象
     * 在对象池回收再利用时调用，重置状态
     * 
     * @param maxCapacity 新的最大容量
     */
    final void reuse(int maxCapacity) {
        // 设置新的最大容量
        maxCapacity(maxCapacity);
        // 重置引用计数为1
        resetRefCnt();
        // 重置读写指针为0
        setIndex0(0, 0);
        // 清除所有标记
        discardMarks();
    }

    /**
     * 返回此缓冲区的当前容量
     * 
     * @return 当前容量（字节数）
     */
    @Override
    public final int capacity() {
        return length;
    }

    /**
     * 返回最大可快速写入的字节数
     * 不需要扩容就能写入的最大字节数
     * 
     * @return 最大可快速写入的字节数
     */
    @Override
    public int maxFastWritableBytes() {
        return Math.min(maxLength, maxCapacity()) - writerIndex;
    }

    /**
     * 调整此缓冲区的容量
     * 根据需要可能会重新分配内存
     * 
     * @param newCapacity 新的容量大小
     * @return 此缓冲区实例
     */
    @Override
    public final ByteBuf capacity(int newCapacity) {
        // 如果新容量等于当前容量，直接返回
        if (newCapacity == length) {
            ensureAccessible();
            return this;
        }
        // 检查新容量是否合法
        checkNewCapacity(newCapacity);
        
        // 非unpooled的块可以尝试优化扩容
        if (!chunk.unpooled) {
            // 扩容情况：如果新容量小于等于maxLength，可以直接调整长度
            if (newCapacity > length) {
                if (newCapacity <= maxLength) {
                    length = newCapacity;
                    return this;
                }
            } 
            // 缩容情况：如果新容量大于maxLength的一半，或者符合特定条件，可以直接调整长度
            else if (newCapacity > maxLength >>> 1 &&
                    (maxLength > 512 || newCapacity > maxLength - 16)) {
                // 这里newCapacity < length
                length = newCapacity;
                trimIndicesToCapacity(newCapacity);
                return this;
            }
        }

        // 如果无法通过简单调整长度完成，需要重新分配内存
        chunk.arena.reallocate(this, newCapacity);
        return this;
    }

    /**
     * 返回此缓冲区的分配器
     * 
     * @return 创建此缓冲区的ByteBufAllocator
     */
    @Override
    public final ByteBufAllocator alloc() {
        return allocator;
    }

    /**
     * 返回此缓冲区的字节序
     * 池化缓冲区总是使用大端字节序
     * 
     * @return ByteOrder.BIG_ENDIAN
     */
    @Override
    public final ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    /**
     * 返回此缓冲区包装的底层缓冲区
     * 池化缓冲区不是另一个缓冲区的包装，因此返回null
     * 
     * @return null
     */
    @Override
    public final ByteBuf unwrap() {
        return null;
    }

    /**
     * 创建此缓冲区的复制视图，共享内容但有独立的索引
     * 会增加引用计数
     * 
     * @return 新的复制视图ByteBuf
     */
    @Override
    public final ByteBuf retainedDuplicate() {
        return PooledDuplicatedByteBuf.newInstance(this, this, readerIndex(), writerIndex());
    }

    /**
     * 创建此缓冲区当前可读部分的切片视图
     * 会增加引用计数
     * 
     * @return 新的切片视图ByteBuf
     */
    @Override
    public final ByteBuf retainedSlice() {
        final int index = readerIndex();
        return retainedSlice(index, writerIndex() - index);
    }

    /**
     * 创建此缓冲区指定范围的切片视图
     * 会增加引用计数
     * 
     * @param index 切片的起始索引
     * @param length 切片的长度
     * @return 新的切片视图ByteBuf
     */
    @Override
    public final ByteBuf retainedSlice(int index, int length) {
        return PooledSlicedByteBuf.newInstance(this, this, index, length);
    }

    /**
     * 获取内部使用的NIO ByteBuffer
     * 如果不存在则创建新的
     * 
     * @return 可用于操作的临时NIO ByteBuffer
     */
    protected final ByteBuffer internalNioBuffer() {
        ByteBuffer tmpNioBuf = this.tmpNioBuf;
        if (tmpNioBuf == null) {
            // 如果还没有创建过，创建新的NIO缓冲区
            this.tmpNioBuf = tmpNioBuf = newInternalNioBuffer(memory);
        } else {
            // 重用已有的，先清除之前的状态
            tmpNioBuf.clear();
        }
        return tmpNioBuf;
    }

    /**
     * 创建新的内部NIO ByteBuffer
     * 由具体子类实现，根据内存类型T创建适当的ByteBuffer
     * 
     * @param memory 内存引用
     * @return 新创建的ByteBuffer
     */
    protected abstract ByteBuffer newInternalNioBuffer(T memory);

    /**
     * 释放底层资源
     * 当引用计数降为0时调用
     * 将内存返回到池中并回收此对象
     */
    @Override
    protected final void deallocate() {
        if (handle >= 0) {
            final long handle = this.handle;
            this.handle = -1;
            memory = null;
            // 释放内存块
            chunk.arena.free(chunk, tmpNioBuf, handle, maxLength, cache);
            tmpNioBuf = null;
            chunk = null;
            cache = null;
            // 回收此对象到对象池
            this.recyclerHandle.unguardedRecycle(this);
        }
    }

    /**
     * 计算实际内存索引
     * 将逻辑索引转换为内存中的实际位置
     * 
     * @param index 逻辑索引
     * @return 实际内存索引
     */
    protected final int idx(int index) {
        return offset + index;
    }

    /**
     * 获取指定范围的内部NIO ByteBuffer
     * 
     * @param index 起始索引
     * @param length 长度
     * @param duplicate 是否创建新的副本
     * @return 配置好位置和限制的ByteBuffer
     */
    final ByteBuffer _internalNioBuffer(int index, int length, boolean duplicate) {
        index = idx(index);
        ByteBuffer buffer = duplicate ? newInternalNioBuffer(memory) : internalNioBuffer();
        buffer.limit(index + length).position(index);
        return buffer;
    }

    /**
     * 创建内部NIO ByteBuffer的副本
     * 用于需要独立副本的场景
     * 
     * @param index 起始索引
     * @param length 长度
     * @return 新的ByteBuffer副本
     */
    ByteBuffer duplicateInternalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, true);
    }

    /**
     * 获取内部NIO ByteBuffer，用于读写操作
     * 
     * @param index 起始索引
     * @param length 长度
     * @return 配置好的内部ByteBuffer
     */
    @Override
    public final ByteBuffer internalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, false);
    }

    /**
     * 返回此缓冲区的NIO缓冲区数量
     * 池化缓冲区总是由单个内存块组成
     * 
     * @return 始终为1
     */
    @Override
    public final int nioBufferCount() {
        return 1;
    }

    /**
     * 获取此缓冲区指定范围的NIO ByteBuffer
     * 
     * @param index 起始索引
     * @param length 长度
     * @return 新创建的ByteBuffer切片
     */
    @Override
    public final ByteBuffer nioBuffer(int index, int length) {
        return duplicateInternalNioBuffer(index, length).slice();
    }

    /**
     * 获取此缓冲区指定范围的NIO ByteBuffer数组
     * 
     * @param index 起始索引
     * @param length 长度
     * @return 包含单个ByteBuffer的数组
     */
    @Override
    public final ByteBuffer[] nioBuffers(int index, int length) {
        return new ByteBuffer[] { nioBuffer(index, length) };
    }

    /**
     * 判断此缓冲区是否是连续的内存
     * 池化缓冲区总是连续的
     * 
     * @return 始终为true
     */
    @Override
    public final boolean isContiguous() {
        return true;
    }

    /**
     * 将指定范围的数据写入到输出通道
     * 
     * @param index 数据起始索引
     * @param out 目标输出通道
     * @param length 数据长度
     * @return 写入的字节数
     * @throws IOException 如果I/O操作失败
     */
    @Override
    public final int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        return out.write(duplicateInternalNioBuffer(index, length));
    }

    /**
     * 从当前读取位置读取数据并写入到输出通道
     * 读取完成后会更新读取索引
     * 
     * @param out 目标输出通道
     * @param length 要读取的长度
     * @return 实际读取并写入的字节数
     * @throws IOException 如果I/O操作失败
     */
    @Override
    public final int readBytes(GatheringByteChannel out, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(_internalNioBuffer(readerIndex, length, false));
        readerIndex += readBytes;
        return readBytes;
    }

    /**
     * 将指定范围的数据写入到文件通道的指定位置
     * 
     * @param index 数据起始索引
     * @param out 目标文件通道
     * @param position 文件中的写入位置
     * @param length 数据长度
     * @return 写入的字节数
     * @throws IOException 如果I/O操作失败
     */
    @Override
    public final int getBytes(int index, FileChannel out, long position, int length) throws IOException {
        return out.write(duplicateInternalNioBuffer(index, length), position);
    }

    /**
     * 从当前读取位置读取数据并写入到文件通道的指定位置
     * 读取完成后会更新读取索引
     * 
     * @param out 目标文件通道
     * @param position 文件中的写入位置
     * @param length 要读取的长度
     * @return 实际读取并写入的字节数
     * @throws IOException 如果I/O操作失败
     */
    @Override
    public final int readBytes(FileChannel out, long position, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(_internalNioBuffer(readerIndex, length, false), position);
        readerIndex += readBytes;
        return readBytes;
    }

    /**
     * 从输入通道读取数据到指定位置
     * 
     * @param index 写入的起始位置
     * @param in 源输入通道
     * @param length 最大读取长度
     * @return 实际读取的字节数，如果通道已关闭则返回-1
     * @throws IOException 如果I/O操作失败
     */
    @Override
    public final int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length));
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }

    /**
     * 从文件通道的指定位置读取数据到缓冲区的指定位置
     * 
     * @param index 写入的起始位置
     * @param in 源文件通道
     * @param position 文件中的读取位置
     * @param length 最大读取长度
     * @return 实际读取的字节数，如果通道已关闭则返回-1
     * @throws IOException 如果I/O操作失败
     */
    @Override
    public final int setBytes(int index, FileChannel in, long position, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length), position);
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }
}
