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

import static io.netty.buffer.PoolChunk.isSubpage;
import static java.lang.Math.max;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.util.internal.LongCounter;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;

/**
 * <p>
 * Netty内存管理系统的核心组件，负责内存块的分配、管理和回收。
 * </p>
 * 
 * <p>
 * PoolArena是一个内存分配区域的抽象，管理多个内存块(chunk)和小内存页(subpage)，
 * 根据请求的内存大小采用不同的分配策略。它通过多级分类和缓存机制，实现高效的内存
 * 分配和复用，减少内存碎片和GC压力。
 * </p>
 * 
 * <h3>内存分配策略</h3>
 * <ul>
 * <li><b>小对象</b> (≤4KB): 使用subpage级别分配，优先从线程本地缓存分配</li>
 * <li><b>普通对象</b> (4KB-16MB): 在chunk内进行页级别分配，使用伙伴算法</li>
 * <li><b>大对象</b> (>16MB): 直接分配独立chunk，不进入缓存系统</li>
 * </ul>
 * 
 * <h3>内存管理特性</h3>
 * <ul>
 * <li>通过多级PoolChunkList管理不同使用率的内存块</li>
 * <li>支持线程本地缓存(PoolThreadCache)减少锁竞争</li>
 * <li>使用伙伴分配算法和位图追踪内存分配状态</li>
 * <li>提供详细的内存使用统计和监控能力</li>
 * <li>支持堆内存和直接内存的统一抽象</li>
 * </ul>
 * 
 * <p>
 * PoolArena是一个抽象类，有两个具体实现：
 * </p>
 * <ul>
 * <li>{@link HeapArena} - 管理基于JVM堆的内存</li>
 * <li>{@link DirectArena} - 管理堆外直接内存</li>
 * </ul>
 * 
 * @param <T> 内存类型，对于HeapArena是byte[]，对于DirectArena是ByteBuffer
 * 
 * @see PooledByteBufAllocator
 * @see PoolChunk
 * @see PoolSubpage
 * @see PoolThreadCache
 * @see SizeClasses
 */
abstract class PoolArena<T> implements PoolArenaMetric {
    private static final boolean HAS_UNSAFE = PlatformDependent.hasUnsafe();

    enum SizeClass {
        Small,
        Normal
    }

    final PooledByteBufAllocator parent;

    final PoolSubpage<T>[] smallSubpagePools;

    private final PoolChunkList<T> q050;
    private final PoolChunkList<T> q025;
    private final PoolChunkList<T> q000;
    private final PoolChunkList<T> qInit;
    private final PoolChunkList<T> q075;
    private final PoolChunkList<T> q100;

    private final List<PoolChunkListMetric> chunkListMetrics;

    // Metrics for allocations and deallocations
    private long allocationsNormal;
    // We need to use the LongCounter here as this is not guarded via synchronized
    // block.
    private final LongCounter allocationsSmall = PlatformDependent.newLongCounter();
    private final LongCounter allocationsHuge = PlatformDependent.newLongCounter();
    private final LongCounter activeBytesHuge = PlatformDependent.newLongCounter();

    private long deallocationsSmall;
    private long deallocationsNormal;

    // We need to use the LongCounter here as this is not guarded via synchronized
    // block.
    private final LongCounter deallocationsHuge = PlatformDependent.newLongCounter();

    // Number of thread caches backed by this arena.
    final AtomicInteger numThreadCaches = new AtomicInteger();

    // TODO: Test if adding padding helps under contention
    // private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    private final ReentrantLock lock = new ReentrantLock();

    final SizeClasses sizeClass;

    /**
     * PoolArena的构造函数
     * 负责初始化内存分配的核心组件，包括Subpage池和Chunk列表
     */
    protected PoolArena(PooledByteBufAllocator parent, SizeClasses sizeClass) {
        // 1. 参数校验和基础属性初始化
        assert null != sizeClass;
        this.parent = parent;
        this.sizeClass = sizeClass;

        // 2. 初始化小对象Subpage池
        smallSubpagePools = newSubpagePoolArray(sizeClass.nSubpages);
        for (int i = 0; i < smallSubpagePools.length; i++) {
            smallSubpagePools[i] = newSubpagePoolHead(i);
        }

        // 3. 初始化Chunk列表，按使用率分类
        // q100: 使用率100%的Chunk列表
        q100 = new PoolChunkList<T>(this, null, 100, Integer.MAX_VALUE, sizeClass.chunkSize);
        // q075: 使用率75%-100%的Chunk列表
        q075 = new PoolChunkList<T>(this, q100, 75, 100, sizeClass.chunkSize);
        // q050: 使用率50%-100%的Chunk列表
        q050 = new PoolChunkList<T>(this, q100, 50, 100, sizeClass.chunkSize);
        // q025: 使用率25%-75%的Chunk列表
        q025 = new PoolChunkList<T>(this, q050, 25, 75, sizeClass.chunkSize);
        // q000: 使用率1%-50%的Chunk列表
        q000 = new PoolChunkList<T>(this, q025, 1, 50, sizeClass.chunkSize);
        // qInit: 新创建的Chunk列表
        qInit = new PoolChunkList<T>(this, q000, Integer.MIN_VALUE, 25, sizeClass.chunkSize);

        // 4. 建立Chunk列表之间的双向链表关系
        q100.prevList(q075);
        q075.prevList(q050);
        q050.prevList(q025);
        q025.prevList(q000);
        q000.prevList(null);
        qInit.prevList(qInit);

        // 5. 创建不可变的Chunk列表指标集合
        List<PoolChunkListMetric> metrics = new ArrayList<PoolChunkListMetric>(6);
        metrics.add(qInit);
        metrics.add(q000);
        metrics.add(q025);
        metrics.add(q050);
        metrics.add(q075);
        metrics.add(q100);
        chunkListMetrics = Collections.unmodifiableList(metrics);
    }

    private PoolSubpage<T> newSubpagePoolHead(int index) {
        PoolSubpage<T> head = new PoolSubpage<T>(index);
        head.prev = head;
        head.next = head;
        return head;
    }

    @SuppressWarnings("unchecked")
    private PoolSubpage<T>[] newSubpagePoolArray(int size) {
        return new PoolSubpage[size];
    }

    abstract boolean isDirect();

    /**
     * PoolArena中的内存分配入口方法
     * 负责创建新的ByteBuf并分配内存空间
     * 
     * @param cache       线程本地缓存，用于加速内存分配
     * @param reqCapacity 请求的内存容量
     * @param maxCapacity ByteBuf的最大容量
     * @return 分配好的PooledByteBuf实例
     */
    PooledByteBuf<T> allocate(PoolThreadCache cache, int reqCapacity, int maxCapacity) {
        // 1. 创建新的ByteBuf实例
        PooledByteBuf<T> buf = newByteBuf(maxCapacity);
        // 2. 为ByteBuf分配实际的内存空间
        allocate(cache, buf, reqCapacity);
        return buf;
    }

    /**
     * PoolArena中的内存分配核心方法
     * 根据请求大小选择不同的分配策略
     * 
     * <p>
     * <strong>内存分配策略流程</strong>:
     * </p>
     * <ol>
     * <li><strong>请求大小分类：</strong> 首先将请求容量转换为对应的大小类别索引(sizeIdx)</li>
     * <li><strong>分配策略选择：</strong> 根据sizeIdx选择不同的分配路径：
     * <ul>
     * <li><em>小对象分配</em> (≤4KB):
     * <ol>
     * <li>优先从线程本地缓存(PoolThreadCache)分配</li>
     * <li>缓存未命中时，从共享的小内存页(subpage)分配</li>
     * <li>无可用小内存页时，分配新的内存页并切分</li>
     * </ol>
     * </li>
     * <li><em>普通对象分配</em> (4KB-16MB):
     * <ol>
     * <li>尝试从线程本地缓存分配单页或多页</li>
     * <li>缓存未命中时，寻找最佳匹配的内存块(chunk)</li>
     * <li>使用伙伴算法在内存块内分配合适大小</li>
     * </ol>
     * </li>
     * <li><em>大对象分配</em> (>16MB):
     * <ol>
     * <li>直接分配专用内存块，不进入缓存</li>
     * <li>根据需要进行内存对齐处理</li>
     * <li>大对象释放时直接归还系统</li>
     * </ol>
     * </li>
     * </ul>
     * </li>
     * <li><strong>内存初始化：</strong> 分配后设置PooledByteBuf的内存引用、偏移量和容量</li>
     * <li><strong>统计更新：</strong> 记录分配的内存大小和分配次数用于监控</li>
     * </ol>
     * 
     * <p>
     * <strong>优化策略</strong>:
     * </p>
     * <ul>
     * <li>使用线程本地缓存减少线程间同步开销</li>
     * <li>采用分级分配减少内存碎片</li>
     * <li>内存规格化和对齐提高访问效率</li>
     * </ul>
     * 
     * @param cache       线程本地缓存
     * @param buf         待分配的ByteBuf
     * @param reqCapacity 请求的内存容量
     */
    private void allocate(PoolThreadCache cache, PooledByteBuf<T> buf, final int reqCapacity) {
        // 1. 将请求容量转换为sizeIdx
        final int sizeIdx = sizeClass.size2SizeIdx(reqCapacity);

        // 2. 根据sizeIdx选择分配策略
        if (sizeIdx <= sizeClass.smallMaxSizeIdx) {
            // 小对象分配策略 (<4KB)
            tcacheAllocateSmall(cache, buf, reqCapacity, sizeIdx);
        } else if (sizeIdx < sizeClass.nSizes) {
            // 普通对象分配策略 (4KB-16MB)
            tcacheAllocateNormal(cache, buf, reqCapacity, sizeIdx);
        } else {
            // 大对象分配策略 (>16MB)
            int normCapacity = sizeClass.directMemoryCacheAlignment > 0
                    ? sizeClass.normalizeSize(reqCapacity) // 需要内存对齐时进行规范化
                    : reqCapacity; // 不需要对齐时直接使用
            allocateHuge(buf, normCapacity);
        }
    }

    private void tcacheAllocateSmall(PoolThreadCache cache, PooledByteBuf<T> buf,
            final int reqCapacity, final int sizeIdx) {

        // 1. 首先尝试从线程缓存中分配
        if (cache.allocateSmall(this, buf, reqCapacity, sizeIdx)) {
            return; // 如果从缓存分配成功，直接返回
        }

        // 2. 获取对应大小的 Subpage 池头节点
        final PoolSubpage<T> head = smallSubpagePools[sizeIdx];
        final boolean needsNormalAllocation;

        // 3. 加锁保护 Subpage 池的访问
        head.lock();
        try {
            // 4. 获取下一个可用的 Subpage
            final PoolSubpage<T> s = head.next;
            needsNormalAllocation = s == head; // 判断是否需要正常分配

            // 5. 如果 Subpage 池不为空
            if (!needsNormalAllocation) {
                // 验证 Subpage 状态
                assert s.doNotDestroy && s.elemSize == sizeClass.sizeIdx2size(sizeIdx)
                        : "doNotDestroy=" + s.doNotDestroy +
                                ", elemSize=" + s.elemSize +
                                ", sizeIdx=" + sizeIdx;

                // 6. 分配内存
                long handle = s.allocate();
                assert handle >= 0;

                // 7. 初始化 ByteBuf
                s.chunk.initBufWithSubpage(buf, null, handle, reqCapacity, cache);
            }
        } finally {
            head.unlock(); // 释放锁
        }

        // 8. 如果需要正常分配
        if (needsNormalAllocation) {
            lock(); // 加锁保护 Arena
            try {
                // 9. 执行正常分配
                allocateNormal(buf, reqCapacity, sizeIdx, cache);
            } finally {
                unlock(); // 释放锁
            }
        }

        // 10. 增加小内存分配计数
        incSmallAllocation();
    }

    private void tcacheAllocateNormal(PoolThreadCache cache, PooledByteBuf<T> buf, final int reqCapacity,
            final int sizeIdx) {
        if (cache.allocateNormal(this, buf, reqCapacity, sizeIdx)) {
            // was able to allocate out of the cache so move on
            return;
        }
        lock();
        try {
            allocateNormal(buf, reqCapacity, sizeIdx, cache);
            ++allocationsNormal;
        } finally {
            unlock();
        }
    }

    private void allocateNormal(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache threadCache) {
        assert lock.isHeldByCurrentThread();
        if (q050.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
                q025.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
                q000.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
                qInit.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
                q075.allocate(buf, reqCapacity, sizeIdx, threadCache)) {
            return;
        }

        // Add a new chunk.
        PoolChunk<T> c = newChunk(sizeClass.pageSize, sizeClass.nPSizes, sizeClass.pageShifts, sizeClass.chunkSize);
        boolean success = c.allocate(buf, reqCapacity, sizeIdx, threadCache);
        assert success;
        qInit.add(c);
    }

    private void incSmallAllocation() {
        allocationsSmall.increment();
    }

    private void allocateHuge(PooledByteBuf<T> buf, int reqCapacity) {
        PoolChunk<T> chunk = newUnpooledChunk(reqCapacity); // 步骤1: 创建非池化内存块
        activeBytesHuge.add(chunk.chunkSize()); // 步骤2: 更新内存使用统计
        buf.initUnpooled(chunk, reqCapacity); // 步骤3: 初始化缓冲区
        allocationsHuge.increment(); // 步骤4: 更新分配计数器
    }

    void free(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, int normCapacity, PoolThreadCache cache) {
        chunk.decrementPinnedMemory(normCapacity);
        if (chunk.unpooled) {
            int size = chunk.chunkSize();
            destroyChunk(chunk);
            activeBytesHuge.add(-size);
            deallocationsHuge.increment();
        } else {
            SizeClass sizeClass = sizeClass(handle);
            if (cache != null && cache.add(this, chunk, nioBuffer, handle, normCapacity, sizeClass)) {
                // cached so not free it.
                return;
            }

            freeChunk(chunk, handle, normCapacity, sizeClass, nioBuffer, false);
        }
    }

    private static SizeClass sizeClass(long handle) {
        return isSubpage(handle) ? SizeClass.Small : SizeClass.Normal;
    }

    void freeChunk(PoolChunk<T> chunk, long handle, int normCapacity, SizeClass sizeClass, ByteBuffer nioBuffer,
            boolean finalizer) {
        final boolean destroyChunk;
        lock();
        try {
            // We only call this if freeChunk is not called because of the PoolThreadCache
            // finalizer as otherwise this
            // may fail due lazy class-loading in for example tomcat.
            if (!finalizer) {
                switch (sizeClass) {
                    case Normal:
                        ++deallocationsNormal;
                        break;
                    case Small:
                        ++deallocationsSmall;
                        break;
                    default:
                        throw new Error();
                }
            }
            destroyChunk = !chunk.parent.free(chunk, handle, normCapacity, nioBuffer);
        } finally {
            unlock();
        }
        if (destroyChunk) {
            // destroyChunk not need to be called while holding the synchronized lock.
            destroyChunk(chunk);
        }
    }

    void reallocate(PooledByteBuf<T> buf, int newCapacity) {
        assert newCapacity >= 0 && newCapacity <= buf.maxCapacity();

        final int oldCapacity;
        final PoolChunk<T> oldChunk;
        final ByteBuffer oldNioBuffer;
        final long oldHandle;
        final T oldMemory;
        final int oldOffset;
        final int oldMaxLength;
        final PoolThreadCache oldCache;

        // We synchronize on the ByteBuf itself to ensure there is no "concurrent"
        // reallocations for the same buffer.
        // We do this to ensure the ByteBuf internal fields that are used to allocate /
        // free are not accessed
        // concurrently. This is important as otherwise we might end up corrupting our
        // internal state of our data
        // structures.
        //
        // Also note we don't use a Lock here but just synchronized even tho this might
        // seem like a bad choice for Loom.
        // This is done to minimize the overhead per ByteBuf. The time this would block
        // another thread should be
        // relative small and so not be a problem for Loom.
        // See https://github.com/netty/netty/issues/13467
        synchronized (buf) {
            oldCapacity = buf.length;
            if (oldCapacity == newCapacity) {
                return;
            }

            oldChunk = buf.chunk;
            oldNioBuffer = buf.tmpNioBuf;
            oldHandle = buf.handle;
            oldMemory = buf.memory;
            oldOffset = buf.offset;
            oldMaxLength = buf.maxLength;
            oldCache = buf.cache;

            // This does not touch buf's reader/writer indices
            allocate(parent.threadCache(), buf, newCapacity);
        }
        int bytesToCopy;
        if (newCapacity > oldCapacity) {
            bytesToCopy = oldCapacity;
        } else {
            buf.trimIndicesToCapacity(newCapacity);
            bytesToCopy = newCapacity;
        }
        memoryCopy(oldMemory, oldOffset, buf, bytesToCopy);
        free(oldChunk, oldNioBuffer, oldHandle, oldMaxLength, oldCache);
    }

    @Override
    public int numThreadCaches() {
        return numThreadCaches.get();
    }

    @Override
    public int numTinySubpages() {
        return 0;
    }

    @Override
    public int numSmallSubpages() {
        return smallSubpagePools.length;
    }

    @Override
    public int numChunkLists() {
        return chunkListMetrics.size();
    }

    @Override
    public List<PoolSubpageMetric> tinySubpages() {
        return Collections.emptyList();
    }

    @Override
    public List<PoolSubpageMetric> smallSubpages() {
        return subPageMetricList(smallSubpagePools);
    }

    @Override
    public List<PoolChunkListMetric> chunkLists() {
        return chunkListMetrics;
    }

    private static List<PoolSubpageMetric> subPageMetricList(PoolSubpage<?>[] pages) {
        List<PoolSubpageMetric> metrics = new ArrayList<PoolSubpageMetric>();
        for (PoolSubpage<?> head : pages) {
            if (head.next == head) {
                continue;
            }
            PoolSubpage<?> s = head.next;
            for (;;) {
                metrics.add(s);
                s = s.next;
                if (s == head) {
                    break;
                }
            }
        }
        return metrics;
    }

    @Override
    public long numAllocations() {
        final long allocsNormal;
        lock();
        try {
            allocsNormal = allocationsNormal;
        } finally {
            unlock();
        }
        return allocationsSmall.value() + allocsNormal + allocationsHuge.value();
    }

    @Override
    public long numTinyAllocations() {
        return 0;
    }

    @Override
    public long numSmallAllocations() {
        return allocationsSmall.value();
    }

    @Override
    public long numNormalAllocations() {
        lock();
        try {
            return allocationsNormal;
        } finally {
            unlock();
        }
    }

    @Override
    public long numDeallocations() {
        final long deallocs;
        lock();
        try {
            deallocs = deallocationsSmall + deallocationsNormal;
        } finally {
            unlock();
        }
        return deallocs + deallocationsHuge.value();
    }

    @Override
    public long numTinyDeallocations() {
        return 0;
    }

    @Override
    public long numSmallDeallocations() {
        lock();
        try {
            return deallocationsSmall;
        } finally {
            unlock();
        }
    }

    @Override
    public long numNormalDeallocations() {
        lock();
        try {
            return deallocationsNormal;
        } finally {
            unlock();
        }
    }

    @Override
    public long numHugeAllocations() {
        return allocationsHuge.value();
    }

    @Override
    public long numHugeDeallocations() {
        return deallocationsHuge.value();
    }

    @Override
    public long numActiveAllocations() {
        long val = allocationsSmall.value() + allocationsHuge.value()
                - deallocationsHuge.value();
        lock();
        try {
            val += allocationsNormal - (deallocationsSmall + deallocationsNormal);
        } finally {
            unlock();
        }
        return max(val, 0);
    }

    @Override
    public long numActiveTinyAllocations() {
        return 0;
    }

    @Override
    public long numActiveSmallAllocations() {
        return max(numSmallAllocations() - numSmallDeallocations(), 0);
    }

    @Override
    public long numActiveNormalAllocations() {
        final long val;
        lock();
        try {
            val = allocationsNormal - deallocationsNormal;
        } finally {
            unlock();
        }
        return max(val, 0);
    }

    @Override
    public long numActiveHugeAllocations() {
        return max(numHugeAllocations() - numHugeDeallocations(), 0);
    }

    @Override
    public long numActiveBytes() {
        long val = activeBytesHuge.value();
        lock();
        try {
            for (int i = 0; i < chunkListMetrics.size(); i++) {
                for (PoolChunkMetric m : chunkListMetrics.get(i)) {
                    val += m.chunkSize();
                }
            }
        } finally {
            unlock();
        }
        return max(0, val);
    }

    /**
     * Return an estimate of the number of bytes that are currently pinned to buffer
     * instances, by the arena. The
     * pinned memory is not accessible for use by any other allocation, until the
     * buffers using have all been released.
     */
    public long numPinnedBytes() {
        long val = activeBytesHuge.value(); // Huge chunks are exact-sized for the buffers they were allocated to.
        for (int i = 0; i < chunkListMetrics.size(); i++) {
            for (PoolChunkMetric m : chunkListMetrics.get(i)) {
                val += ((PoolChunk<?>) m).pinnedBytes();
            }
        }
        return max(0, val);
    }

    protected abstract PoolChunk<T> newChunk(int pageSize, int maxPageIdx, int pageShifts, int chunkSize);

    protected abstract PoolChunk<T> newUnpooledChunk(int capacity);

    protected abstract PooledByteBuf<T> newByteBuf(int maxCapacity);

    protected abstract void memoryCopy(T src, int srcOffset, PooledByteBuf<T> dst, int length);

    protected abstract void destroyChunk(PoolChunk<T> chunk);

    @Override
    public String toString() {
        lock();
        try {
            StringBuilder buf = new StringBuilder()
                    .append("Chunk(s) at 0~25%:")
                    .append(StringUtil.NEWLINE)
                    .append(qInit)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 0~50%:")
                    .append(StringUtil.NEWLINE)
                    .append(q000)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 25~75%:")
                    .append(StringUtil.NEWLINE)
                    .append(q025)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 50~100%:")
                    .append(StringUtil.NEWLINE)
                    .append(q050)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 75~100%:")
                    .append(StringUtil.NEWLINE)
                    .append(q075)
                    .append(StringUtil.NEWLINE)
                    .append("Chunk(s) at 100%:")
                    .append(StringUtil.NEWLINE)
                    .append(q100)
                    .append(StringUtil.NEWLINE)
                    .append("small subpages:");
            appendPoolSubPages(buf, smallSubpagePools);
            buf.append(StringUtil.NEWLINE);
            return buf.toString();
        } finally {
            unlock();
        }
    }

    private static void appendPoolSubPages(StringBuilder buf, PoolSubpage<?>[] subpages) {
        for (int i = 0; i < subpages.length; i++) {
            PoolSubpage<?> head = subpages[i];
            if (head.next == head || head.next == null) {
                continue;
            }

            buf.append(StringUtil.NEWLINE)
                    .append(i)
                    .append(": ");
            PoolSubpage<?> s = head.next;
            while (s != null) {
                buf.append(s);
                s = s.next;
                if (s == head) {
                    break;
                }
            }
        }
    }

    @Override
    protected final void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            destroyPoolSubPages(smallSubpagePools);
            destroyPoolChunkLists(qInit, q000, q025, q050, q075, q100);
        }
    }

    private static void destroyPoolSubPages(PoolSubpage<?>[] pages) {
        for (PoolSubpage<?> page : pages) {
            page.destroy();
        }
    }

    private void destroyPoolChunkLists(PoolChunkList<T>... chunkLists) {
        for (PoolChunkList<T> chunkList : chunkLists) {
            chunkList.destroy(this);
        }
    }

    static final class HeapArena extends PoolArena<byte[]> {
        private final AtomicReference<PoolChunk<byte[]>> lastDestroyedChunk;

        HeapArena(PooledByteBufAllocator parent, SizeClasses sizeClass) {
            super(parent, sizeClass);
            lastDestroyedChunk = new AtomicReference<PoolChunk<byte[]>>();
        }

        private static byte[] newByteArray(int size) {
            return PlatformDependent.allocateUninitializedArray(size);
        }

        @Override
        boolean isDirect() {
            return false;
        }

        @Override
        protected PoolChunk<byte[]> newChunk(int pageSize, int maxPageIdx, int pageShifts, int chunkSize) {
            PoolChunk<byte[]> chunk = lastDestroyedChunk.getAndSet(null);
            if (chunk != null) {
                assert chunk.chunkSize == chunkSize &&
                        chunk.pageSize == pageSize &&
                        chunk.maxPageIdx == maxPageIdx &&
                        chunk.pageShifts == pageShifts;
                return chunk; // The parameters are always the same, so it's fine to reuse a previously
                              // allocated chunk.
            }
            return new PoolChunk<byte[]>(
                    this, null, newByteArray(chunkSize), pageSize, pageShifts, chunkSize, maxPageIdx);
        }

        @Override
        protected PoolChunk<byte[]> newUnpooledChunk(int capacity) {
            return new PoolChunk<byte[]>(this, null, newByteArray(capacity), capacity);
        }

        @Override
        protected void destroyChunk(PoolChunk<byte[]> chunk) {
            // Rely on GC. But keep one chunk for reuse.
            if (!chunk.unpooled && lastDestroyedChunk.get() == null) {
                lastDestroyedChunk.set(chunk); // The check-and-set does not need to be atomic.
            }
        }

        @Override
        protected PooledByteBuf<byte[]> newByteBuf(int maxCapacity) {
            return HAS_UNSAFE ? PooledUnsafeHeapByteBuf.newUnsafeInstance(maxCapacity)
                    : PooledHeapByteBuf.newInstance(maxCapacity);
        }

        @Override
        protected void memoryCopy(byte[] src, int srcOffset, PooledByteBuf<byte[]> dst, int length) {
            if (length == 0) {
                return;
            }

            System.arraycopy(src, srcOffset, dst.memory, dst.offset, length);
        }
    }

    static final class DirectArena extends PoolArena<ByteBuffer> {

        DirectArena(PooledByteBufAllocator parent, SizeClasses sizeClass) {
            super(parent, sizeClass);
        }

        @Override
        boolean isDirect() {
            return true;
        }

        @Override
        protected PoolChunk<ByteBuffer> newChunk(int pageSize, int maxPageIdx, int pageShifts, int chunkSize) {
            // 1. 判断是否需要内存对齐
            if (sizeClass.directMemoryCacheAlignment == 0) {
                // 2. 不需要对齐时的简单处理
                ByteBuffer memory = allocateDirect(chunkSize);
                return new PoolChunk<ByteBuffer>(this, memory, memory, pageSize, pageShifts,
                        chunkSize, maxPageIdx);
            }

            // 3. 需要内存对齐时的处理
            final ByteBuffer base = allocateDirect(chunkSize + sizeClass.directMemoryCacheAlignment);
            final ByteBuffer memory = PlatformDependent.alignDirectBuffer(base, sizeClass.directMemoryCacheAlignment);
            return new PoolChunk<ByteBuffer>(this, base, memory, pageSize,
                    pageShifts, chunkSize, maxPageIdx);
        }

        @Override
        protected PoolChunk<ByteBuffer> newUnpooledChunk(int capacity) {
            if (sizeClass.directMemoryCacheAlignment == 0) {
                ByteBuffer memory = allocateDirect(capacity);
                return new PoolChunk<ByteBuffer>(this, memory, memory, capacity);
            }

            final ByteBuffer base = allocateDirect(capacity + sizeClass.directMemoryCacheAlignment);
            final ByteBuffer memory = PlatformDependent.alignDirectBuffer(base, sizeClass.directMemoryCacheAlignment);
            return new PoolChunk<ByteBuffer>(this, base, memory, capacity);
        }

        private static ByteBuffer allocateDirect(int capacity) {
            return PlatformDependent.useDirectBufferNoCleaner() ? PlatformDependent.allocateDirectNoCleaner(capacity)
                    : ByteBuffer.allocateDirect(capacity);
        }

        @Override
        protected void destroyChunk(PoolChunk<ByteBuffer> chunk) {
            if (PlatformDependent.useDirectBufferNoCleaner()) {
                PlatformDependent.freeDirectNoCleaner((ByteBuffer) chunk.base);
            } else {
                PlatformDependent.freeDirectBuffer((ByteBuffer) chunk.base);
            }
        }

        @Override
        protected PooledByteBuf<ByteBuffer> newByteBuf(int maxCapacity) {
            if (HAS_UNSAFE) {
                return PooledUnsafeDirectByteBuf.newInstance(maxCapacity);
            } else {
                return PooledDirectByteBuf.newInstance(maxCapacity);
            }
        }

        @Override
        protected void memoryCopy(ByteBuffer src, int srcOffset, PooledByteBuf<ByteBuffer> dstBuf, int length) {
            if (length == 0) {
                return;
            }

            if (HAS_UNSAFE) {
                PlatformDependent.copyMemory(
                        PlatformDependent.directBufferAddress(src) + srcOffset,
                        PlatformDependent.directBufferAddress(dstBuf.memory) + dstBuf.offset, length);
            } else {
                // We must duplicate the NIO buffers because they may be accessed by other Netty
                // buffers.
                src = src.duplicate();
                ByteBuffer dst = dstBuf.internalNioBuffer();
                src.position(srcOffset).limit(srcOffset + length);
                dst.position(dstBuf.offset);
                dst.put(src);
            }
        }
    }

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    @Override
    public int sizeIdx2size(int sizeIdx) {
        return sizeClass.sizeIdx2size(sizeIdx);
    }

    @Override
    public int sizeIdx2sizeCompute(int sizeIdx) {
        return sizeClass.sizeIdx2sizeCompute(sizeIdx);
    }

    @Override
    public long pageIdx2size(int pageIdx) {
        return sizeClass.pageIdx2size(pageIdx);
    }

    @Override
    public long pageIdx2sizeCompute(int pageIdx) {
        return sizeClass.pageIdx2sizeCompute(pageIdx);
    }

    @Override
    public int size2SizeIdx(int size) {
        return sizeClass.size2SizeIdx(size);
    }

    @Override
    public int pages2pageIdx(int pages) {
        return sizeClass.pages2pageIdx(pages);
    }

    @Override
    public int pages2pageIdxFloor(int pages) {
        return sizeClass.pages2pageIdxFloor(pages);
    }

    @Override
    public int normalizeSize(int size) {
        return sizeClass.normalizeSize(size);
    }
}
