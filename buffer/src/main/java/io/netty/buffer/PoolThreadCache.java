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

import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

import io.netty.buffer.PoolArena.SizeClass;
import io.netty.util.Recycler.EnhancedHandle;
import io.netty.util.internal.MathUtil;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.ObjectPool.Handle;
import io.netty.util.internal.ObjectPool.ObjectCreator;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 线程本地缓存，用于优化内存分配性能。此实现是Netty内存池系统的核心组件之一。
 * <p>
 * PoolThreadCache实现了基于线程的内存缓存机制，显著减少了多线程环境下内存分配的锁竞争，
 * 提高了内存分配和释放的吞吐量。该实现参考了jemalloc的设计理念和Facebook的可扩展内存分配技术。
 * </p>
 * 
 * <h3>设计目标</h3>
 * <ul>
 *   <li>减少线程间竞争：通过线程本地存储避免共享内存结构的锁争用</li>
 *   <li>提高内存复用：快速复用已释放的内存块，减少系统内存分配调用</li>
 *   <li>降低GC压力：通过内存池化减少对象创建和回收</li>
 *   <li>提高分配速度：避免频繁获取全局锁，加速内存分配路径</li>
 * </ul>
 * 
 * <h3>缓存层次结构</h3>
 * <p>
 * PoolThreadCache为每个线程维护两类内存缓存：
 * <ul>
 *   <li><b>小内存缓存</b>：用于缓存小于一个页面的内存块(通常≤4KB)</li>
 *   <li><b>普通内存缓存</b>：用于缓存大于一个页面的内存块(通常4KB-16MB)</li>
 * </ul>
 * 每种类型又分为堆内存(HeapArena)和直接内存(DirectArena)两个版本。
 * </p>
 * 
 * <h3>内存使用流程</h3>
 * <p>
 * 1. <b>分配</b>：尝试从线程缓存中获取合适大小的内存块，避免从共享池分配
 * <pre>
 *    分配请求 → 线程缓存查找 → 命中：直接返回
 *                         → 未命中：从PoolArena分配并加入缓存
 * </pre>
 * 
 * 2. <b>释放</b>：释放的内存不会立即返回系统，而是先进入线程缓存
 * <pre>
 *    释放请求 → 添加到线程缓存 → 缓存已满：触发清理
 *                           → 缓存未满：保留待复用
 * </pre>
 * </p>
 * 
 * <h3>缓存调整机制</h3>
 * <p>
 * 为防止内存在缓存中长期闲置，PoolThreadCache实现了定期清理机制：
 * <ul>
 *   <li>基于分配计数的清理：当分配次数达到阈值时触发</li>
 *   <li>定时清理：可配置的定时清理间隔</li>
 *   <li>手动清理：可由应用程序主动触发</li>
 * </ul>
 * </p>
 * 
 * <h3>与PoolArena的关系</h3>
 * <p>
 * 每个PoolThreadCache绑定到特定的堆内存Arena和直接内存Arena：
 * <ul>
 *   <li>Arena负责大块内存管理和全局策略</li>
 *   <li>ThreadCache作为线程与Arena间的桥梁，缓存小块内存</li>
 *   <li>多个线程的缓存通过负载均衡算法分配到不同Arena</li>
 * </ul>
 * </p>
 * 
 * <h3>内存统计</h3>
 * <p>
 * PoolThreadCache维护各种缓存的分配和命中统计信息，
 * 用于监控内存使用情况和性能分析。
 * </p>
 * 
 * @see PoolArena
 * @see PoolChunk
 * @see PoolSubpage
 * @see PooledByteBufAllocator
 */
final class PoolThreadCache {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PoolThreadCache.class);
    private static final int INTEGER_SIZE_MINUS_ONE = Integer.SIZE - 1;

    final PoolArena<byte[]> heapArena; // 堆内存区域
    final PoolArena<ByteBuffer> directArena; // 直接内存区域

    // Hold the caches for the different size classes, which are small and normal.
    private final MemoryRegionCache<byte[]>[] smallSubPageHeapCaches; // 小页堆内存缓存
    private final MemoryRegionCache<ByteBuffer>[] smallSubPageDirectCaches; // 小页直接内存缓存
    private final MemoryRegionCache<byte[]>[] normalHeapCaches; // 普通堆内存缓存
    private final MemoryRegionCache<ByteBuffer>[] normalDirectCaches; // 普通直接内存缓存

    private final int freeSweepAllocationThreshold;
    private final AtomicBoolean freed = new AtomicBoolean();
    @SuppressWarnings("unused") // Field is only here for the finalizer.
    private final FreeOnFinalize freeOnFinalize;

    private int allocations;

    // TODO: Test if adding padding helps under contention
    // private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /**
     * 创建一个新的线程本地缓存实例。
     * <p>
     * 此构造函数初始化线程与全局内存池的绑定关系，为线程创建专用的内存缓存结构。
     * 每个线程在首次分配内存时会创建一个PoolThreadCache实例，并与特定的堆内存Arena
     * 和直接内存Arena建立关联。此缓存系统是Netty内存池减少线程竞争的核心机制。
     * </p>
     * 
     * <h3>缓存结构初始化</h3>
     * <p>
     * 根据传入的参数创建四种类型的缓存数组：
     * <ul>
     *   <li>smallSubPageDirectCaches - 小型直接内存缓存</li>
     *   <li>normalDirectCaches - 普通直接内存缓存</li>
     *   <li>smallSubPageHeapCaches - 小型堆内存缓存</li>
     *   <li>normalHeapCaches - 普通堆内存缓存</li>
     * </ul>
     * 每种缓存的大小和数量由构造参数控制。
     * </p>
     * 
     * <h3>Arena关联</h3>
     * <p>
     * 构造时会将缓存与指定的Arena建立关联，并递增Arena的线程缓存计数器，
     * 这用于后续的负载均衡决策。如果没有提供对应的Arena，相关缓存会被设为null。
     * </p>
     *
     * @param heapArena 堆内存Arena，可以为null表示不使用堆内存缓存
     * @param directArena 直接内存Arena，可以为null表示不使用直接内存缓存
     * @param smallCacheSize 小型缓存的容量，控制每个小型缓存队列的长度
     * @param normalCacheSize 普通缓存的容量，控制每个普通缓存队列的长度
     * @param maxCachedBufferCapacity 可缓存的最大缓冲区容量，超过此大小的内存不会被缓存
     * @param freeSweepAllocationThreshold 触发缓存清理的分配次数阈值
     * @param useFinalizer 是否使用终结器进行缓存清理，针对非FastThreadLocal线程
     * 
     * @throws IllegalArgumentException 如果maxCachedBufferCapacity为负数，或在使用缓存的情况下freeSweepAllocationThreshold小于1
     */
    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena,
            int smallCacheSize, int normalCacheSize, int maxCachedBufferCapacity,
            int freeSweepAllocationThreshold, boolean useFinalizer) {
        // 检查参数有效性：最大可缓存容量必须非负
        checkPositiveOrZero(maxCachedBufferCapacity, "maxCachedBufferCapacity");
        
        // 初始化清理阈值：当分配次数达到此值时会触发缓存清理
        this.freeSweepAllocationThreshold = freeSweepAllocationThreshold;
        
        // 保存Arena引用，建立线程缓存与全局Arena的关联
        this.heapArena = heapArena;
        this.directArena = directArena;
        
        // 初始化直接内存相关缓存
        if (directArena != null) {
            // 创建小型直接内存子页缓存数组，数量与Arena的子页数量一致
            smallSubPageDirectCaches = createSubPageCaches(smallCacheSize, directArena.sizeClass.nSubpages);
            
            // 创建普通直接内存缓存数组，基于最大缓存容量和Arena配置
            normalDirectCaches = createNormalCaches(normalCacheSize, maxCachedBufferCapacity, directArena);
            
            // 增加Arena的线程缓存计数，用于负载均衡
            directArena.numThreadCaches.getAndIncrement();
        } else {
            // 没有配置直接内存Arena，相关缓存设为null
            smallSubPageDirectCaches = null;
            normalDirectCaches = null;
        }
        
        // 初始化堆内存相关缓存，逻辑与直接内存类似
        if (heapArena != null) {
            // 创建小型堆内存子页缓存数组
            smallSubPageHeapCaches = createSubPageCaches(smallCacheSize, heapArena.sizeClass.nSubpages);
            
            // 创建普通堆内存缓存数组
            normalHeapCaches = createNormalCaches(normalCacheSize, maxCachedBufferCapacity, heapArena);
            
            // 增加Arena的线程缓存计数
            heapArena.numThreadCaches.getAndIncrement();
        } else {
            // 没有配置堆内存Arena，相关缓存设为null
            smallSubPageHeapCaches = null;
            normalHeapCaches = null;
        }

        // 参数有效性检查：如果使用了任何缓存，freeSweepAllocationThreshold必须大于0
        if ((smallSubPageDirectCaches != null || normalDirectCaches != null
                || smallSubPageHeapCaches != null || normalHeapCaches != null)
                && freeSweepAllocationThreshold < 1) {
            throw new IllegalArgumentException("freeSweepAllocationThreshold: "
                    + freeSweepAllocationThreshold + " (expected: > 0)");
        }
        
        // 根据需要创建终结器，用于非FastThreadLocal线程的缓存回收
        freeOnFinalize = useFinalizer ? new FreeOnFinalize(this) : null;
    }

    /**
     * 创建子页缓存数组
     * 
     * @param cacheSize 每个缓存的大小
     * @param numCaches 缓存数量
     * @return 缓存数组
     */
    private static <T> MemoryRegionCache<T>[] createSubPageCaches(
            int cacheSize, int numCaches) {
        if (cacheSize > 0 && numCaches > 0) {
            @SuppressWarnings("unchecked")
            MemoryRegionCache<T>[] cache = new MemoryRegionCache[numCaches];
            for (int i = 0; i < cache.length; i++) {
                cache[i] = new SubPageMemoryRegionCache<T>(cacheSize);
            }
            return cache;
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> MemoryRegionCache<T>[] createNormalCaches(
            int cacheSize, int maxCachedBufferCapacity, PoolArena<T> area) {
        if (cacheSize > 0 && maxCachedBufferCapacity > 0) {
            int max = Math.min(area.sizeClass.chunkSize, maxCachedBufferCapacity);
            // Create as many normal caches as we support based on how many sizeIdx we have
            // and what the upper
            // bound is that we want to cache in general.
            List<MemoryRegionCache<T>> cache = new ArrayList<MemoryRegionCache<T>>();
            for (int idx = area.sizeClass.nSubpages; idx < area.sizeClass.nSizes &&
                    area.sizeClass.sizeIdx2size(idx) <= max; idx++) {
                cache.add(new NormalMemoryRegionCache<T>(cacheSize));
            }
            return cache.toArray(new MemoryRegionCache[0]);
        } else {
            return null;
        }
    }

    // val > 0
    static int log2(int val) {
        return INTEGER_SIZE_MINUS_ONE - Integer.numberOfLeadingZeros(val);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if
     * successful {@code false} otherwise
     */
    boolean allocateSmall(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int sizeIdx) {
        return allocate(cacheForSmall(area, sizeIdx), buf, reqCapacity);
    }

    /**
     * Try to allocate a normal buffer out of the cache. Returns {@code true} if
     * successful {@code false} otherwise
     */
    boolean allocateNormal(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int sizeIdx) {
        return allocate(cacheForNormal(area, sizeIdx), buf, reqCapacity);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private boolean allocate(MemoryRegionCache<?> cache, PooledByteBuf buf, int reqCapacity) {
        if (cache == null) {
            // no cache found so just return false here
            return false;
        }
        boolean allocated = cache.allocate(buf, reqCapacity, this);
        if (++allocations >= freeSweepAllocationThreshold) {
            allocations = 0;
            trim();
        }
        return allocated;
    }

    /**
     * Add {@link PoolChunk} and {@code handle} to the cache if there is enough
     * room.
     * Returns {@code true} if it fit into the cache {@code false} otherwise.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    boolean add(PoolArena<?> area, PoolChunk chunk, ByteBuffer nioBuffer,
            long handle, int normCapacity, SizeClass sizeClass) {
        int sizeIdx = area.sizeClass.size2SizeIdx(normCapacity);
        MemoryRegionCache<?> cache = cache(area, sizeIdx, sizeClass);
        if (cache == null) {
            return false;
        }
        if (freed.get()) {
            return false;
        }
        return cache.add(chunk, nioBuffer, handle, normCapacity);
    }

    private MemoryRegionCache<?> cache(PoolArena<?> area, int sizeIdx, SizeClass sizeClass) {
        switch (sizeClass) {
            case Normal:
                return cacheForNormal(area, sizeIdx);
            case Small:
                return cacheForSmall(area, sizeIdx);
            default:
                throw new Error();
        }
    }

    /**
     * Should be called if the Thread that uses this cache is about to exit to
     * release resources out of the cache
     */
    void free(boolean finalizer) {
        // As free() may be called either by the finalizer or by
        // FastThreadLocal.onRemoval(...) we need to ensure
        // we only call this one time.
        if (freed.compareAndSet(false, true)) {
            if (freeOnFinalize != null) {
                // Help GC: this can race with a finalizer thread, but will be null out
                // regardless
                freeOnFinalize.cache = null;
            }
            int numFreed = free(smallSubPageDirectCaches, finalizer) +
                    free(normalDirectCaches, finalizer) +
                    free(smallSubPageHeapCaches, finalizer) +
                    free(normalHeapCaches, finalizer);

            if (numFreed > 0 && logger.isDebugEnabled()) {
                logger.debug("Freed {} thread-local buffer(s) from thread: {}", numFreed,
                        Thread.currentThread().getName());
            }

            if (directArena != null) {
                directArena.numThreadCaches.getAndDecrement();
            }

            if (heapArena != null) {
                heapArena.numThreadCaches.getAndDecrement();
            }
        }
    }

    private static int free(MemoryRegionCache<?>[] caches, boolean finalizer) {
        if (caches == null) {
            return 0;
        }

        int numFreed = 0;
        for (MemoryRegionCache<?> c : caches) {
            numFreed += free(c, finalizer);
        }
        return numFreed;
    }

    private static int free(MemoryRegionCache<?> cache, boolean finalizer) {
        if (cache == null) {
            return 0;
        }
        return cache.free(finalizer);
    }

    void trim() {
        trim(smallSubPageDirectCaches);
        trim(normalDirectCaches);
        trim(smallSubPageHeapCaches);
        trim(normalHeapCaches);
    }

    private static void trim(MemoryRegionCache<?>[] caches) {
        if (caches == null) {
            return;
        }
        for (MemoryRegionCache<?> c : caches) {
            trim(c);
        }
    }

    private static void trim(MemoryRegionCache<?> cache) {
        if (cache == null) {
            return;
        }
        cache.trim();
    }

    private MemoryRegionCache<?> cacheForSmall(PoolArena<?> area, int sizeIdx) {
        if (area.isDirect()) {
            return cache(smallSubPageDirectCaches, sizeIdx);
        }
        return cache(smallSubPageHeapCaches, sizeIdx);
    }

    private MemoryRegionCache<?> cacheForNormal(PoolArena<?> area, int sizeIdx) {
        // We need to subtract area.sizeClass.nSubpages as sizeIdx is the overall index
        // for all sizes.
        int idx = sizeIdx - area.sizeClass.nSubpages;
        if (area.isDirect()) {
            return cache(normalDirectCaches, idx);
        }
        return cache(normalHeapCaches, idx);
    }

    private static <T> MemoryRegionCache<T> cache(MemoryRegionCache<T>[] cache, int sizeIdx) {
        if (cache == null || sizeIdx > cache.length - 1) {
            return null;
        }
        return cache[sizeIdx];
    }

    /**
     * 内存区域缓存的抽象基类，是线程本地缓存系统的核心组件。
     * <p>
     * MemoryRegionCache实现了基于队列的高效内存缓存机制，用于存储和复用已释放的内存块，
     * 显著降低内存分配开销和GC压力。作为抽象类，它定义了内存缓存的基本框架，同时允许
     * 子类针对不同类型的内存块（如子页和普通页）实现特定的初始化逻辑。
     * </p>
     * 
     * <h3>核心设计</h3>
     * <p>
     * 该缓存是线程本地的，避免了线程间竞争，具有以下特点：
     * <ul>
     *   <li>基于固定大小的MPSC队列，适合内存缓存场景</li>
     *   <li>支持两级缓存策略：添加到缓存和从缓存分配</li>
     *   <li>包含自适应清理机制，防止内存长期闲置</li>
     *   <li>使用对象池技术减少辅助对象（Entry）的创建开销</li>
     * </ul>
     * </p>
     * 
     * <h3>关键属性</h3>
     * <ul>
     *   <li><b>size</b> - 缓存队列的容量，总是2的幂次</li>
     *   <li><b>queue</b> - 存储内存条目的MPSC队列，高效支持多生产者单消费者模式</li>
     *   <li><b>sizeClass</b> - 内存大小类别，用于区分小内存块和普通内存块</li>
     *   <li><b>allocations</b> - 自上次缓存整理以来的分配计数，用于自适应清理</li>
     * </ul>
     * 
     * <h3>内存缓存流程</h3>
     * <p>
     * <ol>
     *   <li><b>缓存添加(add)</b>：当内存被释放时，尝试加入缓存队列</li>
     *   <li><b>缓存分配(allocate)</b>：需要内存时，优先从缓存队列获取</li>
     *   <li><b>缓存清理(free)</b>：根据策略释放缓存中的内存条目</li>
     *   <li><b>缓存整理(trim)</b>：基于使用频率自适应调整缓存大小</li>
     * </ol>
     * </p>
     * 
     * <h3>性能优化</h3>
     * <p>
     * <ul>
     *   <li>使用对象池RECYCLER回收Entry对象，减少GC压力</li>
     *   <li>采用MPSC队列，在多线程添加时无需互斥锁</li>
     *   <li>通过allocations计数实现自适应缓存整理</li>
     *   <li>支持快速路径和无保护模式，提高性能关键路径的效率</li>
     * </ul>
     * </p>
     * 
     * <h3>Entry内部类</h3>
     * <p>
     * 每个Entry封装了一个可重用的内存块的引用和元数据：
     * <ul>
     *   <li>chunk - 内存块引用，指向实际内存数据</li>
     *   <li>nioBuffer - 直接内存缓冲区，用于堆外内存</li>
     *   <li>handle - 内存句柄，定位内存块中的特定区域</li>
     *   <li>normCapacity - 标准化容量，与内存池的规格匹配</li>
     *   <li>recyclerHandle - 对象池句柄，用于Entry对象的回收</li>
     * </ul>
     * </p>
     * 
     * @param <T> 内存类型参数，可以是byte[]（堆内存）或ByteBuffer（直接内存）
     * 
     * @see SubPageMemoryRegionCache
     * @see NormalMemoryRegionCache
     * @see PoolChunk
     */
    private abstract static class MemoryRegionCache<T> {
        /**
         * 缓存队列的容量大小，总是2的幂次值
         * <p>
         * 此值决定了每个线程可缓存的特定大小内存块的最大数量。
         * 较大的值可提高缓存命中率，但会增加内存占用。
         * </p>
         */
        private final int size;
        
        /**
         * 缓存队列，存储可重用的内存块引用
         * <p>
         * 使用MPSC(多生产者单消费者)队列实现，支持在无锁环境下安全操作。
         * 队列大小固定为size，先进先出策略。
         * </p>
         */
        private final Queue<Entry<T>> queue;
        
        /**
         * 内存大小类别
         * <p>
         * 标识此缓存管理的是小内存块(Small)还是普通内存块(Normal)，
         * 不同类型的内存使用不同的初始化和释放策略。
         * </p>
         */
        private final SizeClass sizeClass;
        
        /**
         * 自上次缓存整理以来的分配计数
         * <p>
         * 用于跟踪缓存使用频率，实现自适应缓存清理。
         * 当缓存分配次数增加时，表明缓存被频繁使用；
         * 分配较少时，表明部分缓存项可能闲置，可以释放。
         * </p>
         */
        private int allocations;

        /**
         * 创建内存区域缓存
         * <p>
         * 初始化固定大小的缓存队列，用于存储特定大小类别的内存块引用。
         * 缓存大小会被调整为最近的2的幂次值，以优化内存使用。
         * </p>
         * 
         * @param size 缓存的目标容量，将被调整为2的幂次值
         * @param sizeClass 内存大小类别，Small或Normal
         */
        MemoryRegionCache(int size, SizeClass sizeClass) {
            this.size = MathUtil.safeFindNextPositivePowerOfTwo(size);
            queue = PlatformDependent.newFixedMpscUnpaddedQueue(this.size);
            this.sizeClass = sizeClass;
        }

        /**
         * 使用缓存的内存块初始化ByteBuf
         * <p>
         * 抽象方法，由子类实现，定义如何使用缓存的内存块初始化ByteBuf对象。
         * 不同类型的内存块(如子页和普通页)有不同的初始化逻辑。
         * </p>
         * 
         * @param chunk 内存块，包含实际内存数据
         * @param nioBuffer 可能的NIO ByteBuffer，用于直接内存
         * @param handle 定位内存块中特定部分的句柄
         * @param buf 待初始化的ByteBuf对象
         * @param reqCapacity 请求的容量大小
         * @param threadCache 线程本地缓存，用于后续操作
         */
        protected abstract void initBuf(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle,
                PooledByteBuf<T> buf, int reqCapacity, PoolThreadCache threadCache);

        /**
         * 将内存块添加到缓存
         * <p>
         * 当内存被释放时，此方法尝试将内存块引用添加到缓存队列中以备后用。
         * 如果缓存队列已满，则立即回收Entry对象而不缓存内存引用。
         * </p>
         * 
         * @param chunk 内存块
         * @param nioBuffer NIO ByteBuffer，用于直接内存
         * @param handle 内存句柄
         * @param normCapacity 标准化的内存容量
         * @return 是否成功添加到缓存队列
         */
        @SuppressWarnings("unchecked")
        public final boolean add(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, int normCapacity) {
            Entry<T> entry = newEntry(chunk, nioBuffer, handle, normCapacity);
            boolean queued = queue.offer(entry);
            if (!queued) {
                // 如果队列已满，立即回收Entry对象
                entry.unguardedRecycle();
            }

            return queued;
        }

        /**
         * 尝试从缓存分配内存
         * <p>
         * 当需要分配内存时，此方法尝试从缓存队列中获取预先缓存的内存块。
         * 如果缓存命中，使用缓存的内存初始化ByteBuf并增加分配计数。
         * </p>
         * 
         * @param buf 待分配内存的ByteBuf
         * @param reqCapacity 请求的容量大小
         * @param threadCache 线程本地缓存
         * @return 是否成功从缓存分配内存
         */
        public final boolean allocate(PooledByteBuf<T> buf, int reqCapacity, PoolThreadCache threadCache) {
            Entry<T> entry = queue.poll();
            if (entry == null) {
                return false;  // 缓存未命中
            }
            initBuf(entry.chunk, entry.nioBuffer, entry.handle, buf, reqCapacity, threadCache);
            entry.unguardedRecycle();  // 回收Entry对象，但内存已被复用

            // 增加分配计数，用于自适应缓存整理
            // 此操作非线程安全，但在线程本地缓存环境中这是安全的
            ++allocations;
            return true;
        }

        /**
         * 清空缓存，释放所有缓存的内存块
         * <p>
         * 将缓存队列中的所有内存项返还给全局内存池，可用于主动释放资源
         * 或在线程退出时清理资源。
         * </p>
         * 
         * @param finalizer 是否由终结器调用，影响Entry对象的回收策略
         * @return 释放的内存项数量
         */
        public final int free(boolean finalizer) {
            return free(Integer.MAX_VALUE, finalizer);
        }

        /**
         * 释放有限数量的缓存项
         * <p>
         * 从缓存队列中移除并释放指定数量的内存项，通常用于缓存整理。
         * </p>
         * 
         * @param max 最大释放数量
         * @param finalizer 是否由终结器调用
         * @return 实际释放的内存项数量
         */
        private int free(int max, boolean finalizer) {
            int numFreed = 0;
            for (; numFreed < max; numFreed++) {
                Entry<T> entry = queue.poll();
                if (entry != null) {
                    freeEntry(entry, finalizer);
                } else {
                    // 队列已清空
                    return numFreed;
                }
            }
            return numFreed;
        }

        /**
         * 根据使用频率整理缓存
         * <p>
         * 自适应缓存整理算法，根据分配计数判断缓存使用频率，
         * 释放可能闲置的缓存内存项。这有助于防止缓存中的内存长期未使用。
         * </p>
         */
        public final void trim() {
            int free = size - allocations;  // 计算未被充分利用的项数
            allocations = 0;  // 重置分配计数

            // 如果有项未被充分利用，则释放这些项
            if (free > 0) {
                free(free, false);
            }
        }

        /**
         * 释放单个缓存条目
         * <p>
         * 将内存引用返还给全局内存池，并可选择性地回收Entry对象。
         * </p>
         * 
         * @param entry 要释放的缓存条目
         * @param finalizer 是否由终结器调用
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private void freeEntry(Entry entry, boolean finalizer) {
            // 在回收Entry对象前捕获状态
            PoolChunk chunk = entry.chunk;
            long handle = entry.handle;
            ByteBuffer nioBuffer = entry.nioBuffer;
            int normCapacity = entry.normCapacity;

            if (!finalizer) {
                // 如果不是由终结器调用，立即回收Entry对象
                // 这允许PoolChunk被垃圾回收
                entry.recycle();
            }

            // 将内存返还给Arena
            chunk.arena.freeChunk(chunk, handle, normCapacity, sizeClass, nioBuffer, finalizer);
        }

        /**
         * 缓存条目类，封装可重用内存块的引用和元数据
         * <p>
         * 每个Entry对象代表缓存中的一个内存块，包含所有必要的信息
         * 以重用该内存块或将其返还给全局内存池。
         * 设计为可回收对象，通过对象池减少GC压力。
         * </p>
         */
        static final class Entry<T> {
            /**
             * 对象池句柄，用于Entry对象的回收
             */
            final EnhancedHandle<Entry<?>> recyclerHandle;
            
            /**
             * 内存块引用，指向实际内存数据
             */
            PoolChunk<T> chunk;
            
            /**
             * 与内存关联的NIO缓冲区，用于直接内存操作
             */
            ByteBuffer nioBuffer;
            
            /**
             * 内存句柄，定位内存块中的特定区域
             */
            long handle = -1;
            
            /**
             * 标准化容量，与内存池的规格匹配
             */
            int normCapacity;

            /**
             * 创建Entry对象
             * 
             * @param recyclerHandle 对象池句柄
             */
            Entry(Handle<Entry<?>> recyclerHandle) {
                this.recyclerHandle = (EnhancedHandle<Entry<?>>) recyclerHandle;
            }

            /**
             * 回收Entry对象到对象池
             * <p>
             * 清空所有引用后将对象返回对象池，启用安全检查
             * </p>
             */
            void recycle() {
                chunk = null;
                nioBuffer = null;
                handle = -1;
                recyclerHandle.recycle(this);
            }

            /**
             * 无保护模式回收Entry对象
             * <p>
             * 绕过安全检查，直接回收对象，用于性能关键路径
             * </p>
             */
            void unguardedRecycle() {
                chunk = null;
                nioBuffer = null;
                handle = -1;
                recyclerHandle.unguardedRecycle(this);
            }
        }

        /**
         * 创建并初始化新的Entry对象
         * <p>
         * 从对象池获取Entry对象并初始化，避免创建新对象
         * </p>
         * 
         * @param chunk 内存块
         * @param nioBuffer NIO缓冲区
         * @param handle 内存句柄
         * @param normCapacity 标准化容量
         * @return 初始化后的Entry对象
         */
        @SuppressWarnings("rawtypes")
        private static Entry newEntry(PoolChunk<?> chunk, ByteBuffer nioBuffer, long handle, int normCapacity) {
            Entry entry = RECYCLER.get();
            entry.chunk = chunk;
            entry.nioBuffer = nioBuffer;
            entry.handle = handle;
            entry.normCapacity = normCapacity;
            return entry;
        }

        /**
         * Entry对象池，用于减少GC压力
         */
        @SuppressWarnings("rawtypes")
        private static final ObjectPool<Entry> RECYCLER = ObjectPool.newPool(new ObjectCreator<Entry>() {
            @SuppressWarnings("unchecked")
            @Override
            public Entry newObject(Handle<Entry> handle) {
                return new Entry(handle);
            }
        });
    }

    private static final class FreeOnFinalize {

        private volatile PoolThreadCache cache;

        private FreeOnFinalize(PoolThreadCache cache) {
            this.cache = cache;
        }

        /// TODO: In the future when we move to Java9+ we should use
        /// java.lang.ref.Cleaner.
        @SuppressWarnings({ "FinalizeDeclaration", "deprecation" })
        @Override
        protected void finalize() throws Throwable {
            try {
                super.finalize();
            } finally {
                PoolThreadCache cache = this.cache;
                // this can race with a non-finalizer thread calling free: regardless who wins,
                // the cache will be
                // null out
                this.cache = null;
                if (cache != null) {
                    cache.free(true);
                }
            }
        }
    }
}
