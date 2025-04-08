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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThreadExecutorMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Netty的池化ByteBuf分配器实现，通过内存池管理机制提高内存分配效率，减少GC压力。
 * <p>
 * PooledByteBufAllocator是Netty默认的内存分配器，实现了高效的内存池管理，
 * 包含堆内存和直接内存两种类型的内存池，以及支持多级缓存的线程本地缓存机制。
 * </p>
 * 
 * <h3>内存池化架构</h3>
 * 内存分配的层次结构：
 * 
 * <pre>
 * PooledByteBufAllocator
 *  ├── PoolArena[] (heapArenas/directArenas)
 *  │    ├── PoolChunkList (qInit/q000/q025/q050/q075/q100)
 *  │    │    └── PoolChunk
 *  │    │         └── memory (byte[]/ByteBuffer)
 *  │    └── PoolSubpage
 *  └── PoolThreadCache (threadCache)
 *       ├── MemoryRegionCache[] (tiny缓存)
 *       ├── MemoryRegionCache[] (small缓存)
 *       └── MemoryRegionCache[] (normal缓存)
 * </pre>
 * 
 * <h3>关键组件</h3>
 * <ul>
 * <li><b>PoolArena</b>: 内存区域管理单元，每个线程使用一个特定的Arena以减少竞争</li>
 * <li><b>PoolChunk</b>: 大块内存块，默认16MB，使用伙伴算法进行内存分配</li>
 * <li><b>PoolSubpage</b>: 管理小于一个页(默认8KB)的内存分配，使用位图追踪</li>
 * <li><b>PoolThreadCache</b>: 线程本地缓存，提高分配/释放性能，减少跨线程同步</li>
 * </ul>
 * 
 * <h3>内存大小分类</h3>
 * <ul>
 * <li><b>Tiny</b>: 小于512字节的内存块</li>
 * <li><b>Small</b>: 介于512字节和一个页大小(8KB)之间的内存块</li>
 * <li><b>Normal</b>: 介于一个页大小和chunk最大大小(16MB)之间的内存块</li>
 * <li><b>Huge</b>: 大于一个chunk(16MB)的内存块，不进行池化管理</li>
 * </ul>
 * 
 * <h3>性能优化策略</h3>
 * <ul>
 * <li>多Arena设计：减少线程间竞争</li>
 * <li>伙伴分配算法：高效管理大块内存</li>
 * <li>位图追踪：高效管理小块内存</li>
 * <li>线程本地缓存：加速内存分配和回收</li>
 * <li>内存复用：减少实际分配和释放操作</li>
 * </ul>
 * 
 * @see PoolArena
 * @see PoolChunk
 * @see PoolSubpage
 * @see PoolThreadCache
 * @see ByteBufAllocator
 */
public class PooledByteBufAllocator extends AbstractByteBufAllocator implements ByteBufAllocatorMetricProvider {

    // 日志记录器
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PooledByteBufAllocator.class);

    // 堆内存和直接内存arena数量的默认值
    private static final int DEFAULT_NUM_HEAP_ARENA; // 堆内存arena默认数量
    private static final int DEFAULT_NUM_DIRECT_ARENA; // 直接内存arena默认数量

    // 内存分配的关键参数
    private static final int DEFAULT_PAGE_SIZE; // 默认页大小
    private static final int DEFAULT_MAX_ORDER; // 默认最大order,决定chunk大小(8192 << 9 = 4 MB/chunk)
    private static final int DEFAULT_SMALL_CACHE_SIZE; // 小缓存大小
    private static final int DEFAULT_NORMAL_CACHE_SIZE; // 普通缓存大小
    static final int DEFAULT_MAX_CACHED_BUFFER_CAPACITY; // 最大可缓存的buffer容量

    // 缓存维护相关参数
    private static final int DEFAULT_CACHE_TRIM_INTERVAL; // 缓存清理间隔
    private static final long DEFAULT_CACHE_TRIM_INTERVAL_MILLIS; // 缓存清理间隔(毫秒)
    private static final boolean DEFAULT_USE_CACHE_FOR_ALL_THREADS; // 是否为所有线程启用缓存

    // 内存对齐相关参数
    private static final int DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT; // 直接内存缓存对齐
    static final int DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK; // 每个chunk最大缓存的ByteBuf数量

    // 快速线程本地变量的终结器缓存禁用标志
    private static final boolean DEFAULT_DISABLE_CACHE_FINALIZERS_FOR_FAST_THREAD_LOCAL_THREADS;

    // 关键常量
    private static final int MIN_PAGE_SIZE = 4096; // 最小页大小(4KB)
    private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2); // 最大chunk大小
    private static final int CACHE_NOT_USED = 0; // 缓存未使用标记

    // 定义一个线程缓存清理任务
    private final Runnable trimTask = new Runnable() {
        @Override
        public void run() {
            // 调用当前分配器的线程缓存清理方法
            PooledByteBufAllocator.this.trimCurrentThreadCache();
        }
    };

    /**
     * 静态初始化块，负责初始化PooledByteBufAllocator的各项默认配置参数。
     * <p>
     * 此块完成以下初始化工作：
     * <ol>
     * <li>计算内存页大小和对齐方式</li>
     * <li>确定最大order值(影响chunk大小)</li>
     * <li>计算堆内存和直接内存Arena的默认数量</li>
     * <li>设置各级缓存的大小和阈值</li>
     * <li>配置缓存清理策略</li>
     * </ol>
     * </p>
     */
    static {
        // 步骤1: 初始化内存对齐参数
        // 从系统属性读取直接内存对齐值，默认为0表示不进行特殊对齐
        int defaultAlignment = SystemPropertyUtil.getInt(
                "io.netty.allocator.directMemoryCacheAlignment", 0);
        // 从系统属性读取页大小，默认为8KB(8192字节)
        int defaultPageSize = SystemPropertyUtil.getInt("io.netty.allocator.pageSize", 8192);
        Throwable pageSizeFallbackCause = null;
        try {
            // 验证页大小和对齐方式是否有效
            validateAndCalculatePageShifts(defaultPageSize, defaultAlignment);
        } catch (Throwable t) {
            // 如果参数无效，记录异常并回退到默认值
            pageSizeFallbackCause = t;
            defaultPageSize = 8192; // 回退到8KB的标准页大小
            defaultAlignment = 0; // 取消内存对齐要求
        }
        // 设置页大小和内存对齐常量
        DEFAULT_PAGE_SIZE = defaultPageSize;
        DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT = defaultAlignment;

        // 步骤2: 初始化最大order值
        // 从系统属性读取最大order值，默认为9，表示chunk大小为pageSize<<9 (8KB*512=4MB)
        int defaultMaxOrder = SystemPropertyUtil.getInt("io.netty.allocator.maxOrder", 9);
        Throwable maxOrderFallbackCause = null;
        try {
            // 验证maxOrder是否会导致有效的chunk大小
            validateAndCalculateChunkSize(DEFAULT_PAGE_SIZE, defaultMaxOrder);
        } catch (Throwable t) {
            // 如果参数无效，记录异常并回退到默认值9
            maxOrderFallbackCause = t;
            defaultMaxOrder = 9;
        }
        DEFAULT_MAX_ORDER = defaultMaxOrder;

        // 步骤3: 计算堆内存和直接内存Arena的默认数量
        // 目标：确保内存池不会占用过多系统内存
        // 假设每个Arena有3个chunks，池不应该消耗超过总可用内存的50%
        final Runtime runtime = Runtime.getRuntime();

        /*
         * 默认使用处理器数量的2倍作为Arena数量，这是为了减少竞争。
         * 这与NIO和EPOLL使用的EventLoop数量策略一致(通常是处理器数的2倍)。
         * 如果选择更小的数量，可能导致Arena上的分配和释放操作出现热点竞争。
         *
         * 详见：https://github.com/netty/netty/issues/3888
         */
        // 计算默认Arena数量：处理器数量的2倍
        final int defaultMinNumArena = NettyRuntime.availableProcessors() * 2;
        // 计算默认chunk大小 = 页大小 * 2^最大order
        final int defaultChunkSize = DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER;

        // 计算堆Arena数量：取配置值与自动计算值的较小者
        // 自动计算逻辑确保Arena总内存不超过JVM最大内存的50%(考虑每个Arena包含3个chunks)
        // 计算堆Arena数量：取配置值与自动计算值的较小者
        // 自动计算逻辑确保Arena总内存不超过JVM最大内存的50%(考虑每个Arena包含3个chunks)
        // 公式解析：runtime.maxMemory() / defaultChunkSize / 2 / 3
        // - /defaultChunkSize：计算JVM最大内存可容纳的chunk总数
        // - /2：限制内存池最多占用系统内存的50%，剩余空间留给非池化分配和JVM其他用途
        // - /3：假设每个Arena管理3个chunk，计算所需的Arena数量
        // 这样设计确保了内存使用、Arena负载和并发性能三者的平衡
        DEFAULT_NUM_HEAP_ARENA = Math.max(0,
                SystemPropertyUtil.getInt(
                        "io.netty.allocator.numHeapArenas",
                        (int) Math.min(
                                defaultMinNumArena,
                                runtime.maxMemory() / defaultChunkSize / 2 / 3)));

        // 计算直接内存Arena数量：类似逻辑，但基于最大直接内存而非JVM堆内存
        DEFAULT_NUM_DIRECT_ARENA = Math.max(0,
                SystemPropertyUtil.getInt(
                        "io.netty.allocator.numDirectArenas",
                        (int) Math.min(
                                defaultMinNumArena,
                                PlatformDependent.maxDirectMemory() / defaultChunkSize / 2 / 3)));

        // 步骤4: 配置缓存大小参数
        // 设置小型缓存大小，用于small类别的内存块
        DEFAULT_SMALL_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.smallCacheSize", 256);
        // 设置普通缓存大小，用于normal类别的内存块
        DEFAULT_NORMAL_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.normalCacheSize", 64);

        // 设置可缓存的最大缓冲区容量，默认32KB
        // 这个设计参考了'Scalable memory allocation using jemalloc'论文的建议
        DEFAULT_MAX_CACHED_BUFFER_CAPACITY = SystemPropertyUtil.getInt(
                "io.netty.allocator.maxCachedBufferCapacity", 32 * 1024);

        // 设置缓存清理阈值：达到这个分配次数后会检查并清理不常用的缓存条目
        DEFAULT_CACHE_TRIM_INTERVAL = SystemPropertyUtil.getInt(
                "io.netty.allocator.cacheTrimInterval", 8192);

        // 步骤5: 配置缓存清理时间间隔
        // 处理兼容性：检查旧配置项是否存在
        if (SystemPropertyUtil.contains("io.netty.allocation.cacheTrimIntervalMillis")) {
            // 警告用户使用了过时的配置名称
            logger.warn("-Dio.netty.allocation.cacheTrimIntervalMillis is deprecated," +
                    " use -Dio.netty.allocator.cacheTrimIntervalMillis");

            // 如果新旧配置都存在，优先使用新配置
            if (SystemPropertyUtil.contains("io.netty.allocator.cacheTrimIntervalMillis")) {
                // 使用新的配置名称
                DEFAULT_CACHE_TRIM_INTERVAL_MILLIS = SystemPropertyUtil.getLong(
                        "io.netty.allocator.cacheTrimIntervalMillis", 0);
            } else {
                // 使用旧的配置名称
                DEFAULT_CACHE_TRIM_INTERVAL_MILLIS = SystemPropertyUtil.getLong(
                        "io.netty.allocation.cacheTrimIntervalMillis", 0);
            }
        } else {
            // 只使用新的配置名称
            DEFAULT_CACHE_TRIM_INTERVAL_MILLIS = SystemPropertyUtil.getLong(
                    "io.netty.allocator.cacheTrimIntervalMillis", 0);
        }

        // 步骤6: 其他配置选项
        // 决定哪些线程可以使用PoolThreadCache线程本地缓存
        // 是否为所有线程启用缓存，默认只为Netty的I/O线程启用
        // 业务流程影响：
        // - 当应用程序线程调用allocate()方法时，决定是否为该线程创建PoolThreadCache
        // - I/O线程（如EventLoop线程）总是获得缓存，以优化网络处理性能
        // - 应用业务线程默认不使用缓存，除非显式配置
        DEFAULT_USE_CACHE_FOR_ALL_THREADS = SystemPropertyUtil.getBoolean(
                "io.netty.allocator.useCacheForAllThreads", false);

        // 是否为FastThreadLocal线程禁用缓存终结器
        // 业务流程影响：
        // - 当FastThreadLocal线程调用release()方法时，决定是否禁用缓存终结器
        // - I/O线程（如EventLoop线程）总是获得缓存，以优化网络处理性能
        // - 应用业务线程默认不使用缓存，除非显式配置
        DEFAULT_DISABLE_CACHE_FINALIZERS_FOR_FAST_THREAD_LOCAL_THREADS = SystemPropertyUtil.getBoolean(
                "io.netty.allocator.disableCacheFinalizersForFastThreadLocalThreads", false);
        // 设置每个chunk最大缓存的ByteBuffer数量
        // 使用1023而非1024是因为使用ArrayDeque作为存储，它会分配内部数组为1024
        // 如果设置为1024，实际会分配2048大小的数组，浪费空间
        // 业务流程影响：
        // - 当Buffer被释放时，它可能被放入线程本地缓存以便重用
        // - 此值限制每个chunk可以在缓存中保留多少个释放的buffer
        // - 防止缓存无限增长，造成内存泄漏
        DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK = SystemPropertyUtil.getInt(
                "io.netty.allocator.maxCachedByteBuffersPerChunk", 1023);

        // 步骤7: 在调试级别记录所有配置参数
        if (logger.isDebugEnabled()) {
            // 记录堆内存Arena数量
            logger.debug("-Dio.netty.allocator.numHeapArenas: {}", DEFAULT_NUM_HEAP_ARENA);
            // 记录直接内存Arena数量
            logger.debug("-Dio.netty.allocator.numDirectArenas: {}", DEFAULT_NUM_DIRECT_ARENA);

            // 记录页大小，如果有回退错误则同时记录错误原因
            if (pageSizeFallbackCause == null) {
                logger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE);
            } else {
                logger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE, pageSizeFallbackCause);
            }

            // 记录最大order值，如果有回退错误则同时记录错误原因
            if (maxOrderFallbackCause == null) {
                logger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER);
            } else {
                logger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER, maxOrderFallbackCause);
            }

            // 记录计算得到的chunk大小
            logger.debug("-Dio.netty.allocator.chunkSize: {}", DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER);
            // 记录小型缓存大小
            logger.debug("-Dio.netty.allocator.smallCacheSize: {}", DEFAULT_SMALL_CACHE_SIZE);
            // 记录普通缓存大小
            logger.debug("-Dio.netty.allocator.normalCacheSize: {}", DEFAULT_NORMAL_CACHE_SIZE);
            // 记录最大可缓存的缓冲区容量
            logger.debug("-Dio.netty.allocator.maxCachedBufferCapacity: {}", DEFAULT_MAX_CACHED_BUFFER_CAPACITY);
            // 记录缓存清理间隔(分配次数)
            logger.debug("-Dio.netty.allocator.cacheTrimInterval: {}", DEFAULT_CACHE_TRIM_INTERVAL);
            // 记录缓存清理间隔(毫秒)
            logger.debug("-Dio.netty.allocator.cacheTrimIntervalMillis: {}", DEFAULT_CACHE_TRIM_INTERVAL_MILLIS);
            // 记录是否为所有线程启用缓存
            logger.debug("-Dio.netty.allocator.useCacheForAllThreads: {}", DEFAULT_USE_CACHE_FOR_ALL_THREADS);
            // 记录每个chunk最大缓存的ByteBuffer数量
            logger.debug("-Dio.netty.allocator.maxCachedByteBuffersPerChunk: {}",
                    DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK);
            // 记录是否为FastThreadLocal线程禁用缓存终结器
            logger.debug("-Dio.netty.allocator.disableCacheFinalizersForFastThreadLocalThreads: {}",
                    DEFAULT_DISABLE_CACHE_FINALIZERS_FOR_FAST_THREAD_LOCAL_THREADS);
        }
    }

    /**
     * 默认的PooledByteBufAllocator实例。
     * <p>
     * 根据平台偏好决定是否优先使用直接内存。在大多数现代操作系统上，
     * 默认会优先使用直接内存以提高网络IO性能。此实例在Netty应用中被
     * 广泛使用，作为默认的内存分配策略。
     * </p>
     */
    public static final PooledByteBufAllocator DEFAULT = new PooledByteBufAllocator(
            PlatformDependent.directBufferPreferred());

    /**
     * 堆内存区域(Arena)数组。
     * <p>
     * 每个PoolArena管理一组内存块(chunk)的分配和回收。
     * 使用多个Arena可以减少线程竞争，提高并发性能。
     * 每个线程会被分配到一个特定的Arena，通常基于线程ID的哈希值。
     * </p>
     */
    private final PoolArena<byte[]>[] heapArenas;

    /**
     * 直接内存区域(Arena)数组。
     * <p>
     * 管理直接内存(堆外内存)的分配和回收。
     * 直接内存适合用于网络IO操作，可以避免内存复制，
     * 但分配和释放的开销较大，因此使用内存池进行管理。
     * </p>
     */
    private final PoolArena<ByteBuffer>[] directArenas;

    /**
     * 小型缓存的容量大小。
     * <p>
     * 线程本地缓存中用于缓存小内存块(通常小于8KB)的队列容量。
     * 较大的缓存大小可以提高内存复用效率，减少锁竞争，
     * 但会增加内存占用。此值可在构造函数中配置。
     * </p>
     */
    private final int smallCacheSize;

    /**
     * 标准缓存的容量大小。
     * <p>
     * 线程本地缓存中用于缓存普通大小内存块(通常8KB-16MB)的队列容量。
     * 控制每个线程可以缓存的普通大小内存块数量，
     * 影响内存分配性能和内存占用平衡。
     * </p>
     */
    private final int normalCacheSize;

    /**
     * 堆内存区域指标列表。
     * <p>
     * 提供只读的堆内存区域统计信息，用于监控和调试。
     * 包含内存分配、释放、使用率等指标，
     * 可通过JMX或日志系统访问这些指标。
     * </p>
     */
    private final List<PoolArenaMetric> heapArenaMetrics;

    /**
     * 直接内存区域指标列表。
     * <p>
     * 提供只读的直接内存区域统计信息，用于监控和调试。
     * 包含内存分配、释放、使用率等指标，
     * 对于检测直接内存泄漏特别重要。
     * </p>
     */
    private final List<PoolArenaMetric> directArenaMetrics;

    /**
     * 线程本地缓存管理器。
     * <p>
     * 为每个线程维护一个本地内存缓存，避免多线程竞争。
     * 通过ThreadLocal实现，每个线程首次访问时创建专属缓存。
     * 这是Netty内存池高性能的关键组件，显著减少了锁竞争。
     * </p>
     */
    private final PoolThreadLocalCache threadCache;

    /**
     * 内存块(chunk)的大小，单位为字节。
     * <p>
     * 内存池中最大的分配单位，默认为16MB。
     * 所有小于此大小的内存请求都会在chunk内部分配，
     * 而超过此大小的请求会被视为"huge"，直接分配独立内存。
     * 此值影响内存利用率和碎片化程度。
     * </p>
     */
    private final int chunkSize;

    /**
     * 分配器指标收集器。
     * <p>
     * 提供整个分配器的统计信息和指标数据。
     * 包含已分配内存总量、活跃分配数、使用率等信息，
     * 用于监控系统内存使用情况和性能分析。
     * </p>
     */
    private final PooledByteBufAllocatorMetric metric;

    /**
     * 创建默认配置的PooledByteBufAllocator实例。
     * <p>
     * 使用系统默认设置，不优先使用直接内存。
     * </p>
     */
    public PooledByteBufAllocator() {
        this(false);
    }

    @SuppressWarnings("deprecation")
    public PooledByteBufAllocator(boolean preferDirect) {
        this(preferDirect, DEFAULT_NUM_HEAP_ARENA, DEFAULT_NUM_DIRECT_ARENA, DEFAULT_PAGE_SIZE, DEFAULT_MAX_ORDER);
    }

    @SuppressWarnings("deprecation")
    public PooledByteBufAllocator(int nHeapArena, int nDirectArena, int pageSize, int maxOrder) {
        this(false, nHeapArena, nDirectArena, pageSize, maxOrder);
    }

    /**
     * @deprecated use
     *             {@link PooledByteBufAllocator#PooledByteBufAllocator(boolean, int, int, int, int, int, int, boolean)}
     */
    @Deprecated
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder) {
        this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
                0, DEFAULT_SMALL_CACHE_SIZE, DEFAULT_NORMAL_CACHE_SIZE);
    }

    /**
     * @deprecated use
     *             {@link PooledByteBufAllocator#PooledByteBufAllocator(boolean, int, int, int, int, int, int, boolean)}
     */
    @Deprecated
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder,
            int tinyCacheSize, int smallCacheSize, int normalCacheSize) {
        this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder, smallCacheSize, normalCacheSize, DEFAULT_USE_CACHE_FOR_ALL_THREADS, DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT);
    }

    /**
     * @deprecated use
     *             {@link PooledByteBufAllocator#PooledByteBufAllocator(boolean, int, int, int, int, int, int, boolean)}
     */
    @Deprecated
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena,
            int nDirectArena, int pageSize, int maxOrder, int tinyCacheSize,
            int smallCacheSize, int normalCacheSize,
            boolean useCacheForAllThreads) {
        this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
                smallCacheSize, normalCacheSize,
                useCacheForAllThreads);
    }

    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena,
            int nDirectArena, int pageSize, int maxOrder,
            int smallCacheSize, int normalCacheSize,
            boolean useCacheForAllThreads) {
        this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
                smallCacheSize, normalCacheSize,
                useCacheForAllThreads, DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT);
    }

    /**
     * @deprecated use
     *             {@link PooledByteBufAllocator#PooledByteBufAllocator(boolean, int, int, int, int, int, int, boolean, int)}
     */
    @Deprecated
    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder,
            int tinyCacheSize, int smallCacheSize, int normalCacheSize,
            boolean useCacheForAllThreads, int directMemoryCacheAlignment) {
        this(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder,
                smallCacheSize, normalCacheSize,
                useCacheForAllThreads, directMemoryCacheAlignment);
    }

    public PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder,
            int smallCacheSize, int normalCacheSize,
            boolean useCacheForAllThreads, int directMemoryCacheAlignment) {
        super(preferDirect);
        threadCache = new PoolThreadLocalCache(useCacheForAllThreads);
        this.smallCacheSize = smallCacheSize;
        this.normalCacheSize = normalCacheSize;

        if (directMemoryCacheAlignment != 0) {
            if (!PlatformDependent.hasAlignDirectByteBuffer()) {
                throw new UnsupportedOperationException("Buffer alignment is not supported. " +
                        "Either Unsafe or ByteBuffer.alignSlice() must be available.");
            }

            // Ensure page size is a whole multiple of the alignment, or bump it to the next
            // whole multiple.
            pageSize = (int) PlatformDependent.align(pageSize, directMemoryCacheAlignment);
        }

        chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);

        checkPositiveOrZero(nHeapArena, "nHeapArena");
        checkPositiveOrZero(nDirectArena, "nDirectArena");

        checkPositiveOrZero(directMemoryCacheAlignment, "directMemoryCacheAlignment");
        if (directMemoryCacheAlignment > 0 && !isDirectMemoryCacheAlignmentSupported()) {
            throw new IllegalArgumentException("directMemoryCacheAlignment is not supported");
        }

        if ((directMemoryCacheAlignment & -directMemoryCacheAlignment) != directMemoryCacheAlignment) {
            throw new IllegalArgumentException("directMemoryCacheAlignment: "
                    + directMemoryCacheAlignment + " (expected: power of two)");
        }

        int pageShifts = validateAndCalculatePageShifts(pageSize, directMemoryCacheAlignment);

        if (nHeapArena > 0) {
            heapArenas = newArenaArray(nHeapArena);
            List<PoolArenaMetric> metrics = new ArrayList<PoolArenaMetric>(heapArenas.length);
            final SizeClasses sizeClasses = new SizeClasses(pageSize, pageShifts, chunkSize, 0);
            for (int i = 0; i < heapArenas.length; i++) {
                PoolArena.HeapArena arena = new PoolArena.HeapArena(this, sizeClasses);
                heapArenas[i] = arena;
                metrics.add(arena);
            }
            heapArenaMetrics = Collections.unmodifiableList(metrics);
        } else {
            heapArenas = null;
            heapArenaMetrics = Collections.emptyList();
        }

        if (nDirectArena > 0) {
            directArenas = newArenaArray(nDirectArena);
            List<PoolArenaMetric> metrics = new ArrayList<PoolArenaMetric>(directArenas.length);
            final SizeClasses sizeClasses = new SizeClasses(pageSize, pageShifts, chunkSize,
                    directMemoryCacheAlignment);
            for (int i = 0; i < directArenas.length; i++) {
                PoolArena.DirectArena arena = new PoolArena.DirectArena(this, sizeClasses);
                directArenas[i] = arena;
                metrics.add(arena);
            }
            directArenaMetrics = Collections.unmodifiableList(metrics);
        } else {
            directArenas = null;
            directArenaMetrics = Collections.emptyList();
        }
        metric = new PooledByteBufAllocatorMetric(this);
    }

    @SuppressWarnings("unchecked")
    private static <T> PoolArena<T>[] newArenaArray(int size) {
        return new PoolArena[size];
    }

    private static int validateAndCalculatePageShifts(int pageSize, int alignment) {
        if (pageSize < MIN_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: " + MIN_PAGE_SIZE + ')');
        }

        if ((pageSize & pageSize - 1) != 0) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
        }

        if (pageSize < alignment) {
            throw new IllegalArgumentException("Alignment cannot be greater than page size. " +
                    "Alignment: " + alignment + ", page size: " + pageSize + '.');
        }

        // Logarithm base 2. At this point we know that pageSize is a power of two.
        return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
    }

    private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        for (int i = maxOrder; i > 0; i--) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(String.format(
                        "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        // 1. 获取当前线程的缓存
        PoolThreadCache cache = threadCache.get();
        // 2. 获取堆内存Arena
        PoolArena<byte[]> heapArena = cache.heapArena;

        final ByteBuf buf;
        // 3. 判断是否有可用的堆内存Arena
        if (heapArena != null) {
            // 4. 使用池化分配
            buf = heapArena.allocate(cache, initialCapacity, maxCapacity);
        } else {
            // 5. 非池化分配
            buf = PlatformDependent.hasUnsafe() ? new UnpooledUnsafeHeapByteBuf(this, initialCapacity, maxCapacity)
                    : new UnpooledHeapByteBuf(this, initialCapacity, maxCapacity);
        }
        // 6. 包装为支持内存泄漏检测的缓冲区
        return toLeakAwareBuffer(buf);
    }

    /**
     * 创建一个新的直接内存缓冲区。
     * <分配内存，如果池化分配不可用，>
     * 该方法实现了{@link AbstractByteBufAllocator#newDirectBuffer(int, int)}抽象方法，
     * 负责直接内存ByteBuf的分配。方法首先尝试使用池化机制分配内存，如果池化分配不可用，
     * 则降级为非池化分配。
     * <p>
     * 内存分配流程：
     * <ol>
     * <li>获取当前线程的缓存</li>
     * <li>尝试从线程缓存中获取直接内存Arena</li>
     * <li>如果Arena可用，使用池化内存分配策略</li>
     * <li>如果Arena不可用，降级为非池化分配：
     * <ul>
     * <li>当平台支持Unsafe时，使用Unsafe实现的高效直接内存分配</li>
     * <li>当平台不支持Unsafe时，使用标准JDK的直接内存分配</li>
     * </ul>
     * </li>
     * <li>返回前将缓冲区包装为支持内存泄漏检测的缓冲区</li>
     * </ol>
     * 
     * <p>
     * 内存池化分配的优势：
     * <ul>
     * <li>减少内存分配和释放的开销</li>
     * <li>通过内存复用减少垃圾收集压力</li>
     * <li>降低内存碎片化</li>
     * <li>提高内存分配的吞吐量</li>
     * </ul>
     * 
     * @param initialCapacity 缓冲区的初始容量（以字节为单位）
     * @param maxCapacity     缓冲区允许的最大容量（以字节为单位）
     * @return 新创建的直接内存缓冲区
     * 
     * @see PoolArena#allocate(PoolThreadCache, int, int)
     * @see UnsafeByteBufUtil#newUnsafeDirectByteBuf(ByteBufAllocator, int, int)
     * @see UnpooledDirectByteBuf
     * @see AbstractByteBufAllocator#toLeakAwareBuffer(ByteBuf)
     */
    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        // 1. 获取当前线程的缓存
        PoolThreadCache cache = threadCache.get();
        // 2. 获取线程缓存中的直接内存Arena
        PoolArena<ByteBuffer> directArena = cache.directArena;

        final ByteBuf buf;
        // 3. 判断是否有可用的直接内存Arena
        if (directArena != null) {
            // 4. 使用Arena池化分配内存
            buf = directArena.allocate(cache, initialCapacity, maxCapacity);
        } else {
            // 5. 没有可用Arena时，降级为非池化分配
            buf = PlatformDependent.hasUnsafe() ?
            // 5.1 如果支持Unsafe，使用Unsafe实现直接内存分配
                    UnsafeByteBufUtil.newUnsafeDirectByteBuf(this, initialCapacity, maxCapacity) :
                    // 5.2 不支持Unsafe时，使用标准JDK直接内存分配
                    new UnpooledDirectByteBuf(this, initialCapacity, maxCapacity);
        }
        // 6. 包装为支持内存泄漏检测的缓冲区
        /**
         * - 内存泄漏检测功能：
         * 它将普通缓冲区包装为支持内存泄漏检测的缓冲区
         * 记录缓冲区的分配位置和使用情况
         * 当缓冲区未正确释放时，能够提供详细的分配和泄漏信息
         * - 如果不使用此包装，可能导致的问题：
         * 内存泄漏无法被追踪，导致系统长期运行后内存耗尽
         * 难以定位哪些代码未正确释放缓冲区
         * 在大型系统中，内存问题排查将变得极其困难
         * - 实现机制：
         * 使用ResourceLeakDetector来跟踪缓冲区的生命周期
         * 当检测级别设置为PARANOID或ADVANCED时，能够提供更详细的泄漏信息
         * 对性能有轻微影响，但在开发和测试环境中非常有价值
         */
        // TODO: 内存泄漏检测功能
        // question: 是否可以设置堆外内存的自动回收机制
        return toLeakAwareBuffer(buf);
    }

    /**
     * Default number of heap arenas - System Property:
     * io.netty.allocator.numHeapArenas - default 2 * cores
     */
    public static int defaultNumHeapArena() {
        return DEFAULT_NUM_HEAP_ARENA;
    }

    /**
     * Default number of direct arenas - System Property:
     * io.netty.allocator.numDirectArenas - default 2 * cores
     */
    public static int defaultNumDirectArena() {
        return DEFAULT_NUM_DIRECT_ARENA;
    }

    /**
     * Default buffer page size - System Property: io.netty.allocator.pageSize -
     * default 8192
     */
    public static int defaultPageSize() {
        return DEFAULT_PAGE_SIZE;
    }

    /**
     * Default maximum order - System Property: io.netty.allocator.maxOrder -
     * default 9
     */
    public static int defaultMaxOrder() {
        return DEFAULT_MAX_ORDER;
    }

    /**
     * Default control creation of PoolThreadCache finalizers for
     * FastThreadLocalThreads -
     * System Property:
     * io.netty.allocator.disableCacheFinalizersForFastThreadLocalThreads - default
     * false
     */
    public static boolean defaultDisableCacheFinalizersForFastThreadLocalThreads() {
        return DEFAULT_DISABLE_CACHE_FINALIZERS_FOR_FAST_THREAD_LOCAL_THREADS;
    }

    /**
     * Default thread caching behavior - System Property:
     * io.netty.allocator.useCacheForAllThreads - default false
     */
    public static boolean defaultUseCacheForAllThreads() {
        return DEFAULT_USE_CACHE_FOR_ALL_THREADS;
    }

    /**
     * Default prefer direct - System Property: io.netty.noPreferDirect - default
     * false
     */
    public static boolean defaultPreferDirect() {
        return PlatformDependent.directBufferPreferred();
    }

    /**
     * Default tiny cache size - default 0
     *
     * @deprecated Tiny caches have been merged into small caches.
     */
    @Deprecated
    public static int defaultTinyCacheSize() {
        return 0;
    }

    /**
     * Default small cache size - System Property: io.netty.allocator.smallCacheSize
     * - default 256
     */
    public static int defaultSmallCacheSize() {
        return DEFAULT_SMALL_CACHE_SIZE;
    }

    /**
     * Default normal cache size - System Property:
     * io.netty.allocator.normalCacheSize - default 64
     */
    public static int defaultNormalCacheSize() {
        return DEFAULT_NORMAL_CACHE_SIZE;
    }

    /**
     * Return {@code true} if direct memory cache alignment is supported,
     * {@code false} otherwise.
     */
    public static boolean isDirectMemoryCacheAlignmentSupported() {
        return PlatformDependent.hasUnsafe();
    }

    @Override
    public boolean isDirectBufferPooled() {
        return directArenas != null;
    }

    /**
     * @deprecated will be removed
     *             Returns {@code true} if the calling {@link Thread} has a
     *             {@link ThreadLocal} cache for the allocated
     *             buffers.
     */
    @Deprecated
    public boolean hasThreadLocalCache() {
        return threadCache.isSet();
    }

    /**
     * @deprecated will be removed
     *             Free all cached buffers for the calling {@link Thread}.
     */
    @Deprecated
    public void freeThreadLocalCache() {
        threadCache.remove();
    }

    /**
     * 线程本地缓存管理器，为每个线程提供专用的内存缓存。
     * <p>
     * 此类继承自FastThreadLocal，为每个访问的线程创建和维护独立的PoolThreadCache实例。
     * 它实现了Netty内存池的线程隔离策略，是减少多线程内存分配竞争的核心机制。
     * </p>
     * 
     * <h3>工作原理</h3>
     * <ol>
     * <li>首次访问：线程首次请求内存时，调用initialValue()创建专属缓存</li>
     * <li>Arena分配：为线程分配负载最低的PoolArena，实现负载均衡</li>
     * <li>条件判断：根据线程类型和配置决定是否启用完整缓存</li>
     * <li>缓存清理：配置定期执行的缓存修剪任务，防止内存泄漏</li>
     * <li>资源释放：线程终止时自动调用onRemoval()清理相关资源</li>
     * </ol>
     *
     * <h3>性能影响</h3>
     * <p>
     * 此缓存机制能显著提升内存密集型应用的性能，特别是在高并发场景下：
     * <ul>
     * <li>减少锁竞争：缓存隔离降低多线程同步开销</li>
     * <li>加速分配：频繁使用的内存大小可以从缓存快速分配</li>
     * <li>减少GC：内存复用降低垃圾收集压力</li>
     * </ul>
     * </p>
     *
     * @see PoolThreadCache 线程本地缓存的实际实现类
     * @see FastThreadLocal Netty优化的线程本地变量，性能优于JDK ThreadLocal
     */
    private final class PoolThreadLocalCache extends FastThreadLocal<PoolThreadCache> {
        // 是否为所有类型的线程启用缓存，默认false（仅为Netty I/O线程启用）
        private final boolean useCacheForAllThreads;

        /**
         * 构造线程本地缓存管理器
         * 
         * @param useCacheForAllThreads 是否为所有线程类型启用缓存
         */
        PoolThreadLocalCache(boolean useCacheForAllThreads) {
            this.useCacheForAllThreads = useCacheForAllThreads;
        }

        /**
         * 为当前线程创建专用的内存缓存实例。
         * <p>
         * 此方法虽然在概念上只被单个线程调用（ThreadLocal模式），但仍需使用synchronized修饰，原因如下：
         * <ul>
         * <li>共享资源访问：方法内部会调用leastUsedArena()选择并更新Arena的线程计数器</li>
         * <li>负载均衡一致性：同步确保在选择Arena过程中，其他线程不会同时修改计数值</li>
         * <li>防止竞态条件：没有同步时，多个线程可能同时选择同一个Arena，破坏负载均衡</li>
         * </ul>
         * </p>
         * 
         * <h3>示例场景（无同步时）</h3>
         * <ol>
         * <li>线程A和线程B同时执行initialValue()</li>
         * <li>两个线程都读取Arena1的numThreadCaches值为0</li>
         * <li>两个线程都认为Arena1是负载最低的并选择它</li>
         * <li>结果Arena1获得2个线程，而其他可能空闲的Arena未被使用</li>
         * </ol>
         * @return 为当前线程创建的PoolThreadCache实例
         */
        @Override
        protected synchronized PoolThreadCache initialValue() {
            // 选择负载最低的Arena分配给当前线程，实现负载均衡
            final PoolArena<byte[]> heapArena = leastUsedArena(heapArenas);
            final PoolArena<ByteBuffer> directArena = leastUsedArena(directArenas);

            // 获取当前线程信息
            final Thread current = Thread.currentThread();
            // 检查当前线程是否关联到EventExecutor
            final EventExecutor executor = ThreadExecutorMap.currentExecutor();

            // 决定是否为当前线程启用完整缓存功能
            if (useCacheForAllThreads ||
            // 如果是FastThreadLocalThread类型（Netty优化的线程），总是启用缓存
                    current instanceof FastThreadLocalThread ||
                    // 如果线程被EventExecutor使用，很可能会进行大量内存分配，启用缓存
                    executor != null) {
                // 创建完整功能的缓存，配置缓存大小和清理参数
                final PoolThreadCache cache = new PoolThreadCache(
                        heapArena, directArena, smallCacheSize, normalCacheSize,
                        DEFAULT_MAX_CACHED_BUFFER_CAPACITY, DEFAULT_CACHE_TRIM_INTERVAL, useCacheFinalizers(current));

                // 如果配置了缓存清理间隔，添加定期执行的清理任务
                if (DEFAULT_CACHE_TRIM_INTERVAL_MILLIS > 0) {
                    if (executor != null) {
                        // 在EventExecutor上调度定期执行的缓存清理任务
                        executor.scheduleAtFixedRate(trimTask, DEFAULT_CACHE_TRIM_INTERVAL_MILLIS,
                                DEFAULT_CACHE_TRIM_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
                    }
                }
                return cache;
            }
            // 不启用缓存，创建零容量缓存对象（实际不缓存任何内存）
            return new PoolThreadCache(heapArena, directArena, 0, 0, 0, 0, false);
        }

        @Override
        protected void onRemoval(PoolThreadCache threadCache) {
            // 当线程终止或显式移除缓存时，释放所有缓存的内存
            threadCache.free(false);
        }

        /**
         * 为当前线程选择负载最低的Arena，实现负载均衡
         * <p>
         * 此方法通过检查每个Arena当前绑定的线程缓存数量，
         * 选择绑定线程最少的Arena分配给当前线程，
         * 从而减少多线程在同一个Arena上竞争的概率。
         * </p>
         *
         * @param <T>    Arena类型参数（byte[]用于堆内存，ByteBuffer用于直接内存）
         * @param arenas 可用的Arena数组
         * @return 负载最低的Arena，如果没有可用Arena则返回null
         */
        private <T> PoolArena<T> leastUsedArena(PoolArena<T>[] arenas) {
            // 无可用Arena时返回null
            if (arenas == null || arenas.length == 0) {
                return null;
            }

            // 默认选择第一个Arena作为初始最小负载Arena
            PoolArena<T> minArena = arenas[0];

            // 优化：如果第一个Arena从未被使用过，直接返回它
            // 这可以避免不必要的循环比较，提高性能
            if (minArena.numThreadCaches.get() == CACHE_NOT_USED) {
                return minArena;
            }

            // 遍历所有Arena，寻找绑定线程数最少的那个
            for (int i = 1; i < arenas.length; i++) {
                PoolArena<T> arena = arenas[i];
                // 比较当前Arena与最小负载Arena的绑定线程数
                if (arena.numThreadCaches.get() < minArena.numThreadCaches.get()) {
                    minArena = arena;
                }
            }

            return minArena;
        }
    }

    private static boolean useCacheFinalizers(Thread current) {
        if (!defaultDisableCacheFinalizersForFastThreadLocalThreads()) {
            return true;
        }
        return current instanceof FastThreadLocalThread &&
                ((FastThreadLocalThread) current).willCleanupFastThreadLocals();
    }

    @Override
    public PooledByteBufAllocatorMetric metric() {
        return metric;
    }

    /**
     * Return the number of heap arenas.
     *
     * @deprecated use {@link PooledByteBufAllocatorMetric#numHeapArenas()}.
     */
    @Deprecated
    public int numHeapArenas() {
        return heapArenaMetrics.size();
    }

    /**
     * Return the number of direct arenas.
     *
     * @deprecated use {@link PooledByteBufAllocatorMetric#numDirectArenas()}.
     */
    @Deprecated
    public int numDirectArenas() {
        return directArenaMetrics.size();
    }

    /**
     * Return a {@link List} of all heap {@link PoolArenaMetric}s that are provided
     * by this pool.
     *
     * @deprecated use {@link PooledByteBufAllocatorMetric#heapArenas()}.
     */
    @Deprecated
    public List<PoolArenaMetric> heapArenas() {
        return heapArenaMetrics;
    }

    /**
     * Return a {@link List} of all direct {@link PoolArenaMetric}s that are
     * provided by this pool.
     *
     * @deprecated use {@link PooledByteBufAllocatorMetric#directArenas()}.
     */
    @Deprecated
    public List<PoolArenaMetric> directArenas() {
        return directArenaMetrics;
    }

    /**
     * Return the number of thread local caches used by this
     * {@link PooledByteBufAllocator}.
     *
     * @deprecated use {@link PooledByteBufAllocatorMetric#numThreadLocalCaches()}.
     */
    @Deprecated
    public int numThreadLocalCaches() {
        return Math.max(numThreadLocalCaches(heapArenas), numThreadLocalCaches(directArenas));
    }

    private static int numThreadLocalCaches(PoolArena<?>[] arenas) {
        if (arenas == null) {
            return 0;
        }

        int total = 0;
        for (PoolArena<?> arena : arenas) {
            total += arena.numThreadCaches.get();
        }

        return total;
    }

    /**
     * Return the size of the tiny cache.
     *
     * @deprecated use {@link PooledByteBufAllocatorMetric#tinyCacheSize()}.
     */
    @Deprecated
    public int tinyCacheSize() {
        return 0;
    }

    /**
     * Return the size of the small cache.
     *
     * @deprecated use {@link PooledByteBufAllocatorMetric#smallCacheSize()}.
     */
    @Deprecated
    public int smallCacheSize() {
        return smallCacheSize;
    }

    /**
     * Return the size of the normal cache.
     *
     * @deprecated use {@link PooledByteBufAllocatorMetric#normalCacheSize()}.
     */
    @Deprecated
    public int normalCacheSize() {
        return normalCacheSize;
    }

    /**
     * Return the chunk size for an arena.
     *
     * @deprecated use {@link PooledByteBufAllocatorMetric#chunkSize()}.
     */
    @Deprecated
    public final int chunkSize() {
        return chunkSize;
    }

    final long usedHeapMemory() {
        return usedMemory(heapArenas);
    }

    final long usedDirectMemory() {
        return usedMemory(directArenas);
    }

    private static long usedMemory(PoolArena<?>[] arenas) {
        if (arenas == null) {
            return -1;
        }
        long used = 0;
        for (PoolArena<?> arena : arenas) {
            used += arena.numActiveBytes();
            if (used < 0) {
                return Long.MAX_VALUE;
            }
        }
        return used;
    }

    /**
     * Returns the number of bytes of heap memory that is currently pinned to heap
     * buffers allocated by a
     * {@link ByteBufAllocator}, or {@code -1} if unknown.
     * A buffer can pin more memory than its {@linkplain ByteBuf#capacity()
     * capacity} might indicate,
     * due to implementation details of the allocator.
     */
    public final long pinnedHeapMemory() {
        return pinnedMemory(heapArenas);
    }

    /**
     * Returns the number of bytes of direct memory that is currently pinned to
     * direct buffers allocated by a
     * {@link ByteBufAllocator}, or {@code -1} if unknown.
     * A buffer can pin more memory than its {@linkplain ByteBuf#capacity()
     * capacity} might indicate,
     * due to implementation details of the allocator.
     */
    public final long pinnedDirectMemory() {
        return pinnedMemory(directArenas);
    }

    private static long pinnedMemory(PoolArena<?>[] arenas) {
        if (arenas == null) {
            return -1;
        }
        long used = 0;
        for (PoolArena<?> arena : arenas) {
            used += arena.numPinnedBytes();
            if (used < 0) {
                return Long.MAX_VALUE;
            }
        }
        return used;
    }

    final PoolThreadCache threadCache() {
        PoolThreadCache cache = threadCache.get();
        assert cache != null;
        return cache;
    }

    /**
     * Trim thread local cache for the current {@link Thread}, which will give back
     * any cached memory that was not
     * allocated frequently since the last trim operation.
     *
     * Returns {@code true} if a cache for the current {@link Thread} exists and so
     * was trimmed, false otherwise.
     */
    public boolean trimCurrentThreadCache() {
        PoolThreadCache cache = threadCache.getIfExists();
        if (cache != null) {
            cache.trim();
            return true;
        }
        return false;
    }

    /**
     * Returns the status of the allocator (which contains all metrics) as string.
     * Be aware this may be expensive
     * and so should not called too frequently.
     */
    public String dumpStats() {
        int heapArenasLen = heapArenas == null ? 0 : heapArenas.length;
        StringBuilder buf = new StringBuilder(512)
                .append(heapArenasLen)
                .append(" heap arena(s):")
                .append(StringUtil.NEWLINE);
        if (heapArenasLen > 0) {
            for (PoolArena<byte[]> a : heapArenas) {
                buf.append(a);
            }
        }

        int directArenasLen = directArenas == null ? 0 : directArenas.length;

        buf.append(directArenasLen)
                .append(" direct arena(s):")
                .append(StringUtil.NEWLINE);
        if (directArenasLen > 0) {
            for (PoolArena<ByteBuffer> a : directArenas) {
                buf.append(a);
            }
        }

        return buf.toString();
    }
}
