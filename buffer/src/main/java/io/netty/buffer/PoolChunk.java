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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.util.internal.LongCounter;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.PriorityQueue;

/**
 * PoolChunk中PageRun/PoolSubpage分配算法的详细描述
 * <p>
 * 本类实现了Netty内存池中的核心内存分配算法，采用分页管理机制，支持大内存块(run)和小内存块(subpage)的分配。
 * 该实现参考了jemalloc的设计理念，通过位图管理和内存复用提高内存使用效率。
 * </p>
 *
 * <h3>核心概念</h3>
 * <p>
 * 以下术语对于理解代码实现至关重要：
 * <ul>
 *   <li><b>page（页）</b> - 内存块中可以分配的最小单位，大小固定</li>
 *   <li><b>run（运行块）</b> - 一组连续的页的集合，用于大内存分配</li>
 *   <li><b>chunk（块）</b> - 一组运行块的集合，是内存分配的最大单位</li>
 *   <li><b>subpage（子页）</b> - 页的细分，用于小内存分配</li>
 * </ul>
 * 在本实现中：chunkSize = maxPages * pageSize
 * </p>
 *
 * <h3>内存分配机制</h3>
 * <p>
 * 初始化时，系统分配一个大小为chunkSize的字节数组作为内存池。
 * 当需要创建指定大小的ByteBuf时：
 * <ol>
 *   <li>在字节数组中搜索第一个有足够空闲空间的位置</li>
 *   <li>返回一个编码了偏移信息的(long)handle（句柄）</li>
 *   <li>该内存段被标记为已保留，确保只能被一个ByteBuf使用</li>
 * </ol>
 * </p>
 *
 * <h3>大小标准化</h3>
 * <p>
 * 所有内存大小都通过{@link PoolArena#sizeClass#size2SizeIdx(int)}方法进行标准化，
 * 确保当请求大于pageSize的内存段时，normalizedCapacity等于{@link SizeClasses}中的下一个最近大小。
 * 这种标准化机制有助于减少内存碎片，提高内存使用效率。
 * </p>
 *
 * <h3>内存布局</h3>
 * <pre>
 *     /-----------------\
 *     | run（运行块）     |
 *     |                 |
 *     |                 |
 *     |-----------------|
 *     | run（运行块）     |
 *     |                 |
 *     |-----------------|
 *     | unalloctated    |
 *     | (freed)         |
 *     |                 |
 *     |-----------------|
 *     | subpage（子页）   |
 *     |-----------------|
 *     | unallocated     |
 *     | (freed)         |
 *     | ...             |
 *     | ...             |
 *     | ...             |
 *     |                 |
 *     |                 |
 *     |                 |
 *     \-----------------/
 * </pre>
 *
 * <h3>句柄(Handle)设计</h3>
 * <p>
 * handle是一个64位长整型数，其位布局如下：
 * <pre>
 * oooooooo ooooooos ssssssss ssssssue bbbbbbbb bbbbbbbb bbbbbbbb bbbbbbbb
 * </pre>
 * 其中：
 * <ul>
 *   <li>o: runOffset（运行块在chunk中的页偏移量），15位</li>
 *   <li>s: size（运行块的页数），15位</li>
 *   <li>u: isUsed?（是否已使用），1位</li>
 *   <li>e: isSubpage?（是否是子页），1位</li>
 *   <li>b: bitmapIdx（子页的位图索引），如果不是子页则为0，32位</li>
 * </ul>
 * </p>
 *
 * <h3>数据结构</h3>
 * <p>
 * <b>runsAvailMap（可用运行块映射）</b>：
 * <ul>
 *   <li>管理所有运行块（已使用和未使用）的映射</li>
 *   <li>存储每个运行块的第一个和最后一个runOffset</li>
 *   <li>键：runOffset</li>
 *   <li>值：handle</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>runsAvail（可用运行块队列）</b>：
 * <ul>
 *   <li>一个{@link PriorityQueue}数组</li>
 *   <li>每个队列管理相同大小的运行块</li>
 *   <li>运行块按偏移量排序，确保总是分配偏移量较小的运行块</li>
 * </ul>
 * </p>
 *
 * <h3>核心算法</h3>
 * <p>
 * <b>初始化</b>：
 * <ul>
 *   <li>存储初始运行块（整个chunk）</li>
 *   <li>初始参数：runOffset = 0, size = chunkSize, isUsed = false, isSubpage = false, bitmapIdx = 0</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>allocateRun(size)算法</b>：
 * <ol>
 *   <li>根据大小在runsAvails中找到第一个可用的运行块</li>
 *   <li>如果运行块的页数大于请求的页数，则分割它，并保存尾部运行块供后续使用</li>
 * </ol>
 * </p>
 *
 * <p>
 * <b>allocateSubpage(size)算法</b>：
 * <ol>
 *   <li>根据大小找到一个未满的子页</li>
 *   <li>如果已存在则直接返回，否则分配新的PoolSubpage并调用init()</li>
 *   <li>注意：初始化时子页对象被添加到PoolArena的subpagesPool中</li>
 *   <li>调用subpage.allocate()完成分配</li>
 * </ol>
 * </p>
 *
 * <p>
 * <b>free(handle, length, nioBuffer)算法</b>：
 * <ol>
 *   <li>如果是子页，将内存块返回到这个子页</li>
 *   <li>如果子页未使用或是运行块，则开始释放这个运行块</li>
 *   <li>合并连续的可用运行块</li>
 *   <li>保存合并后的运行块</li>
 * </ol>
 * </p>
 *
 * @see PoolArena
 * @see PoolSubpage
 * @see SizeClasses
 */
final class PoolChunk<T> implements PoolChunkMetric {
    /**
     * 位操作常量组 - 用于定义内存句柄(handle)中各个字段的位长度
     * 
     * 所有这些常量共同决定了内存句柄的位布局: 
     * oooooooo ooooooos ssssssss ssssssue bbbbbbbb bbbbbbbb bbbbbbbb bbbbbbbb
     */
    private static final int SIZE_BIT_LENGTH = 15; // 大小字段的位长度
    private static final int INUSED_BIT_LENGTH = 1; // 使用标志的位长度
    private static final int SUBPAGE_BIT_LENGTH = 1; // 子页标志的位长度
    private static final int BITMAP_IDX_BIT_LENGTH = 32; // 位图索引的位长度

    /**
     * 位移常量组 - 用于位操作时构建和解析内存句柄
     * 
     * 这些常量定义了句柄中各字段的偏移位置，使得可以通过位运算有效地提取或设置字段值
     */
    static final int IS_SUBPAGE_SHIFT = BITMAP_IDX_BIT_LENGTH; // 子页标志的位移
    static final int IS_USED_SHIFT = SUBPAGE_BIT_LENGTH + IS_SUBPAGE_SHIFT; // 使用标志的位移
    static final int SIZE_SHIFT = INUSED_BIT_LENGTH + IS_USED_SHIFT; // 大小字段的位移
    static final int RUN_OFFSET_SHIFT = SIZE_BIT_LENGTH + SIZE_SHIFT; // 运行偏移量的位移

    /** 所属的内存竞技场，负责管理内存分配的更高级别结构 */
    final PoolArena<T> arena;
    
    /** 基础对象，用于内存访问的底层引用，通常是原始内存块的引用 */
    final Object base;
    
    /** 实际内存对象，泛型T可以是直接内存(DirectBuffer)或堆内存(byte[]) */
    final T memory;
    
    /** 指示是否为非池化块，true表示这是一个不参与池化管理的特殊块 */
    final boolean unpooled;
    
    /** 页面大小(字节)，是内存分配的基本单位 */
    final int pageSize;
    
    /** 页大小的位移值，用于快速计算地址(pageSize = 1 << pageShifts) */
    final int pageShifts;
    
    /** 块大小(字节)，表示整个PoolChunk管理的内存总量 */
    final int chunkSize;
    
    /** 最大页索引，用于限制页数组的边界 */
    final int maxPageIdx;
    
    /** 当前块中可用的字节数，随着分配和释放动态变化 */
    int freeBytes;

    /**
     * 可用运行块映射表，用于高效管理和检索可用内存区域
     * 
     * 存储格式: 键=runOffset, 值=handle
     * - 每个运行块由第一页偏移量和最后一页偏移量标识
     * - 通过这种方式可以快速查找相邻的可用运行块进行合并
     */
    private final LongLongHashMap runsAvailMap;

    /**
     * 可用运行块优先队列数组，按大小分类管理可用运行块
     * 
     * 数组索引对应页大小类别，每个队列内部按偏移量排序
     * 使用优先队列确保总是分配偏移量较小的运行块，有助于减少内存碎片
     */
    private final IntPriorityQueue[] runsAvail;

    /**
     * 运行块数据结构访问锁，确保在多线程环境下安全操作
     * 
     * 保护runsAvailMap和runsAvail数据结构的并发访问
     */
    private final ReentrantLock runsAvailLock;

    /**
     * 子页数组，管理块中所有的子页实例
     * 
     * 索引是页偏移量，每个元素是一个PoolSubpage实例
     * 用于小内存分配的精细管理
     */
    private final PoolSubpage<T>[] subpages;

    /**
     * 固定内存计数器 - 统计当前正在被ByteBuf实例使用的内存量
     * 
     * 这个计数器跟踪被"固定"的内存总量，即已分配且正在使用的内存
     * 用于监控内存使用情况和检测可能的内存泄漏
     */
    private final LongCounter pinnedBytes = PlatformDependent.newLongCounter();

    /**
     * ByteBuffer缓存池 - 用于缓存从内存创建的ByteBuffer实例
     * 
     * 这些缓存的ByteBuffer实例只是底层内存的视图容器，不包含实际数据
     * 缓存它们可以显著减少GC压力，因为在Pooled*ByteBuf操作中频繁需要这些视图
     * 
     * 特别说明:
     * 1. 如果PoolChunk是非池化的(unpooled=true)，此字段可能为null
     * 2. 在非池化情况下，缓存ByteBuffer实例没有意义，因为它们不会被复用
     */
    private final Deque<ByteBuffer> cachedNioBuffers;

    /** 指向父级块列表的引用，用于内存使用率管理和块移动 */
    PoolChunkList<T> parent;
    
    /** 双向链表中前一个块的引用，便于快速遍历和块管理 */
    PoolChunk<T> prev;
    
    /** 双向链表中后一个块的引用，便于快速遍历和块管理 */
    PoolChunk<T> next;

    // TODO: Test if adding padding helps under contention
    // private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /**
     * PoolChunk的构造函数，用于初始化一个新的内存块。
     * <p>
     * 该构造函数负责初始化一个内存块的所有必要组件，包括：
     * <ul>
     *   <li>基本属性（arena、内存地址、大小等）</li>
     *   <li>运行块管理结构（runsAvail队列数组）</li>
     *   <li>子页管理结构（subpages数组）</li>
     *   <li>初始可用运行块</li>
     * </ul>
     * </p>
     * 
     * <h3>内存布局</h3>
     * <p>
     * 新创建的内存块初始状态为：
     * <ul>
     *   <li>一个完整的可用运行块，大小为整个chunk</li>
     *   <li>子页数组初始化为空</li>
     *   <li>运行块可用队列初始化为空</li>
     * </ul>
     * </p>
     * 
     * <h3>关键参数</h3>
     * <p>
     * <ul>
     *   <li>pageSize：页面大小，通常为8KB</li>
     *   <li>pageShifts：页面大小的位移值，用于快速计算</li>
     *   <li>chunkSize：整个内存块的大小，通常为16MB</li>
     *   <li>maxPageIdx：最大页面索引，用于确定运行块队列数组大小</li>
     * </ul>
     * </p>
     *
     * @param arena 所属的内存分配区域
     * @param base 内存基址（用于计算实际内存地址）
     * @param memory 实际的内存对象（ByteBuffer或byte[]）
     * @param pageSize 页面大小
     * @param pageShifts 页面大小的位移值
     * @param chunkSize 内存块大小
     * @param maxPageIdx 最大页面索引
     */
    @SuppressWarnings("unchecked")
    PoolChunk(PoolArena<T> arena, Object base, T memory, int pageSize, int pageShifts, int chunkSize, int maxPageIdx) {
        // 标记这是一个池化的内存块
        unpooled = false;
        
        // 初始化基本属性
        this.arena = arena;           // 所属的内存分配区域
        this.base = base;             // 内存基址
        this.memory = memory;         // 实际的内存对象
        this.pageSize = pageSize;     // 页面大小
        this.pageShifts = pageShifts; // 页面大小的位移值
        this.chunkSize = chunkSize;   // 内存块大小
        //? maxPageIdx和pageSize chunkSize之间是否存在关系?
        this.maxPageIdx = maxPageIdx; // 最大页面索引
        
        // 初始化可用字节数为整个chunk大小
        freeBytes = chunkSize;

        // 创建运行块可用队列数组，用于管理不同大小的运行块
        // 数组大小由maxPageIdx决定，每个元素是一个优先级队列
        runsAvail = newRunsAvailqueueArray(maxPageIdx);
        
        // 创建运行块管理的锁，用于同步访问
        runsAvailLock = new ReentrantLock();
        
        // 创建运行块映射表，用于快速查找运行块
        // 键为运行块偏移量，值为运行块句柄
        runsAvailMap = new LongLongHashMap(-1);
        

        // 初始化第一个可用运行块
        // 1. 计算页面数量
        int pages = chunkSize >> pageShifts;
        // 创建子页数组，大小为chunk中的页面数量
        // chunkSize >> pageShifts 计算一个chunk的总页面数量
        subpages = new PoolSubpage[pages];
        

        // 2. 创建初始运行块句柄
        // 句柄格式：0...0 0000 0000 0000 0000 0000 0000 0000 0000
        //           |<-- 页数(15位) -->|<-- 未使用(32位) -->|
        // ? 初始句柄为什么不是全0的long结构?

        long initHandle = (long) pages << SIZE_SHIFT;
        // 3. 将初始运行块插入可用运行块映射
        insertAvailRun(0, pages, initHandle);

        // 创建ByteBuffer缓存队列，初始容量为8
        // 用于缓存已分配的ByteBuffer对象，减少对象创建
        cachedNioBuffers = new ArrayDeque<ByteBuffer>(8);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunk(PoolArena<T> arena, Object base, T memory, int size) {
        unpooled = true;
        this.arena = arena;
        this.base = base;
        this.memory = memory;
        pageSize = 0;
        pageShifts = 0;
        maxPageIdx = 0;
        runsAvailMap = null;
        runsAvail = null;
        runsAvailLock = null;
        subpages = null;
        chunkSize = size;
        cachedNioBuffers = null;
    }

    private static IntPriorityQueue[] newRunsAvailqueueArray(int size) {
        IntPriorityQueue[] queueArray = new IntPriorityQueue[size];
        for (int i = 0; i < queueArray.length; i++) {
            queueArray[i] = new IntPriorityQueue();
        }
        return queueArray;
    }

    /**
     * 将可用的运行块(run)插入到管理数据结构中
     * <p>
     * 该方法负责将一个新的可用运行块添加到两个关键数据结构中：
     * <ul>
     *   <li>runsAvail - 按大小分类的优先级队列数组</li>
     *   <li>runsAvailMap - 按偏移量索引的哈希映射</li>
     * </ul>
     * </p>
     * 
     * <h3>运行块管理机制</h3>
     * <p>
     * 为了高效地管理和查找运行块，该方法执行以下操作：
     * <ol>
     *   <li>根据运行块的页数确定其在runsAvail数组中的索引位置</li>
     *   <li>将运行块添加到对应大小类别的优先级队列中</li>
     *   <li>在runsAvailMap中记录运行块的首页和尾页（如果有多页）</li>
     * </ol>
     * 这种双重索引机制使得内存分配器能够：
     * <ul>
     *   <li>快速找到特定大小的可用运行块</li>
     *   <li>通过页面偏移量快速定位和合并相邻的运行块</li>
     * </ul>
     * </p>
     *
     * @param runOffset 运行块在chunk中的页面偏移量
     * @param pages 运行块包含的页面数量
     * @param handle 运行块的内存句柄，编码了运行块的关键信息
     * 
     * @see #insertAvailRun0(int, long)
     * @see #lastPage(int, int)
     */
    private void insertAvailRun(int runOffset, int pages, long handle) {
        // 根据页面数量计算适合该运行块的大小类别索引
        // 使用floor确保获取不大于当前页数的最大索引
        int pageIdxFloor = arena.sizeClass.pages2pageIdxFloor(pages);
        
        // 获取对应大小类别的优先级队列
        IntPriorityQueue queue = runsAvail[pageIdxFloor];
        
        // 确保handle表示的是运行块而非子页
        assert isRun(handle);
        
        // 将运行块添加到优先级队列中
        // 注意：这里将64位handle右移32位，只保留高32位信息用于排序
        queue.offer((int) (handle >> BITMAP_IDX_BIT_LENGTH));

        // 在映射表中记录运行块的第一页
        // 这使得我们可以通过页面偏移量快速查找运行块
        insertAvailRun0(runOffset, handle);
        
        if (pages > 1) {
            // 如果运行块有多页，还需要记录最后一页
            // 这对于合并相邻运行块至关重要
            insertAvailRun0(lastPage(runOffset, pages), handle);
        }
    }

    private void insertAvailRun0(int runOffset, long handle) {
        long pre = runsAvailMap.put(runOffset, handle);
        assert pre == -1;
    }

    private void removeAvailRun(long handle) {
        int pageIdxFloor = arena.sizeClass.pages2pageIdxFloor(runPages(handle));
        runsAvail[pageIdxFloor].remove((int) (handle >> BITMAP_IDX_BIT_LENGTH));
        removeAvailRun0(handle);
    }

    private void removeAvailRun0(long handle) {
        int runOffset = runOffset(handle);
        int pages = runPages(handle);
        // remove first page of run
        runsAvailMap.remove(runOffset);
        if (pages > 1) {
            // remove last page of run
            runsAvailMap.remove(lastPage(runOffset, pages));
        }
    }

    private static int lastPage(int runOffset, int pages) {
        return runOffset + pages - 1;
    }

    private long getAvailRunByOffset(int runOffset) {
        return runsAvailMap.get(runOffset);
    }

    @Override
    public int usage() {
        final int freeBytes;
        if (this.unpooled) {
            freeBytes = this.freeBytes;
        } else {
            runsAvailLock.lock();
            try {
                freeBytes = this.freeBytes;
            } finally {
                runsAvailLock.unlock();
            }
        }
        return usage(freeBytes);
    }

    private int usage(int freeBytes) {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    /**
     * 在当前块中为指定的PooledByteBuf分配内存空间。
     * <p>
     * 该方法根据请求大小的不同，采用两种不同的分配策略：
     * <ul>
     *   <li>对于小型内存请求(small)，分配子页(subpage)空间</li>
     *   <li>对于普通内存请求(normal)，分配运行块(run)空间</li>
     * </ul>
     * </p>
     * 
     * <h3>内存分配流程</h3>
     * <p>
     * <b>小型内存分配策略：</b>
     * <ol>
     *   <li>首先尝试从Arena的子页池(smallSubpagePools)中获取可用子页</li>
     *   <li>如果有可用子页，直接在其中分配空间</li>
     *   <li>如果没有可用子页，调用allocateSubpage创建新的子页并分配空间</li>
     * </ol>
     * </p>
     * 
     * <p>
     * <b>普通内存分配策略：</b>
     * <ol>
     *   <li>计算所需的运行块大小(runSize)</li>
     *   <li>调用allocateRun方法分配指定大小的运行块</li>
     * </ol>
     * </p>
     * 
     * <p>
     * 分配成功后，会使用handle(句柄)初始化提供的PooledByteBuf对象。
     * 如果可用，会复用缓存的NIO ByteBuffer以减少对象创建。
     * </p>
     * 
     * <h3>线程安全</h3>
     * <p>
     * 对于小型内存分配，该方法通过对子页池头节点加锁确保线程安全。
     * 普通内存分配依赖于调用allocateRun方法的同步机制。
     * </p>
     *
     * @param buf 要初始化的目标ByteBuf对象
     * @param reqCapacity 请求的容量大小（字节）
     * @param sizeIdx 标准化后的大小索引，由{@link SizeClasses}提供
     * @param cache 线程本地缓存，用于优化内存分配
     * @return 如果分配成功返回true，如果没有足够空间返回false
     * 
     * @see PoolSubpage#allocate()
     * @see #allocateSubpage(int, PoolSubpage)
     * @see #allocateRun(int)
     * @see #initBuf(PooledByteBuf, ByteBuffer, long, int, PoolThreadCache)
     */
    boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache cache) {
        // 声明内存句柄变量，用于存储分配结果
        final long handle;
        
        // 判断请求大小是否为小内存（small或tiny）
        if (sizeIdx <= arena.sizeClass.smallMaxSizeIdx) {
            // 声明子页变量，用于后续可能的子页分配
            final PoolSubpage<T> nextSub;
            
            // 获取对应大小索引的子页池头节点
            // 子页池是按照不同大小组织的双向链表结构
            PoolSubpage<T> head = arena.smallSubpagePools[sizeIdx];
            
            // 对子页池头节点加锁，确保线程安全
            // 因为可能有多个线程同时访问同一个子页池
            head.lock();
            try {
                // 获取子页池中的第一个可用子页
                nextSub = head.next;
                
                // 检查链表是否为空（如果next指向自身，表示链表为空）
                if (nextSub != head) {
                    // 断言验证子页状态正确，确保子页可用且元素大小匹配
                    // doNotDestroy表示子页正在使用中
                    // elemSize必须与请求的大小索引对应的实际大小一致
                    assert nextSub.doNotDestroy && nextSub.elemSize == arena.sizeClass.sizeIdx2size(sizeIdx)
                            : "doNotDestroy=" + nextSub.doNotDestroy + ", elemSize=" + nextSub.elemSize + ", sizeIdx=" +
                                    sizeIdx;
                    
                    // 从找到的子页中分配内存，返回内存句柄
                    handle = nextSub.allocate();
                    
                    // 断言确保分配成功（句柄大于等于0）
                    assert handle >= 0;
                    
                    // 断言确保分配的是子页类型内存
                    assert isSubpage(handle);
                    
                    // 使用分配的子页内存初始化ByteBuf对象
                    // 传递null作为nioBuffer参数，表示不使用缓存的ByteBuffer
                    nextSub.chunk.initBufWithSubpage(buf, null, handle, reqCapacity, cache);
                    
                    // 返回分配成功
                    return true;
                }
                
                // 如果没有可用子页，则创建新的子页并从中分配内存
                handle = allocateSubpage(sizeIdx, head);
                
                // 如果分配失败（返回-1），表示无法创建新子页
                if (handle < 0) {
                    // 返回分配失败
                    return false;
                }
                
                // 断言确保分配的是子页类型内存
                assert isSubpage(handle);
            } finally {
                // 无论成功失败，都要解锁子页池头节点
                head.unlock();
            }
        } else {
            // 处理普通(normal)大小的内存请求，不使用子页
            
            // 计算运行块大小，必须是页大小的整数倍
            int runSize = arena.sizeClass.sizeIdx2size(sizeIdx);
            
            // 分配指定大小的运行块
            handle = allocateRun(runSize);
            
            // 如果分配失败（返回-1），表示无法分配运行块
            if (handle < 0) {
                // 返回分配失败
                return false;
            }
            
            // 断言确保分配的不是子页类型内存
            assert !isSubpage(handle);
        }

        // 从缓存中获取一个ByteBuffer对象（如果可用）
        // 这是一个优化，可以减少ByteBuffer对象的创建
        ByteBuffer nioBuffer = cachedNioBuffers != null ? cachedNioBuffers.pollLast() : null;
        
        // 使用分配的内存（可能是子页或运行块）初始化ByteBuf对象
        initBuf(buf, nioBuffer, handle, reqCapacity, cache);
        
        // 返回分配成功
        return true;
    }

    /**
     * 分配指定大小的运行块(run)
     * <p>
     * 该方法负责从当前chunk中分配一个指定大小的连续内存区域(运行块)。
     * 它会在可用运行块队列中查找第一个足够大的运行块，如果找到的运行块大于请求的大小，
     * 会将其分割成两部分：一部分用于分配，另一部分保留为可用运行块。
     * </p>
     * 
     * <h3>分配策略</h3>
     * <p>
     * 采用"最佳匹配"策略，具体步骤：
     * <ol>
     *   <li>将请求的字节大小转换为页数</li>
     *   <li>找到第一个包含足够大运行块的队列</li>
     *   <li>从该队列中获取偏移量最小的运行块</li>
     *   <li>如果运行块大于所需大小，分割它</li>
     *   <li>更新可用内存计数</li>
     * </ol>
     * </p>
     * 
     * <h3>线程安全</h3>
     * <p>
     * 该方法通过runsAvailLock锁确保线程安全，防止多线程并发分配时的竞态条件。
     * </p>
     * 
     * @param runSize 请求分配的运行块大小(字节)
     * @return 成功分配时返回内存句柄，失败时返回-1
     * 
     * @see #runFirstBestFit(int)
     * @see #splitLargeRun(long, int)
     * @see #removeAvailRun0(long)
     */
    private long allocateRun(int runSize) {
        // 将请求的字节大小转换为页数
        // 右移pageShifts位相当于除以pageSize
        int pages = runSize >> pageShifts;
        
        // 将页数转换为页索引，用于在runsAvail数组中查找合适的队列
        // 这是一个映射操作，将实际页数映射到SizeClasses中定义的标准大小索引
        int pageIdx = arena.sizeClass.pages2pageIdx(pages);

        // 获取锁，确保线程安全
        // 这是必要的，因为可能有多个线程同时尝试分配内存
        runsAvailLock.lock();
        try {
            // 查找第一个包含足够大运行块的队列
            // 从pageIdx开始向上查找，直到找到一个非空队列
            int queueIdx = runFirstBestFit(pageIdx);
            if (queueIdx == -1) {
                // 没有找到足够大的运行块，分配失败
                return -1;
            }

            // 从找到的队列中获取偏移量最小的运行块
            // 这确保了内存分配从低地址向高地址进行，减少内存碎片
            IntPriorityQueue queue = runsAvail[queueIdx];
            long handle = queue.poll();
            
            // 确保获取到有效的句柄
            assert handle != IntPriorityQueue.NO_VALUE;
            
            // 将句柄左移BITMAP_IDX_BIT_LENGTH位，为bitmapIdx字段腾出空间
            // 因为优先队列中存储的是压缩后的句柄（没有包含bitmapIdx）
            handle <<= BITMAP_IDX_BIT_LENGTH;
            
            // 确保获取到的运行块未被使用
            assert !isUsed(handle) : "invalid handle: " + handle;

            // 从可用运行块映射中移除该运行块
            // 这是必要的，因为该运行块即将被分配或分割
            removeAvailRun0(handle);

            // 如果运行块大于所需大小，分割它
            // 这会返回一个新的句柄，表示分配的部分
            handle = splitLargeRun(handle, pages);

            // 计算分配的内存大小并更新可用字节数
            // 这用于跟踪chunk的内存使用情况
            int pinnedSize = runSize(pageShifts, handle);
            freeBytes -= pinnedSize;
            
            // 返回分配的内存句柄
            return handle;
        } finally {
            // 释放锁，允许其他线程进行内存分配
            runsAvailLock.unlock();
        }
    }

    private int calculateRunSize(int sizeIdx) {
        int maxElements = 1 << pageShifts - SizeClasses.LOG2_QUANTUM;
        int runSize = 0;
        int nElements;

        final int elemSize = arena.sizeClass.sizeIdx2size(sizeIdx);

        // find lowest common multiple of pageSize and elemSize
        do {
            runSize += pageSize;
            nElements = runSize / elemSize;
        } while (nElements < maxElements && runSize != nElements * elemSize);

        while (nElements > maxElements) {
            runSize -= pageSize;
            nElements = runSize / elemSize;
        }

        assert nElements > 0;
        assert runSize <= chunkSize;
        assert runSize >= elemSize;

        return runSize;
    }

    private int runFirstBestFit(int pageIdx) {
        if (freeBytes == chunkSize) {
            return arena.sizeClass.nPSizes - 1;
        }
        for (int i = pageIdx; i < arena.sizeClass.nPSizes; i++) {
            IntPriorityQueue queue = runsAvail[i];
            if (queue != null && !queue.isEmpty()) {
                return i;
            }
        }
        return -1;
    }

    private long splitLargeRun(long handle, int needPages) {
        assert needPages > 0;

        int totalPages = runPages(handle);
        assert needPages <= totalPages;

        int remPages = totalPages - needPages;

        if (remPages > 0) {
            int runOffset = runOffset(handle);

            // keep track of trailing unused pages for later use
            int availOffset = runOffset + needPages;
            long availRun = toRunHandle(availOffset, remPages, 0);
            insertAvailRun(availOffset, remPages, availRun);

            // not avail
            return toRunHandle(runOffset, needPages, 1);
        }

        // mark it as used
        handle |= 1L << IS_USED_SHIFT;
        return handle;
    }

    /**
     * 创建新的子页(PoolSubpage)并从中分配内存。
     * <p>
     * 当Arena中没有可用的合适子页时，该方法会被调用来创建一个新的子页。
     * 首先分配一个运行块(run)，然后在其上创建子页，最后从新创建的子页中分配内存。
     * 新创建的子页会被添加到Arena的子页池中，以便后续复用。
     * </p>
     * 
     * <h3>子页大小计算</h3>
     * <p>
     * 子页大小通过calculateRunSize方法计算，确保：
     * <ul>
     *   <li>大小为页大小的整数倍</li>
     *   <li>大小足够容纳多个元素以提高内存利用率</li>
     *   <li>大小尽可能接近页大小的最小倍数</li>
     * </ul>
     * </p>
     * 
     * <h3>子页创建与管理</h3>
     * <p>
     * 创建的子页会：
     * <ul>
     *   <li>被存储在chunk的subpages数组中</li>
     *   <li>通过双向链表与Arena的子页池连接</li>
     *   <li>初始化内部位图用于跟踪内存分配状态</li>
     * </ul>
     * </p>
     *
     * @param sizeIdx 标准化大小的索引值，用于确定元素大小
     * @param head 对应大小的子页池头节点，用于链接新创建的子页
     * @return 成功分配时返回内存句柄，失败时返回-1
     * 
     * @see PoolSubpage#allocate()
     * @see #allocateRun(int)
     * @see #calculateRunSize(int)
     */
    private long allocateSubpage(int sizeIdx, PoolSubpage<T> head) {
        // 计算子页所需的运行块大小
        // 这个大小通常是页大小的整数倍，确保能容纳多个相同大小的元素
        // calculateRunSize会找到pageSize和elemSize的最小公倍数，优化内存利用率
        int runSize = calculateRunSize(sizeIdx);
        
        // 分配一个指定大小的运行块作为子页的基础
        // 这会调用allocateRun方法，从chunk中分配连续内存
        // 返回的句柄已经设置了isUsed=1标志
        long runHandle = allocateRun(runSize);
        if (runHandle < 0) {
            // 如果运行块分配失败（可能是内存不足），子页创建也失败
            // 返回-1表示分配失败
            return -1;
        }

        // 获取运行块在chunk中的页偏移量
        // 这将用作子页在subpages数组中的索引，便于后续快速查找
        int runOffset = runOffset(runHandle);
        
        // 确保该位置没有已存在的子页
        // 这是一个断言检查，防止覆盖已有子页，避免内存泄漏
        assert subpages[runOffset] == null;
        
        // 获取元素大小，用于初始化子页
        // 这决定了子页内每个小内存块的大小，由SizeClasses提供标准化大小
        int elemSize = arena.sizeClass.sizeIdx2size(sizeIdx);

        // 创建新的子页对象
        // 参数包括：
        // - head: 子页池头节点，用于将子页链接到池中
        // - this: 当前chunk，表示子页所属的内存块
        // - pageShifts: 页大小位移值，用于计算内存偏移量
        // - runOffset: 运行块偏移量，子页在chunk中的位置
        // - runSize: 运行块大小，决定子页可用的总内存
        // - elemSize: 元素大小，决定子页内每个小内存块的大小
        PoolSubpage<T> subpage = new PoolSubpage<T>(head, this, pageShifts, runOffset,
                runSize(pageShifts, runHandle), elemSize);

        // 将新创建的子页存储在subpages数组中
        // 这样可以通过runOffset快速找到对应的子页
        // 当释放子页内存时，也需要通过这个数组找到子页对象
        subpages[runOffset] = subpage;
        
        // 从新创建的子页中分配一个元素
        // 这会更新子页的内部位图状态，标记第一个位置为已使用
        // 返回的句柄包含了子页位置和位图索引信息
        return subpage.allocate();
    }

    /**
     * Free a subpage or a run of pages When a subpage is freed from PoolSubpage, it
     * might be added back to subpage pool
     * of the owning PoolArena. If the subpage pool in PoolArena has at least one
     * other PoolSubpage of given elemSize,
     * we can completely free the owning Page so it is available for subsequent
     * allocations
     *
     * @param handle handle to free
     */
    void free(long handle, int normCapacity, ByteBuffer nioBuffer) {
        if (isSubpage(handle)) {
            int sIdx = runOffset(handle);
            PoolSubpage<T> subpage = subpages[sIdx];
            assert subpage != null;
            PoolSubpage<T> head = subpage.chunk.arena.smallSubpagePools[subpage.headIndex];
            // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and
            // synchronize on it.
            // This is need as we may add it back and so alter the linked-list structure.
            head.lock();
            try {
                assert subpage.doNotDestroy;
                if (subpage.free(head, bitmapIdx(handle))) {
                    // the subpage is still used, do not free it
                    return;
                }
                assert !subpage.doNotDestroy;
                // Null out slot in the array as it was freed and we should not use it anymore.
                subpages[sIdx] = null;
            } finally {
                head.unlock();
            }
        }

        int runSize = runSize(pageShifts, handle);
        // start free run
        runsAvailLock.lock();
        try {
            // collapse continuous runs, successfully collapsed runs
            // will be removed from runsAvail and runsAvailMap
            long finalRun = collapseRuns(handle);

            // set run as not used
            finalRun &= ~(1L << IS_USED_SHIFT);
            // if it is a subpage, set it to run
            finalRun &= ~(1L << IS_SUBPAGE_SHIFT);

            insertAvailRun(runOffset(finalRun), runPages(finalRun), finalRun);
            freeBytes += runSize;
        } finally {
            runsAvailLock.unlock();
        }

        if (nioBuffer != null && cachedNioBuffers != null &&
                cachedNioBuffers.size() < PooledByteBufAllocator.DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK) {
            cachedNioBuffers.offer(nioBuffer);
        }
    }

    private long collapseRuns(long handle) {
        return collapseNext(collapsePast(handle));
    }

    private long collapsePast(long handle) {
        for (;;) {
            int runOffset = runOffset(handle);
            int runPages = runPages(handle);

            long pastRun = getAvailRunByOffset(runOffset - 1);
            if (pastRun == -1) {
                return handle;
            }

            int pastOffset = runOffset(pastRun);
            int pastPages = runPages(pastRun);

            // is continuous
            if (pastRun != handle && pastOffset + pastPages == runOffset) {
                // remove past run
                removeAvailRun(pastRun);
                handle = toRunHandle(pastOffset, pastPages + runPages, 0);
            } else {
                return handle;
            }
        }
    }

    private long collapseNext(long handle) {
        for (;;) {
            int runOffset = runOffset(handle);
            int runPages = runPages(handle);

            long nextRun = getAvailRunByOffset(runOffset + runPages);
            if (nextRun == -1) {
                return handle;
            }

            int nextOffset = runOffset(nextRun);
            int nextPages = runPages(nextRun);

            // is continuous
            if (nextRun != handle && runOffset + runPages == nextOffset) {
                // remove next run
                removeAvailRun(nextRun);
                handle = toRunHandle(runOffset, runPages + nextPages, 0);
            } else {
                return handle;
            }
        }
    }

    private static long toRunHandle(int runOffset, int runPages, int inUsed) {
        return (long) runOffset << RUN_OFFSET_SHIFT
                | (long) runPages << SIZE_SHIFT
                | (long) inUsed << IS_USED_SHIFT;
    }

    void initBuf(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
            PoolThreadCache threadCache) {
        if (isSubpage(handle)) {
            initBufWithSubpage(buf, nioBuffer, handle, reqCapacity, threadCache);
        } else {
            int maxLength = runSize(pageShifts, handle);
            buf.init(this, nioBuffer, handle, runOffset(handle) << pageShifts,
                    reqCapacity, maxLength, arena.parent.threadCache());
        }
    }

    /**
     * 使用子页(subpage)内存初始化PooledByteBuf对象。
     * <p>
     * 该方法是小内存分配流程的最后一步，负责将已分配的子页内存与ByteBuf对象关联起来。
     * 通过解析内存句柄(handle)，计算实际内存地址偏移量，然后将相关参数传递给ByteBuf的init方法。
     * </p>
     * 
     * <h3>内存布局</h3>
     * <p>
     * 子页内存在物理上的位置由两部分计算得出：
     * <ul>
     *   <li>运行块偏移量(runOffset) - 确定子页在chunk中的页位置</li>
     *   <li>位图索引(bitmapIdx) - 确定在子页内部的具体位置</li>
     * </ul>
     * 最终内存偏移量 = (runOffset << pageShifts) + bitmapIdx * elemSize
     * </p>
     * 
     * <h3>参数验证</h3>
     * <p>
     * 方法会验证：
     * <ul>
     *   <li>子页必须处于活跃状态(doNotDestroy为true)</li>
     *   <li>请求容量不能超过子页元素大小</li>
     * </ul>
     * </p>
     *
     * @param buf 需要初始化的PooledByteBuf对象
     * @param nioBuffer 可选的缓存NIO ByteBuffer，用于减少对象创建
     * @param handle 内存句柄，包含子页分配信息
     * @param reqCapacity 请求的容量大小（字节）
     * @param threadCache 线程本地缓存，用于优化后续内存操作
     * 
     * @see PooledByteBuf#init(PoolChunk, ByteBuffer, long, int, int, int, PoolThreadCache)
     */
    void initBufWithSubpage(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
            PoolThreadCache threadCache) {
        int runOffset = runOffset(handle);
        int bitmapIdx = bitmapIdx(handle);

        PoolSubpage<T> s = subpages[runOffset];
        assert s.isDoNotDestroy();
        assert reqCapacity <= s.elemSize : reqCapacity + "<=" + s.elemSize;

        int offset = (runOffset << pageShifts) + bitmapIdx * s.elemSize;
        buf.init(this, nioBuffer, handle, offset, reqCapacity, s.elemSize, threadCache);
    }

    void incrementPinnedMemory(int delta) {
        assert delta > 0;
        pinnedBytes.add(delta);
    }

    void decrementPinnedMemory(int delta) {
        assert delta > 0;
        pinnedBytes.add(-delta);
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public int freeBytes() {
        if (this.unpooled) {
            return freeBytes;
        }
        runsAvailLock.lock();
        try {
            return freeBytes;
        } finally {
            runsAvailLock.unlock();
        }
    }

    public int pinnedBytes() {
        return (int) pinnedBytes.value();
    }

    @Override
    public String toString() {
        final int freeBytes;
        if (this.unpooled) {
            freeBytes = this.freeBytes;
        } else {
            runsAvailLock.lock();
            try {
                freeBytes = this.freeBytes;
            } finally {
                runsAvailLock.unlock();
            }
        }

        return new StringBuilder()
                .append("Chunk(")
                .append(Integer.toHexString(System.identityHashCode(this)))
                .append(": ")
                .append(usage(freeBytes))
                .append("%, ")
                .append(chunkSize - freeBytes)
                .append('/')
                .append(chunkSize)
                .append(')')
                .toString();
    }

    void destroy() {
        arena.destroyChunk(this);
    }

    static int runOffset(long handle) {
        return (int) (handle >> RUN_OFFSET_SHIFT);
    }

    static int runSize(int pageShifts, long handle) {
        return runPages(handle) << pageShifts;
    }

    static int runPages(long handle) {
        return (int) (handle >> SIZE_SHIFT & 0x7fff);
    }

    static boolean isUsed(long handle) {
        return (handle >> IS_USED_SHIFT & 1) == 1L;
    }

    static boolean isRun(long handle) {
        return !isSubpage(handle);
    }

    static boolean isSubpage(long handle) {
        return (handle >> IS_SUBPAGE_SHIFT & 1) == 1L;
    }

    static int bitmapIdx(long handle) {
        return (int) handle;
    }
}
