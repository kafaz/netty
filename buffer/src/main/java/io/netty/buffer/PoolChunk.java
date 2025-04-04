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

    @SuppressWarnings("unchecked")
    PoolChunk(PoolArena<T> arena, Object base, T memory, int pageSize, int pageShifts, int chunkSize, int maxPageIdx) {
        unpooled = false;
        this.arena = arena;
        this.base = base;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        this.maxPageIdx = maxPageIdx;
        freeBytes = chunkSize;

        runsAvail = newRunsAvailqueueArray(maxPageIdx);
        runsAvailLock = new ReentrantLock();
        runsAvailMap = new LongLongHashMap(-1);
        subpages = new PoolSubpage[chunkSize >> pageShifts];

        // insert initial run, offset = 0, pages = chunkSize / pageSize
        int pages = chunkSize >> pageShifts;
        long initHandle = (long) pages << SIZE_SHIFT;
        insertAvailRun(0, pages, initHandle);

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

    private void insertAvailRun(int runOffset, int pages, long handle) {
        int pageIdxFloor = arena.sizeClass.pages2pageIdxFloor(pages);
        IntPriorityQueue queue = runsAvail[pageIdxFloor];
        assert isRun(handle);
        queue.offer((int) (handle >> BITMAP_IDX_BIT_LENGTH));

        // insert first page of run
        insertAvailRun0(runOffset, handle);
        if (pages > 1) {
            // insert last page of run
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

    boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache cache) {
        final long handle;
        if (sizeIdx <= arena.sizeClass.smallMaxSizeIdx) {
            final PoolSubpage<T> nextSub;
            // small
            // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and
            // synchronize on it.
            // This is need as we may add it back and so alter the linked-list structure.
            PoolSubpage<T> head = arena.smallSubpagePools[sizeIdx];
            head.lock();
            try {
                nextSub = head.next;
                if (nextSub != head) {
                    assert nextSub.doNotDestroy && nextSub.elemSize == arena.sizeClass.sizeIdx2size(sizeIdx)
                            : "doNotDestroy=" + nextSub.doNotDestroy + ", elemSize=" + nextSub.elemSize + ", sizeIdx=" +
                                    sizeIdx;
                    handle = nextSub.allocate();
                    assert handle >= 0;
                    assert isSubpage(handle);
                    nextSub.chunk.initBufWithSubpage(buf, null, handle, reqCapacity, cache);
                    return true;
                }
                handle = allocateSubpage(sizeIdx, head);
                if (handle < 0) {
                    return false;
                }
                assert isSubpage(handle);
            } finally {
                head.unlock();
            }
        } else {
            // normal
            // runSize must be multiple of pageSize
            int runSize = arena.sizeClass.sizeIdx2size(sizeIdx);
            handle = allocateRun(runSize);
            if (handle < 0) {
                return false;
            }
            assert !isSubpage(handle);
        }

        ByteBuffer nioBuffer = cachedNioBuffers != null ? cachedNioBuffers.pollLast() : null;
        initBuf(buf, nioBuffer, handle, reqCapacity, cache);
        return true;
    }

    private long allocateRun(int runSize) {
        int pages = runSize >> pageShifts;
        int pageIdx = arena.sizeClass.pages2pageIdx(pages);

        runsAvailLock.lock();
        try {
            // find first queue which has at least one big enough run
            int queueIdx = runFirstBestFit(pageIdx);
            if (queueIdx == -1) {
                return -1;
            }

            // get run with min offset in this queue
            IntPriorityQueue queue = runsAvail[queueIdx];
            long handle = queue.poll();
            assert handle != IntPriorityQueue.NO_VALUE;
            handle <<= BITMAP_IDX_BIT_LENGTH;
            assert !isUsed(handle) : "invalid handle: " + handle;

            removeAvailRun0(handle);

            handle = splitLargeRun(handle, pages);

            int pinnedSize = runSize(pageShifts, handle);
            freeBytes -= pinnedSize;
            return handle;
        } finally {
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
     * Create / initialize a new PoolSubpage of normCapacity. Any PoolSubpage
     * created / initialized here is added to
     * subpage pool in the PoolArena that owns this PoolChunk.
     *
     * @param sizeIdx sizeIdx of normalized size
     * @param head    head of subpages
     *
     * @return index in memoryMap
     */
    private long allocateSubpage(int sizeIdx, PoolSubpage<T> head) {
        // allocate a new run
        int runSize = calculateRunSize(sizeIdx);
        // runSize must be multiples of pageSize
        long runHandle = allocateRun(runSize);
        if (runHandle < 0) {
            return -1;
        }

        int runOffset = runOffset(runHandle);
        assert subpages[runOffset] == null;
        int elemSize = arena.sizeClass.sizeIdx2size(sizeIdx);

        PoolSubpage<T> subpage = new PoolSubpage<T>(head, this, pageShifts, runOffset,
                runSize(pageShifts, runHandle), elemSize);

        subpages[runOffset] = subpage;
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
