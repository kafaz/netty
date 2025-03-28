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

/**
 * PoolChunk 内存分配算法说明
 * 
 * <p>PoolChunk 是 Netty 内存池的核心组件，负责管理大块内存的分配和回收。
 * 它通过分层的设计实现了高效的内存管理，包括页（Page）、运行（Run）和块（Chunk）三个层次。</p>
 *
 * <h3>基本概念</h3>
 * <ul>
 *   <li>页（Page）：内存块中最小的可分配单位</li>
 *   <li>运行（Run）：由多个连续的页组成</li>
 *   <li>块（Chunk）：由多个运行组成，大小 = maxPages * pageSize</li>
 * </ul>
 *
 * <h3>内存布局</h3>
 * <pre>
 * /-----------------\
 * | run            |
 * |                |
 * |                |
 * |-----------------|
 * | run            |
 * |                |
 * |-----------------|
 * | unalloctated   |
 * | (freed)        |
 * |                |
 * |-----------------|
 * | subpage        |
 * |-----------------|
 * | unallocated    |
 * | (freed)        |
 * | ...            |
 * | ...            |
 * | ...            |
 * |                |
 * |                |
 * |                |
 * \-----------------/
 * </pre>
 *
 * <h3>句柄（Handle）结构</h3>
 * <p>句柄是一个长整型数，用于编码内存分配信息，其位布局如下：</p>
 * <pre>
 * oooooooo ooooooos ssssssss ssssssue bbbbbbbb bbbbbbbb bbbbbbbb bbbbbbbb
 * </pre>
 * <ul>
 *   <li>o: runOffset（在块中的页偏移量），15位</li>
 *   <li>s: size（此运行的页数），15位</li>
 *   <li>u: isUsed（是否已使用），1位</li>
 *   <li>e: isSubpage（是否是子页），1位</li>
 *   <li>b: bitmapIdx（子页的位图索引，如果不是子页则为0），32位</li>
 * </ul>
 *
 * <h3>核心数据结构</h3>
 * <ul>
 *   <li>runsAvailMap：管理所有运行（已使用和未使用）的映射
 *       <ul>
 *           <li>key: runOffset（运行偏移量）</li>
 *           <li>value: handle（句柄）</li>
 *       </ul>
 *   </li>
 *   <li>runsAvail：优先级队列数组，每个队列管理相同大小的运行
 *       <ul>
 *           <li>按偏移量排序，优先分配偏移量小的运行</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>主要算法</h3>
 * <ol>
 *   <li>初始化算法
 *       <ul>
 *           <li>初始运行：整个块</li>
 *           <li>runOffset = 0</li>
 *           <li>size = chunkSize</li>
 *           <li>isUsed = false</li>
 *           <li>isSubpage = false</li>
 *           <li>bitmapIdx = 0</li>
 *       </ul>
 *   </li>
 *   <li>分配运行算法 [allocateRun(size)]
 *       <ul>
 *           <li>根据大小在 runsAvails 中查找第一个可用运行</li>
 *           <li>如果运行页数大于请求页数，则分割并保存尾部运行供后续使用</li>
 *       </ul>
 *   </li>
 *   <li>分配子页算法 [allocateSubpage(size)]
 *       <ul>
 *           <li>根据大小查找未满的子页</li>
 *           <li>如果存在则直接返回，否则分配新的 PoolSubpage 并初始化</li>
 *           <li>注意：初始化时子页对象会被添加到 PoolArena 的 subpagesPool</li>
 *           <li>调用 subpage.allocate() 进行分配</li>
 *       </ul>
 *   </li>
 *   <li>释放算法 [free(handle, length, nioBuffer)]
 *       <ul>
 *           <li>如果是子页，将内存块返回到子页</li>
 *           <li>如果子页未使用或是运行，则开始释放此运行</li>
 *           <li>合并连续的可用运行</li>
 *           <li>保存合并后的运行</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <h3>应用场景</h3>
 * <ul>
 *   <li>高性能网络编程中的内存管理</li>
 *   <li>需要频繁分配和释放内存的场景</li>
 *   <li>对内存使用效率要求高的系统</li>
 *   <li>需要减少 GC 压力的应用</li>
 * </ul>
 *
 * <h3>性能优化</h3>
 * <ul>
 *   <li>通过位图管理提高内存分配效率</li>
 *   <li>使用优先级队列优化内存查找</li>
 *   <li>支持内存合并减少碎片</li>
 *   <li>分层设计提高内存利用率</li>
 * </ul>
 *
 * @see PoolArena
 * @see PoolSubpage
 * @see SizeClasses
 */
final class PoolChunk<T> implements PoolChunkMetric {
    // 用于位操作的常量，定义内存管理中各种标志位的长度
    private static final int SIZE_BIT_LENGTH = 15; // 大小字段的位长度
    private static final int INUSED_BIT_LENGTH = 1; // 使用标志的位长度
    private static final int SUBPAGE_BIT_LENGTH = 1; // 子页标志的位长度
    private static final int BITMAP_IDX_BIT_LENGTH = 32; // 位图索引的位长度

    // 位移常量，用于构建和解析内存句柄
    static final int IS_SUBPAGE_SHIFT = BITMAP_IDX_BIT_LENGTH; // 子页标志的位移
    static final int IS_USED_SHIFT = SUBPAGE_BIT_LENGTH + IS_SUBPAGE_SHIFT; // 使用标志的位移
    static final int SIZE_SHIFT = INUSED_BIT_LENGTH + IS_USED_SHIFT; // 大小字段的位移
    static final int RUN_OFFSET_SHIFT = SIZE_BIT_LENGTH + SIZE_SHIFT; // 运行偏移量的位移

    final PoolArena<T> arena; // 所属的内存竞技场
    final Object base; // 基础对象，用于内存访问
    final T memory; // 实际内存对象
    final boolean unpooled; // 是否不使用池化
    final int pageSize; // 页面大小
    final int pageShifts; // 页大小的位移值，用于地址计算
    final int chunkSize; // 块大小
    final int maxPageIdx; // 最大页索引
    int freeBytes; // 可用字节数

    // 存储每个可用运行的第一页和最后一页
    private final LongLongHashMap runsAvailMap;

    // 管理所有可用的运行区域
    private final IntPriorityQueue[] runsAvail;

    // 线程安全锁
    private final ReentrantLock runsAvailLock;

    // 管理块中的所有子页
    private final PoolSubpage<T>[] subpages;

    /**
     * Accounting of pinned memory – memory that is currently in use by ByteBuf
     * instances.
     */
    private final LongCounter pinnedBytes = PlatformDependent.newLongCounter();

    /**
     * 用作从内存创建的 ByteBuffer 的缓存。这些只是副本，因此只是
     * 围绕内存本身的容器。这些在 Pooled*ByteBuf 内的操作中经常需要，
     * 所以可能产生额外的垃圾回收，通过缓存这些副本可以大大减少垃圾回收。
     *
     * 如果 PoolChunk 是非池化的，这个字段可能为 null，因为在这种情况下
     * 池化 ByteBuffer 实例没有任何意义。
     */
    private final Deque<ByteBuffer> cachedNioBuffers;

    // 指向父级块列表的引用
    PoolChunkList<T> parent;
    // 链表前一个块的引用
    PoolChunk<T> prev;
    // 链表后一个块的引用
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
