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

    /**
     * PoolArena类核心属性详解
     * <p>
     * PoolArena是Netty内存池系统的核心组件，负责内存的分配、管理和回收。
     * 以下是各属性的详细解释及其在Netty内存分配中的作用。
     * </p>
     */

    /**
     * 内存大小分类枚举
     * <p>
     * 用于区分不同大小类别的内存分配策略
     * </p>
     */
    enum SizeClass {
        Small, // 小内存块，通常<=4KB，使用subpage级别分配
        Normal // 普通内存块，通常4KB-16MB，使用页级别分配
    }

    /**
     * 分配器父对象
     * <p>
     * 指向创建此Arena的PooledByteBufAllocator实例
     * 用于访问全局配置和获取线程缓存
     * </p>
     */
    final PooledByteBufAllocator parent;

    /**
     * 小对象内存页池数组
     * <p>
     * 存储不同大小规格的小内存页池
     * 每个元素是一个双向链表结构，用于快速分配和回收小内存块
     * 在高并发场景下，减少内存碎片和提高分配效率
     * </p>
     */
    final PoolSubpage<T>[] smallSubpagePools;

    /**
     * 不同使用率的内存块列表
     * <p>
     * PoolChunkList按内存块使用率分类管理，构成多级链表结构：
     * q050: 使用率50%-100%的内存块
     * q025: 使用率25%-75%的内存块
     * q000: 使用率1%-50%的内存块
     * qInit: 新创建的内存块，使用率最低
     * q075: 使用率75%-100%的内存块
     * q100: 使用率100%的内存块，已完全分配
     * </p>
     * <p>
     * 这种分级管理机制使Netty能够优先分配使用率较高的内存块，
     * 提高内存利用率，减少碎片，并根据使用率动态调整内存块的位置
     * </p>
     */
    private final PoolChunkList<T> q050;
    private final PoolChunkList<T> q025;
    private final PoolChunkList<T> q000;
    private final PoolChunkList<T> qInit;
    private final PoolChunkList<T> q075;
    private final PoolChunkList<T> q100;

    /**
     * 内存块列表指标集合
     * <p>
     * 提供只读的内存块列表统计信息
     * 用于监控和调试内存使用情况
     * </p>
     */
    private final List<PoolChunkListMetric> chunkListMetrics;

    /**
     * 分配和释放统计计数器
     * <p>
     * allocationsNormal: 普通内存分配次数
     * allocationsSmall: 小内存分配次数，使用LongCounter保证线程安全
     * allocationsHuge: 大内存分配次数，使用LongCounter保证线程安全
     * activeBytesHuge: 当前活跃的大内存字节总数
     * deallocationsSmall: 小内存释放次数
     * deallocationsNormal: 普通内存释放次数
     * deallocationsHuge: 大内存释放次数，使用LongCounter保证线程安全
     * </p>
     * <p>
     * 这些计数器用于：
     * 1. 监控内存分配和释放的频率
     * 2. 检测内存泄漏
     * 3. 优化内存分配策略
     * 4. 提供运行时指标
     * </p>
     */
    private long allocationsNormal;
    private final LongCounter allocationsSmall = PlatformDependent.newLongCounter();
    private final LongCounter allocationsHuge = PlatformDependent.newLongCounter();
    private final LongCounter activeBytesHuge = PlatformDependent.newLongCounter();
    private long deallocationsSmall;
    private long deallocationsNormal;
    private final LongCounter deallocationsHuge = PlatformDependent.newLongCounter();

    /**
     * 线程缓存计数器
     * <p>
     * 记录使用此Arena的线程缓存数量
     * 用于负载均衡和资源管理
     * </p>
     */
    final AtomicInteger numThreadCaches = new AtomicInteger();

    /**
     * 内存访问同步锁
     * <p>
     * 保护Arena内共享数据的并发访问
     * 用于同步内存分配和释放操作
     * </p>
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 内存大小类别配置
     * <p>
     * 管理内存规格大小的映射关系
     * 包含页大小、块大小、对齐要求等配置
     * 是Netty内存池系统的核心配置类
     * </p>
     */
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

    /**
     * 分配小内存块的核心方法。小内存指小于等于一个页大小（通常为8KB）的内存请求。
     * <p>
     * 此方法实现了Netty内存池的三级分配策略，优先级从高到低依次为：
     * <ol>
     * <li>线程本地缓存（Thread Local Cache）分配 - 无锁，最快</li>
     * <li>共享子页（Shared Subpage）分配 - 轻量级锁，较快</li>
     * <li>新页分配（New Page Allocation）- 全局锁，较慢</li>
     * </ol>
     * </p>
     * 
     * <h3>分配流程详解：</h3>
     * <ol>
     * <li><b>线程缓存尝试</b>：首先尝试从当前线程的本地缓存中分配，这是无锁操作，性能最佳</li>
     * <li><b>子页池分配</b>：如果线程缓存未命中，则尝试从共享子页池中分配
     * <ul>
     * <li>使用细粒度锁保护特定大小类别的子页链表</li>
     * <li>检查链表是否有可用子页</li>
     * <li>如有可用子页，从中分配内存切片并初始化ByteBuf</li>
     * </ul>
     * </li>
     * <li><b>正常分配</b>：如果子页池为空，则需要分配新的页面并进行子页划分
     * <ul>
     * <li>需要获取Arena全局锁</li>
     * <li>调用allocateNormal分配新的完整页面</li>
     * <li>将页面划分为多个子页并加入子页池</li>
     * </ul>
     * </li>
     * <li><b>统计更新</b>：完成分配后，更新小内存分配统计计数</li>
     * </ol>
     * 
     * <h3>锁机制说明：</h3>
     * <ul>
     * <li>使用细粒度锁设计，对每个大小类别的子页链表使用独立的锁</li>
     * <li>只有在需要分配新页面时才获取Arena全局锁</li>
     * <li>这种分层锁设计显著减少了线程竞争，提高了并发性能</li>
     * </ul>
     * 
     * <h3>性能优化考虑：</h3>
     * <ul>
     * <li>优先使用线程本地缓存避免同步开销</li>
     * <li>使用细粒度锁减少锁竞争</li>
     * <li>通过sizeIdx快速定位到特定大小的子页池</li>
     * <li>内存复用减少内存分配和GC压力</li>
     * </ul>
     * 
     * @param cache       当前线程的缓存，用于快速分配和回收内存
     * @param buf         待初始化的ByteBuf对象，分配的内存将绑定到此对象
     * @param reqCapacity 请求的内存容量（字节数）
     * @param sizeIdx     标准化后的大小类别索引，用于定位子页池和确定实际分配大小
     * 
     * @see PoolThreadCache#allocateSmall(PoolArena, PooledByteBuf, int, int)
     * @see PoolSubpage
     * @see #allocateNormal(PooledByteBuf, int, int, PoolThreadCache)
     */
    private void tcacheAllocateSmall(PoolThreadCache cache, PooledByteBuf<T> buf,
            final int reqCapacity, final int sizeIdx) {

        // 第一阶段：线程本地缓存分配
        // 尝试从线程本地缓存中分配内存，这是最快的路径，无需任何同步
        // cache.allocateSmall会返回true表示成功，此时buf已被初始化
        if (cache.allocateSmall(this, buf, reqCapacity, sizeIdx)) {
            return; // 缓存分配成功，直接返回，避免后续更昂贵的分配过程
        }

        // 第二阶段：共享子页池分配
        // 获取对应大小类别的子页池头节点，smallSubpagePools是按不同大小索引组织的子页池数组
        final PoolSubpage<T> head = smallSubpagePools[sizeIdx];
        // 声明变量标记是否需要进行更昂贵的正常分配
        final boolean needsNormalAllocation;

        // 对子页池头节点加锁，这是一个细粒度锁，只锁定特定大小类别的子页池
        // 减少锁竞争，提高并发性能
        head.lock();
        try {
            // 获取链表中第一个可用子页（如果有）
            final PoolSubpage<T> s = head.next;
            // 检查链表是否为空（当next指向head自身时表示链表为空）
            needsNormalAllocation = s == head;

            // 如果子页池不为空，尝试从现有子页分配
            if (!needsNormalAllocation) {
                // 断言验证子页状态正确：
                // 1. 子页标记为不可销毁(正在使用中)
                // 2. 子页的元素大小与当前请求的大小类别匹配
                assert s.doNotDestroy && s.elemSize == sizeClass.sizeIdx2size(sizeIdx)
                        : "doNotDestroy=" + s.doNotDestroy +
                                ", elemSize=" + s.elemSize +
                                ", sizeIdx=" + sizeIdx;

                // 从子页中分配内存，返回内存句柄
                // 内存句柄是一个64位长整型，编码了内存位置信息
                long handle = s.allocate();
                // 确保分配成功（handle >= 0）
                assert handle >= 0;

                // 使用分配的内存句柄初始化ByteBuf对象
                // 此方法设置buf的内存引用、基址、容量等属性
                s.chunk.initBufWithSubpage(buf, null, handle, reqCapacity, cache);
            }
        } finally {
            // 确保无论成功失败都释放子页池的锁
            head.unlock();
        }

        // 第三阶段：正常分配（如需要）
        // 如果子页池为空，需要创建新的子页
        if (needsNormalAllocation) {
            // 获取Arena全局锁，这是一个更重的锁，但使用频率较低
            lock();
            try {
                // 执行正常分配逻辑：
                // 1. 分配一个新的页或页集合
                // 2. 创建新的子页并加入子页池
                // 3. 从新创建的子页中分配内存
                allocateNormal(buf, reqCapacity, sizeIdx, cache);
            } finally {
                // 确保释放Arena全局锁
                unlock();
            }
        }

        // 第四阶段：统计更新
        // 增加小内存分配计数，用于监控和性能分析
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

    /**
     * 在内存竞技场中分配正常大小的内存缓冲区。
     * <p>
     * 该方法按照特定的顺序遍历内存块列表，尝试在现有内存块中分配请求的内存。
     * 如果所有现有内存块都无法满足请求，则创建新的内存块并添加到初始化列表中。
     * </p>
     * 
     * <h3>内存块列表查找顺序</h3>
     * <p>
     * 查找顺序经过精心设计，旨在平衡内存利用率和分配效率：
     * <ol>
     * <li>q050（50% 已用）- 首选，平衡内存利用率</li>
     * <li>q025（25% 已用）- 第二选择，保留部分内存</li>
     * <li>q000（接近空闲）- 第三选择，优先使用已分配块</li>
     * <li>qInit（初始化列表）- 第四选择，新创建但尚未分类的块</li>
     * <li>q075（75% 已用）- 最后选择，高利用率块留作小分配</li>
     * </ol>
     * 这种顺序有助于确保内存利用率保持在合理水平，避免过度分散或过度集中。
     * </p>
     * 
     * <h3>新块分配策略</h3>
     * <p>
     * 只有在所有现有块都无法满足请求时才创建新块，这有助于：
     * <ul>
     * <li>最小化内存碎片</li>
     * <li>减少内存消耗</li>
     * <li>提高缓存效率</li>
     * </ul>
     * </p>
     * 
     * @param buf         要初始化的ByteBuf对象
     * @param reqCapacity 请求的容量（字节）
     * @param sizeIdx     规范化大小的索引值
     * @param threadCache 线程本地缓存，用于进一步优化
     */
    private void allocateNormal(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache threadCache) {
        // 确保当前线程持有锁，防止并发修改内存块列表
        assert lock.isHeldByCurrentThread();

        // 按照优化的顺序尝试在现有内存块中分配内存
        // 首先尝试使用利用率约为50%的块（最佳平衡点）
        if (q050.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
        // 然后尝试使用利用率约为25%的块（较低利用率但非空）
                q025.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
                // 然后尝试使用接近空的块（保留少量内存的块）
                q000.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
                // 然后尝试使用初始化列表中的块（新创建但尚未分类的块）
                qInit.allocate(buf, reqCapacity, sizeIdx, threadCache) ||
                // 最后尝试使用利用率约为75%的块（几乎已满的块）
                q075.allocate(buf, reqCapacity, sizeIdx, threadCache)) {
            // 成功分配，直接返回
            return;
        }

        // 所有现有内存块都无法满足请求，需要创建新的内存块

        // 创建新的内存块，参数包括页面大小、页面索引数、页面位移值和块大小
        PoolChunk<T> c = newChunk(sizeClass.pageSize, sizeClass.nPSizes, sizeClass.pageShifts, sizeClass.chunkSize);

        // 在新创建的块中分配内存
        boolean success = c.allocate(buf, reqCapacity, sizeIdx, threadCache);

        // 断言确保分配成功（新块应该总是能满足请求）
        assert success;

        // 将新块添加到初始化列表中，等待后续根据使用率重新分类
        qInit.add(c);
    }

    private void incSmallAllocation() {
        allocationsSmall.increment();
    }

    /**
     * 分配超大内存块的方法
     * 
     * @param buf         需要初始化的缓冲区
     * @param reqCapacity 请求的内存容量
     */
    private void allocateHuge(PooledByteBuf<T> buf, int reqCapacity) {
        PoolChunk<T> chunk = newUnpooledChunk(reqCapacity); // 步骤1: 创建非池化内存块
        activeBytesHuge.add(chunk.chunkSize()); // 步骤2: 更新内存使用统计
        buf.initUnpooled(chunk, reqCapacity); // 步骤3: 初始化缓冲区
        allocationsHuge.increment(); // 步骤4: 更新分配计数器
    }

    /**
     * 释放内存块回池中或销毁非池化内存。此方法是Netty内存池系统中内存释放的核心入口。
     * <p>
     * 当ByteBuf不再使用时，此方法负责将其占用的内存返还给内存池或释放给系统。
     * 方法会根据内存块的类型(池化或非池化)、大小类别和线程缓存配置采取不同的处理策略。
     * </p>
     * 
     * <h3>重要变量说明：</h3>
     * <ul>
     * <li><b>chunk</b> - 要释放的内存块，包含实际的内存数据和元数据</li>
     * <li><b>nioBuffer</b> - 与内存块关联的临时NIO ByteBuffer，用于I/O操作</li>
     * <li><b>handle</b> - 内存块中的句柄值，用于定位要释放的具体内存部分</li>
     * <li><b>normCapacity</b> - 标准化后的容量大小，与内存分配时使用的规格匹配</li>
     * <li><b>cache</b> - 线程本地缓存，用于缓存释放的内存块以加速后续分配</li>
     * </ul>
     * 
     * <h3>释放策略：</h3>
     * <ol>
     * <li><b>非池化内存(unpooled)</b>：直接销毁内存块，更新统计计数器</li>
     * <li><b>池化内存</b>：
     * <ul>
     * <li>尝试将内存块添加到线程本地缓存(如果启用)</li>
     * <li>如果缓存失败或未启用缓存，则释放回内存池</li>
     * </ul>
     * </li>
     * </ol>
     * 
     * <h3>性能考虑：</h3>
     * <ul>
     * <li>对于频繁分配和释放的小内存块，使用线程本地缓存可显著提高性能</li>
     * <li>大内存块直接释放，避免占用缓存空间</li>
     * <li>方法内部处理了对统计计数器的更新，用于监控内存使用情况</li>
     * </ul>
     *
     * @param chunk        要释放的内存块
     * @param nioBuffer    与内存块关联的NIO ByteBuffer，可能为null
     * @param handle       内存块中的句柄，用于定位具体的内存部分
     * @param normCapacity 标准化的容量大小
     * @param cache        线程本地缓存，若为null则不使用缓存
     * 
     * @see PoolChunk#unpooled
     * @see PoolThreadCache#add(PoolArena, PoolChunk, ByteBuffer, long, int,
     *      PoolArena.SizeClass)
     * @see #freeChunk(PoolChunk, long, int, SizeClass, ByteBuffer, boolean)
     */
    void free(PoolChunk<T> chunk, ByteBuffer nioBuffer, long handle, int normCapacity, PoolThreadCache cache) {
        // 减少内存块的固定内存计数，表示此内存不再被引用
        chunk.decrementPinnedMemory(normCapacity);

        // 判断是否为非池化内存块
        if (chunk.unpooled) {
            // 获取内存块大小
            int size = chunk.chunkSize();
            // 销毁非池化内存块
            destroyChunk(chunk);
            // 更新活跃的大内存字节计数
            activeBytesHuge.add(-size);
            // 增加大内存释放计数
            deallocationsHuge.increment();
        } else {
            // 池化内存块的处理
            // 确定内存大小类别(小内存或普通内存)
            SizeClass sizeClass = sizeClass(handle);

            // 尝试将内存添加到线程本地缓存
            if (cache != null && cache.add(this, chunk, nioBuffer, handle, normCapacity, sizeClass)) {
                // 缓存成功，不需要进一步释放
                return;
            }

            // 缓存失败或未使用缓存，释放内存块回池中
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

    /**
     * 重新分配PooledByteBuf的内存。当缓冲区需要调整容量时，此方法会分配新的内存块，复制原有数据，并释放旧内存。
     * <p>
     * 此方法是Netty内存池系统中ByteBuf扩容和缩容的核心实现。它通过以下步骤完成内存重分配：
     * <ol>
     * <li>保存原缓冲区的状态和内存信息</li>
     * <li>从内存池分配新的内存块</li>
     * <li>复制原有数据到新内存</li>
     * <li>释放原有内存回池中</li>
     * </ol>
     * </p>
     * <p>
     * 为确保多线程环境下的安全性，方法在操作过程中会对ByteBuf对象进行同步，防止内存状态被并发修改导致损坏。
     * </p>
     *
     * <h3>重要变量说明：</h3>
     * <ul>
     * <li><b>oldCapacity</b> - 缓冲区原始容量，即旧内存块的有效字节数</li>
     * <li><b>oldChunk</b> - 原始内存块，包含旧的内存分配</li>
     * <li><b>oldNioBuffer</b> - 与旧内存块关联的临时NIO ByteBuffer，用于I/O操作</li>
     * <li><b>oldHandle</b> - 在旧内存块中的句柄值，用于定位和释放内存</li>
     * <li><b>oldMemory</b> - 旧的内存引用，可能是byte[]或ByteBuffer类型</li>
     * <li><b>oldOffset</b> - 旧内存的起始偏移量，表示有效数据的开始位置</li>
     * <li><b>oldMaxLength</b> - 旧内存块的最大可用长度，通常大于等于实际容量</li>
     * <li><b>oldCache</b> - 原有的线程本地缓存，用于加速内存分配</li>
     * <li><b>bytesToCopy</b> - 需要从旧内存复制到新内存的字节数，取决于新旧容量的大小关系</li>
     * </ul>
     *
     * @param buf         需要重新分配内存的PooledByteBuf
     * @param newCapacity 请求的新容量大小（字节数）
     *
     * @throws IllegalArgumentException 如果newCapacity为负数或超过buf的最大容量
     * @throws OutOfMemoryError         如果内存分配失败
     * 
     * @see PooledByteBuf#capacity(int)
     * @see #allocate(PoolThreadCache, PooledByteBuf, int)
     * @see #free(PoolChunk, ByteBuffer, long, int, PoolThreadCache)
     */
    void reallocate(PooledByteBuf<T> buf, int newCapacity) {
        // 确保新容量在有效范围内
        assert newCapacity >= 0 && newCapacity <= buf.maxCapacity();

        // 用于保存原有内存状态的变量
        final int oldCapacity; // 原始容量
        final PoolChunk<T> oldChunk; // 原始内存块
        final ByteBuffer oldNioBuffer; // 原始NIO缓冲区
        final long oldHandle; // 原始内存句柄
        final T oldMemory; // 原始内存引用
        final int oldOffset; // 原始内存偏移量
        final int oldMaxLength; // 原始最大可用长度
        final PoolThreadCache oldCache; // 原始线程缓存

        // 对ByteBuf对象进行同步，确保在多线程环境下的内存操作安全
        // 这里不使用Lock而是synchronized，是为了减少每个ByteBuf的开销
        // 阻塞时间相对较短，不会对Loom造成问题
        synchronized (buf) {
            oldCapacity = buf.length; // 获取当前容量
            if (oldCapacity == newCapacity) { // 如果容量相同，无需重新分配
                return;
            }

            // 保存当前ByteBuf的所有相关状态
            oldChunk = buf.chunk; // 当前内存块
            oldNioBuffer = buf.tmpNioBuf; // 当前NIO缓冲区
            oldHandle = buf.handle; // 当前内存句柄
            oldMemory = buf.memory; // 当前内存引用
            oldOffset = buf.offset; // 当前偏移量
            oldMaxLength = buf.maxLength; // 当前最大长度
            oldCache = buf.cache; // 当前线程缓存

            // 分配新的内存块，并更新ByteBuf的内部状态
            // 这不会修改ByteBuf的读写索引
            allocate(parent.threadCache(), buf, newCapacity);
        }

        // 确定需要复制的字节数
        int bytesToCopy;
        if (newCapacity > oldCapacity) {
            // 扩容：只复制原有数据
            bytesToCopy = oldCapacity;
        } else {
            // 缩容：调整读写索引，并只复制新容量大小的数据
            buf.trimIndicesToCapacity(newCapacity);
            bytesToCopy = newCapacity;
        }

        // 将数据从旧内存复制到新内存
        memoryCopy(oldMemory, oldOffset, buf, bytesToCopy);

        // 释放旧内存块回内存池
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

    /**
     * 创建非池化的内存块。这是一个抽象方法，由具体的Arena实现类提供实现。
     * 
     * @param capacity 请求的内存容量（以字节为单位）
     * @return 新创建的PoolChunk对象
     * 
     * @implNote 该方法在DirectArena和HeapArena中有不同的实现：
     *           - DirectArena:
     *           使用ByteBuffer.allocateDirect或PlatformDependent.allocateDirectNoCleaner
     *           - HeapArena: 使用byte[]数组
     * 
     * @implSpec 实现必须确保：
     *           - 分配的内存大小不小于请求的capacity
     *           - 返回的PoolChunk对象必须正确初始化
     *           - 内存分配方式必须符合Arena的类型（堆内存或直接内存）
     * 
     * @see DirectArena#newUnpooledChunk(int)
     * @see HeapArena#newUnpooledChunk(int)
     * 
     * @throws OutOfMemoryError         当系统无法分配请求的内存时抛出
     * @throws IllegalArgumentException 当capacity小于等于0时抛出
     */
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

        /**
         * 创建非池化的直接内存块。该方法用于分配超大内存块时，直接创建新的内存块而不使用内存池。
         * 
         * @param capacity 请求的内存容量（以字节为单位）
         * @return 新创建的PoolChunk对象，包含分配的直接内存
         * 
         * @implNote 该方法根据directMemoryCacheAlignment的值采用不同的内存分配策略：
         *           - 当directMemoryCacheAlignment为0时，直接分配所需大小的内存
         *           - 当directMemoryCacheAlignment大于0时，会分配额外的内存空间以确保内存对齐
         * 
         * @implSpec 实现必须确保：
         *           - 分配的内存大小不小于请求的capacity
         *           - 当需要内存对齐时，实际分配的内存可能大于请求的capacity
         *           - 返回的PoolChunk对象必须正确初始化，包含基础内存引用和实际使用内存引用
         * 
         * @see PoolChunk
         * @see PlatformDependent#alignDirectBuffer(ByteBuffer, int)
         * 
         * @throws OutOfMemoryError         当系统无法分配请求的内存时抛出
         * @throws IllegalArgumentException 当capacity小于等于0时抛出
         */
        @Override
        protected PoolChunk<ByteBuffer> newUnpooledChunk(int capacity) {
            // 检查是否需要内存对齐
            if (sizeClass.directMemoryCacheAlignment == 0) {
                // 直接分配内存，无需对齐
                ByteBuffer memory = allocateDirect(capacity);
                // 创建PoolChunk，使用相同的内存引用作为基础内存和实际内存
                return new PoolChunk<ByteBuffer>(this, memory, memory, capacity);
            }

            // 需要内存对齐的情况
            // 分配额外的内存空间用于对齐
            final ByteBuffer base = allocateDirect(capacity + sizeClass.directMemoryCacheAlignment);
            // 进行内存对齐操作
            final ByteBuffer memory = PlatformDependent.alignDirectBuffer(base, sizeClass.directMemoryCacheAlignment);
            // 创建PoolChunk，分别使用原始内存引用和对齐后的内存引用
            return new PoolChunk<ByteBuffer>(this, base, memory, capacity);
        }

        /**
         * 分配直接内存缓冲区。该方法根据系统配置选择不同的直接内存分配策略。
         * 
         * @param capacity 要分配的内存容量（以字节为单位）
         * @return 新分配的ByteBuffer对象
         * 
         * @implNote 该方法提供了两种直接内存分配策略：
         *           -
         *           使用无清理器的直接内存分配（当PlatformDependent.useDirectBufferNoCleaner()返回true时）
         *           -
         *           使用标准JDK直接内存分配（当PlatformDependent.useDirectBufferNoCleaner()返回false时）
         * 
         * @implSpec 实现必须确保：
         *           - 分配的内存大小必须等于请求的capacity
         *           - 返回的ByteBuffer必须是直接缓冲区（isDirect()返回true）
         *           - 当使用无清理器分配时，必须正确处理内存释放
         * 
         * @see PlatformDependent#useDirectBufferNoCleaner()
         * @see PlatformDependent#allocateDirectNoCleaner(int)
         * @see ByteBuffer#allocateDirect(int)
         * 
         * @throws OutOfMemoryError         当系统无法分配请求的内存时抛出
         * @throws IllegalArgumentException 当capacity小于等于0时抛出
         * 
         * @apiNote 该方法主要用于Netty的内存池实现中，用于分配直接内存。
         *          使用无清理器的分配方式可以提高性能，但需要手动管理内存释放。
         */
        private static ByteBuffer allocateDirect(int capacity) {
            return PlatformDependent.useDirectBufferNoCleaner()
                    ? PlatformDependent.allocateDirectNoCleaner(capacity)
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

        /**
         * 创建一个适合当前平台特性的池化直接内存缓冲区。
         * <p>
         * 本方法是Netty内存池中重要的工厂方法，根据平台是否支持Unsafe操作，
         * 选择创建性能最优的ByteBuf实现。当平台支持Unsafe操作时，会创建
         * {@link PooledUnsafeDirectByteBuf}，否则创建{@link PooledDirectByteBuf}。
         * </p>
         * 
         * <h3>实现策略：</h3>
         * <p>
         * 采用基于平台能力的自适应策略：
         * <ul>
         * <li>当平台支持Unsafe操作时（通过{@code HAS_UNSAFE}判断），创建{@link PooledUnsafeDirectByteBuf}，
         * 可直接通过内存地址进行操作，性能最优</li>
         * <li>当平台不支持Unsafe操作时，创建{@link PooledDirectByteBuf}，通过JDK的ByteBuffer API操作内存，
         * 性能次优但兼容性更好</li>
         * </ul>
         * </p>
         * 
         * <h3>性能考虑：</h3>
         * <p>
         * <ul>
         * <li>{@link PooledUnsafeDirectByteBuf} 提供了最高性能，直接通过内存地址访问，绕过JDK的安全检查</li>
         * <li>{@link PooledDirectByteBuf} 性能略低，但在不支持Unsafe的平台上是唯一选择</li>
         * <li>两种实现都使用对象池技术减少GC压力，提高内存分配效率</li>
         * </ul>
         * </p>
         * 
         * <h3>内存管理：</h3>
         * <p>
         * 返回的ByteBuf实例由内存池管理，使用完毕后应当调用{@link ByteBuf#release()}方法释放，
         * 而不是依赖垃圾回收。这确保了内存能够被及时回收到池中重用。
         * </p>
         * 
         * @param maxCapacity 创建的ByteBuf的最大容量，单位为字节
         * @return 根据平台特性创建的池化直接内存缓冲区
         * 
         * @see PooledUnsafeDirectByteBuf
         * @see PooledDirectByteBuf
         * @see PlatformDependent#hasUnsafe()
         */
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
