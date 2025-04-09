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

import io.netty.util.internal.StringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.lang.Math.*;

import java.nio.ByteBuffer;

/**
 * 一个管理 {@link PoolChunk} 对象链表的类，这些对象具有相似的内存使用率范围。
 * <p>
 * PoolChunkList 是 Netty 内存池管理系统的核心组件，负责组织和维护一组内存块（PoolChunk）。
 * 每个 PoolChunkList 实例管理一组使用率在特定范围内（minUsage 到 maxUsage）的 PoolChunk 对象。
 * 当 PoolChunk 的使用率变化时，它可能会被移动到其他更合适的 PoolChunkList 中。
 * </p>
 * <p>
 * PoolChunkList 实例通常组织成一个双向链表，通过 prevList 和 nextList 引用相互连接。
 * 这种设计允许 PoolChunk 对象根据其使用率在不同的 PoolChunkList 之间移动，从而优化内存分配和回收。
 * </p>
 * <p>
 * 该类实现了 {@link PoolChunkListMetric} 接口，提供了关于内部 PoolChunk 对象的度量信息。
 * </p>
 *
 * @param <T> 底层内存类型，通常是 {@link ByteBuffer} 或 Netty 的直接内存
 */
final class PoolChunkList<T> implements PoolChunkListMetric {
    /**
     * 用于表示空度量信息的迭代器常量
     */
    private static final Iterator<PoolChunkMetric> EMPTY_METRICS = Collections.<PoolChunkMetric>emptyList().iterator();
    
    /**
     * 该 PoolChunkList 所属的内存区域
     */
    private final PoolArena<T> arena;
    
    /**
     * 指向使用率更高的下一个 PoolChunkList
     * 当当前列表中的 PoolChunk 使用率超过 maxUsage 时，会被移动到这个列表中
     */
    private final PoolChunkList<T> nextList;
    
    /**
     * 该 PoolChunkList 管理的 PoolChunk 的最小使用率（百分比）
     */
    private final int minUsage;
    
    /**
     * 该 PoolChunkList 管理的 PoolChunk 的最大使用率（百分比）
     */
    private final int maxUsage;
    
    /**
     * 可以从该 PoolChunkList 中分配的最大缓冲区容量
     */
    private final int maxCapacity;
    
    /**
     * 指向该 PoolChunkList 管理的 PoolChunk 链表的头节点
     */
    private PoolChunk<T> head;
    
    /**
     * 空闲字节数的最小阈值
     * 当 PoolChunk 的空闲字节数小于等于此值时，会被移动到 nextList
     */
    private final int freeMinThreshold;
    
    /**
     * 空闲字节数的最大阈值
     * 当 PoolChunk 的空闲字节数大于此值时，会被移动到 prevList
     */
    private final int freeMaxThreshold;

    /**
     * 指向使用率更低的前一个 PoolChunkList
     * 当当前列表中的 PoolChunk 使用率低于 minUsage 时，会被移动到这个列表中
     * 该字段仅在 PoolArena 构造函数中创建 PoolChunkList 链表时更新一次
     */
    private PoolChunkList<T> prevList;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /**
     * 创建一个新的 PoolChunkList 实例。
     * <p>
     * 每个 PoolChunkList 管理一组使用率在特定范围内（minUsage 到 maxUsage）的 PoolChunk 对象。
     * 构造函数计算了基于使用率范围的阈值，用于决定何时将 PoolChunk 移动到其他 PoolChunkList。
     * </p>
     * <p>
     * 该类实现了 {@link PoolChunkListMetric} 接口，提供了关于内部 PoolChunk 对象的度量信息。
     * </p>
     *
     * @param arena 该 PoolChunkList 所属的内存区域
     * @param nextList 使用率更高的下一个 PoolChunkList
     * @param minUsage 该 PoolChunkList 管理的 PoolChunk 的最小使用率（百分比）
     * @param maxUsage 该 PoolChunkList 管理的 PoolChunk 的最大使用率（百分比）
     * @param chunkSize PoolChunk 的大小（字节）
     * @throws IllegalArgumentException 如果 minUsage 大于 maxUsage
     */
    PoolChunkList(PoolArena<T> arena, PoolChunkList<T> nextList, int minUsage, int maxUsage, int chunkSize) {
        assert minUsage <= maxUsage;
        this.arena = arena;
        this.nextList = nextList;
        this.minUsage = minUsage;
        this.maxUsage = maxUsage;
        maxCapacity = calculateMaxCapacity(minUsage, chunkSize);

        /**
         * 阈值计算逻辑说明：
         * 
         * PoolChunkList 使用两个关键阈值来决定何时移动 PoolChunk：
         * 1. freeMinThreshold：当 PoolChunk 的空闲字节数 <= 此值时，将其移到使用率更高的列表
         * 2. freeMaxThreshold：当 PoolChunk 的空闲字节数 > 此值时，将其移到使用率更低的列表
         * 
         * 这些阈值是基于 PoolChunk.usage() 方法的计算逻辑推导出来的
         */
        
        // 阈值计算的基本原理是将使用率条件转换为空闲字节数条件
        // 第一步：理解使用率计算公式
        // usage() = 100 - freeBytes * 100L / chunkSize
        //
        // 第二步：转换使用率条件为空闲字节数条件
        // 例如，条件 "usage() >= maxUsage" 可以转换为：
        //   100 - freeBytes * 100L / chunkSize >= maxUsage
        //   -freeBytes * 100L / chunkSize >= maxUsage - 100
        //   freeBytes * 100L / chunkSize <= 100 - maxUsage
        //   freeBytes <= chunkSize * (100 - maxUsage) / 100
        //
        // 因此 freeMinThreshold = chunkSize * (100 - maxUsage) / 100
        // 当 freeBytes <= freeMinThreshold 时，表示 usage >= maxUsage
        
        // 第三步：处理整数舍入问题
        // 由于 usage() 返回 int 值并在计算过程中进行向下取整，
        // 为了精确匹配，阈值需要进行微调以处理舍入误差。
        // 
        // 例如，条件 "freeBytes * 100 / chunkSize < 1" 表示使用率为 99%
        // 这可以转换为：freeBytes < 1 * chunkSize / 100
        //
        // 为什么使用 0.99999999 而不是简单地 +1：
        // 考虑这种情况：freeBytes = 16777216，chunkSize = 16777216
        // 如果简单地 +1，则 freeMaxThreshold 也是 16777216
        // 此时 usage = 0 < minUsage = 1，但由于舍入，不会触发移动
        //
        // 特殊情况处理：
        // 1. 当 maxUsage = 100 时，freeMinThreshold = 0，表示任何非空 chunk 都应移到下一个列表
        // 2. 当 minUsage = 100 时，freeMaxThreshold = 0，表示任何 chunk 都不应移到上一个列表
        
        // 计算 freeMinThreshold：当 PoolChunk 的空闲字节数小于等于此值时，将其移到使用率更高的列表
        freeMinThreshold = (maxUsage == 100) ? 0 : (int) (chunkSize * (100.0 - maxUsage + 0.99999999) / 100L);
        
        // 计算 freeMaxThreshold：当 PoolChunk 的空闲字节数大于此值时，将其移到使用率更低的列表
        freeMaxThreshold = (minUsage == 100) ? 0 : (int) (chunkSize * (100.0 - minUsage + 0.99999999) / 100L);
    }

    /**
     * 计算从属于该 PoolChunkList 的 {@link PoolChunk} 中可以分配的最大缓冲区容量。
     * <p>
     * 此方法基于给定的最小使用率 {@code minUsage} 和块大小 {@code chunkSize} 计算最大可分配容量。
     * 如果最小使用率为 100%，则无法从该列表中分配任何内存，返回 0。
     * 否则，最大容量为块大小乘以可用百分比（100 - minUsage）。
     * </p>
     * <p>
     * 例如，如果 PoolChunkList 的 minUsage 为 25%，则最多可以分配块大小的 75%，
     * 因为这是该 PoolChunkList 中任何 PoolChunk 可用的最大空间。
     * </p>
     *
     * @param minUsage 最小使用率百分比
     * @param chunkSize 块大小（字节）
     * @return 可分配的最大缓冲区容量（字节）
     */
    private static int calculateMaxCapacity(int minUsage, int chunkSize) {
        minUsage = minUsage0(minUsage);

        if (minUsage == 100) {
            // If the minUsage is 100 we can not allocate anything out of this list.
            return 0;
        }

        // Calculate the maximum amount of bytes that can be allocated from a PoolChunk in this PoolChunkList.
        //
        // As an example:
        // - If a PoolChunkList has minUsage == 25 we are allowed to allocate at most 75% of the chunkSize because
        //   this is the maximum amount available in any PoolChunk in this PoolChunkList.
        return  (int) (chunkSize * (100L - minUsage) / 100L);
    }

    /**
     * 设置该 PoolChunkList 的前一个列表引用。
     * <p>
     * 此方法在 PoolArena 构造函数中创建 PoolChunkList 链表时调用，用于建立 PoolChunkList 之间的双向链接。
     * 该方法仅应调用一次，因此使用断言确保 prevList 字段之前未设置。
     * </p>
     *
     * @param prevList 使用率更低的前一个 PoolChunkList
     */
    void prevList(PoolChunkList<T> prevList) {
        assert this.prevList == null;
        this.prevList = prevList;
    }

    /**
     * 尝试从该 PoolChunkList 中分配内存给指定的 ByteBuf。
     * <p>
     * 此方法遍历 PoolChunkList 中的所有 PoolChunk，尝试分配指定大小的内存。
     * 如果分配成功，并且分配后 PoolChunk 的空闲字节数小于等于 freeMinThreshold，
     * 则将该 PoolChunk 从当前列表移除并添加到 nextList 中。
     * </p>
     * <p>
     * 如果请求的容量大于该 PoolChunkList 可以处理的最大容量，或者所有 PoolChunk 都无法满足请求，
     * 则返回 false。
     * </p>
     *
     * @param buf 要分配内存的 PooledByteBuf
     * @param reqCapacity 请求的容量（字节）
     * @param sizeIdx 规范化大小的索引
     * @param threadCache 线程缓存
     * @return 如果分配成功则返回 true，否则返回 false
     */
    boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache threadCache) {
        // 将大小索引转换为标准化容量（字节数）
        int normCapacity = arena.sizeClass.sizeIdx2size(sizeIdx);
        
        // 检查请求的标准化容量是否超过了此 PoolChunkList 可以处理的最大容量
        // 如果超过，则无法从此列表中分配内存，直接返回 false
        if (normCapacity > maxCapacity) {
            // Either this PoolChunkList is empty or the requested capacity is larger then the capacity which can
            // be handled by the PoolChunks that are contained in this PoolChunkList.
            return false;
        }

        // 遍历当前 PoolChunkList 中的所有 PoolChunk，尝试分配内存
        for (PoolChunk<T> cur = head; cur != null; cur = cur.next) {
            // 尝试从当前 PoolChunk 分配内存
            // 如果分配成功，cur.allocate 会将内存引用设置到 buf 中并返回 true
            if (cur.allocate(buf, reqCapacity, sizeIdx, threadCache)) {
                // 分配成功后，检查 PoolChunk 的剩余空闲字节数
                // 如果空闲字节数小于等于最小阈值，说明使用率已经超过了当前 PoolChunkList 的 maxUsage
                if (cur.freeBytes <= freeMinThreshold) {
                    // 从当前 PoolChunkList 中移除该 PoolChunk
                    remove(cur);
                    // 将该 PoolChunk 添加到使用率更高的下一个 PoolChunkList 中
                    nextList.add(cur);
                }
                // 内存分配成功，返回 true
                return true;
            }
        }
        // 所有 PoolChunk 都无法满足分配请求，返回 false
        return false;
    }

    /**
     * 释放指定 PoolChunk 中的内存。
     * <p>
     * 此方法释放 PoolChunk 中由 handle 标识的内存块。释放后，如果 PoolChunk 的空闲字节数大于 freeMaxThreshold，
     * 则将该 PoolChunk 从当前列表移除，并尝试将其移动到使用率更低的 PoolChunkList 中。
     * </p>
     * <p>
     * 如果 PoolChunk 被成功移动到其他列表或保留在当前列表中，则返回 true。
     * 如果 PoolChunk 无法移动到任何列表（通常是因为它完全空闲且没有 prevList），则返回 false，
     * 此时 PoolChunk 将被销毁，其关联的所有内存将被释放。
     * </p>
     *
     * @param chunk 要释放内存的 PoolChunk
     * @param handle 内存块的句柄
     * @param normCapacity 规范化的容量（字节）
     * @param nioBuffer 相关联的 ByteBuffer，如果有的话
     * @return 如果 PoolChunk 被保留则返回 true，否则返回 false
     */
    boolean free(PoolChunk<T> chunk, long handle, int normCapacity, ByteBuffer nioBuffer) {
        // 调用 PoolChunk 的 free 方法释放指定的内存块
        // handle 是内存块的唯一标识，normCapacity 是标准化的容量大小，nioBuffer 是可能关联的 ByteBuffer
        chunk.free(handle, normCapacity, nioBuffer);
        
        // 释放内存后，检查 PoolChunk 的空闲字节数是否超过最大阈值
        // 如果超过，说明使用率已经低于当前 PoolChunkList 的 minUsage
        if (chunk.freeBytes > freeMaxThreshold) {
            // 从当前 PoolChunkList 中移除该 PoolChunk
            remove(chunk);
            // 尝试将 PoolChunk 移动到使用率更低的 PoolChunkList 中
            // 如果没有合适的 PoolChunkList（如 prevList 为 null），move0 会返回 false
            // 这种情况下，PoolChunk 会被销毁，其内存会被释放
            return move0(chunk);
        }
        // PoolChunk 仍然适合当前 PoolChunkList，保留在当前列表中
        return true;
    }

    /**
     * 尝试将 PoolChunk 移动到适当的 PoolChunkList 中。
     * <p>
     * 此方法检查给定的 PoolChunk 是否适合当前 PoolChunkList。如果 PoolChunk 的使用率小于 maxUsage
     * 但其空闲字节数大于 freeMaxThreshold，则尝试将其移动到使用率更低的 PoolChunkList 中。
     * 否则，将其添加到当前 PoolChunkList 中。
     * </p>
     *
     * @param chunk 要移动的 PoolChunk
     * @return 如果 PoolChunk 被成功移动或添加则返回 true，否则返回 false
     */
    private boolean move(PoolChunk<T> chunk) {
        // 断言确保 PoolChunk 的使用率小于当前 PoolChunkList 的最大使用率
        // 这是调用此方法的前提条件
        assert chunk.usage() < maxUsage;

        // 检查 PoolChunk 的空闲字节数是否超过最大阈值
        // 如果超过，说明使用率已经低于当前 PoolChunkList 的 minUsage
        if (chunk.freeBytes > freeMaxThreshold) {
            // 尝试将 PoolChunk 移动到使用率更低的 PoolChunkList 中
            // 如果没有合适的 PoolChunkList，move0 会返回 false
            return move0(chunk);
        }

        // PoolChunk 的使用率适合当前 PoolChunkList
        // 将其添加到当前列表中
        add0(chunk);
        // 添加成功，返回 true
        return true;
    }

    /**
     * 将 {@link PoolChunk} 向下移动到 {@link PoolChunkList} 链表中的适当位置。
     * <p>
     * 此方法将 PoolChunk 移动到具有正确 minUsage/maxUsage 范围的 PoolChunkList 中，
     * 以匹配 {@link PoolChunk#usage()} 的值。如果没有前一个 PoolChunkList（prevList 为 null），
     * 则返回 false，这将导致 PoolChunk 被销毁，其关联的所有内存将被释放。
     * </p>
     *
     * @param chunk 要移动的 PoolChunk
     * @return 如果 PoolChunk 被成功移动则返回 true，否则返回 false
     */
    private boolean move0(PoolChunk<T> chunk) {
        // 检查是否存在使用率更低的 PoolChunkList
        if (prevList == null) {
            // 如果没有前一个列表（当前已经是使用率最低的列表），则返回 false
            // 这将导致 PoolChunk 被销毁，并释放与之关联的所有内存
            // 这种情况通常发生在 PoolChunk 完全空闲（使用率为 0）时
            assert chunk.usage() == 0;
            return false;
        }
        // 递归调用前一个列表的 move 方法，尝试将 PoolChunk 移动到更合适的列表
        // 这种递归调用会一直向下查找，直到找到合适的列表或者达到链表的尽头
        return prevList.move(chunk);
    }

    /**
     * 将 PoolChunk 添加到适当的 PoolChunkList 中。
     * <p>
     * 此方法检查给定的 PoolChunk 是否适合当前 PoolChunkList。如果 PoolChunk 的空闲字节数小于等于 freeMinThreshold，
     * 则将其添加到 nextList 中。否则，将其添加到当前 PoolChunkList 中。
     * </p>
     *
     * @param chunk 要添加的 PoolChunk
     */
    void add(PoolChunk<T> chunk) {
        // 检查 PoolChunk 的空闲字节数是否小于等于最小阈值
        // 如果是，说明其使用率已经超过了当前 PoolChunkList 的 maxUsage
        if (chunk.freeBytes <= freeMinThreshold) {
            // 将 PoolChunk 添加到使用率更高的下一个 PoolChunkList 中
            // 这是一个递归过程，如果 nextList 也不适合，会继续向上查找
            nextList.add(chunk);
            return;
        }
        // PoolChunk 的使用率适合当前 PoolChunkList，将其添加到当前列表中
        add0(chunk);
    }

    /**
     * 将 {@link PoolChunk} 添加到当前 {@link PoolChunkList} 中。
     * <p>
     * 此方法将给定的 PoolChunk 添加到当前 PoolChunkList 的链表头部。它设置 PoolChunk 的 parent 引用为当前 PoolChunkList，
     * 并适当更新链表的指针。
     * </p>
     *
     * @param chunk 要添加到当前 PoolChunkList 的 PoolChunk
     */
    void add0(PoolChunk<T> chunk) {
        // 设置 PoolChunk 的父引用为当前 PoolChunkList
        // 这样 PoolChunk 就知道它属于哪个 PoolChunkList
        chunk.parent = this;
        
        // 如果当前链表为空（head 为 null），则将 chunk 设为头节点
        if (head == null) {
            // 将 chunk 设置为链表的头节点
            head = chunk;
            // 由于只有一个节点，所以前后引用都为 null
            chunk.prev = null;
            chunk.next = null;
        } else {
            // 如果链表不为空，则将 chunk 插入到链表头部（头插法）
            // 设置 chunk 的前驱为 null，因为它将成为新的头节点
            chunk.prev = null;
            // 设置 chunk 的后继为当前的头节点
            chunk.next = head;
            // 更新当前头节点的前驱为 chunk
            head.prev = chunk;
            // 将 chunk 设置为新的头节点
            head = chunk;
        }
    }

    /**
     * 从当前 PoolChunkList 中移除指定的 PoolChunk。
     * <p>
     * 此方法根据 PoolChunk 在链表中的位置（是否为头节点）适当更新链表的指针，
     * 以将指定的 PoolChunk 从链表中移除。
     * </p>
     *
     * @param cur 要移除的 PoolChunk
     */
    private void remove(PoolChunk<T> cur) {
        // 判断要移除的节点是否为头节点
        if (cur == head) {
            // 如果是头节点，则将头节点指针移动到下一个节点
            head = cur.next;
            // 如果新的头节点不为 null，则将其前驱设为 null
            if (head != null) {
                head.prev = null;
            }
        } else {
            // 如果不是头节点，则需要更新前后节点的引用
            // 获取当前节点的后继节点
            PoolChunk<T> next = cur.next;
            // 将当前节点的前驱节点的后继指向当前节点的后继
            cur.prev.next = next;
            // 如果当前节点的后继不为 null，则更新其前驱指向当前节点的前驱
            if (next != null) {
                next.prev = cur.prev;
            }
        }
        // 注意：此方法不会重置被移除节点的 prev 和 next 引用
        // 这是因为调用此方法后，通常会将节点添加到其他列表中，那里会重新设置这些引用
    }

    @Override
    public int minUsage() {
        return minUsage0(minUsage);
    }

    @Override
    public int maxUsage() {
        // 返回 maxUsage 和 100 中的较小值
        // 这确保返回的最大使用率不会超过 100%
        return min(maxUsage, 100);
    }

    private static int minUsage0(int value) {
        // 返回 value 和 1 中的较大值
        // 这确保最小使用率至少为 1%，防止出现 0% 或负数的使用率
        return max(1, value);
    }

    @Override
    public Iterator<PoolChunkMetric> iterator() {
        // 获取 arena 的锁，确保在遍历过程中不会有其他线程修改链表结构
        arena.lock();
        try {
            // 如果链表为空（head 为 null），则返回一个空的迭代器
            if (head == null) {
                return EMPTY_METRICS;
            }
            
            // 创建一个列表来存储所有 PoolChunk 的度量信息
            List<PoolChunkMetric> metrics = new ArrayList<PoolChunkMetric>();
            
            // 遍历链表中的所有 PoolChunk
            for (PoolChunk<T> cur = head;;) {
                // 将当前 PoolChunk 添加到度量列表中
                // PoolChunk 实现了 PoolChunkMetric 接口，所以可以直接添加
                metrics.add(cur);
                
                // 移动到下一个 PoolChunk
                cur = cur.next;
                
                // 如果到达链表尾部（cur 为 null），则退出循环
                if (cur == null) {
                    break;
                }
            }
            
            // 返回度量列表的迭代器，供调用者遍历
            return metrics.iterator();
        } finally {
            // 无论是否发生异常，都确保释放 arena 的锁
            // 这防止了因异常导致的锁未释放，进而可能引起死锁
            arena.unlock();
        }
    }

    @Override
    public String toString() {
        // 创建一个 StringBuilder 用于构建字符串表示
        StringBuilder buf = new StringBuilder();
        
        // 获取 arena 的锁，确保在生成字符串表示过程中不会有其他线程修改链表结构
        arena.lock();
        try {
            // 如果链表为空（head 为 null），则返回 "none" 字符串
            if (head == null) {
                return "none";
            }

            // 遍历链表中的所有 PoolChunk
            for (PoolChunk<T> cur = head;;) {
                // 将当前 PoolChunk 的字符串表示添加到 StringBuilder 中
                // 这里调用的是 PoolChunk 的 toString() 方法
                buf.append(cur);
                
                // 移动到下一个 PoolChunk
                cur = cur.next;
                
                // 如果到达链表尾部（cur 为 null），则退出循环
                if (cur == null) {
                    break;
                }
                
                // 如果还有下一个 PoolChunk，则添加换行符
                // 这样每个 PoolChunk 的信息会显示在单独的一行
                buf.append(StringUtil.NEWLINE);
            }
        } finally {
            // 无论是否发生异常，都确保释放 arena 的锁
            arena.unlock();
        }
        
        // 返回构建好的字符串表示
        return buf.toString();
    }

    /**
     * 销毁该 PoolChunkList 中的所有 PoolChunk。
     * <p>
     * 此方法会遍历链表中的所有 PoolChunk，并调用 arena 的 destroyChunk 方法销毁它们。
     * 销毁后，将 head 设为 null，表示链表为空。
     * </p>
     *
     * @param arena 用于销毁 PoolChunk 的 PoolArena
     */
    void destroy(PoolArena<T> arena) {
        // 从链表头开始遍历
        PoolChunk<T> chunk = head;
        
        // 遍历链表中的所有 PoolChunk
        while (chunk != null) {
            // 调用 arena 的 destroyChunk 方法销毁当前 PoolChunk
            // 这会释放 PoolChunk 持有的所有内存资源
            arena.destroyChunk(chunk);
            
            // 移动到下一个 PoolChunk
            // 注意：此处不需要保存前一个 chunk 的引用，因为它已经被销毁
            chunk = chunk.next;
        }
        
        // 将 head 设为 null，表示链表为空
        // 这样 GC 可以回收所有 PoolChunk 对象，前提是没有其他引用指向它们
        head = null;
    }
}
