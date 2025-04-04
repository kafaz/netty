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

import java.util.concurrent.locks.ReentrantLock;

import static io.netty.buffer.PoolChunk.RUN_OFFSET_SHIFT;
import static io.netty.buffer.PoolChunk.SIZE_SHIFT;
import static io.netty.buffer.PoolChunk.IS_USED_SHIFT;
import static io.netty.buffer.PoolChunk.IS_SUBPAGE_SHIFT;

/**
 * 子页内存管理类，负责小内存单元的分配与回收。
 * <p>
 * 在Netty内存池架构中，PoolSubpage处于最底层，专门管理小于一个页面大小的内存分配请求。
 * 通过位图（bitmap）追踪内存使用情况，实现高效、低碎片的小内存管理。
 * </p>
 * 
 * <h3>基本原理</h3>
 * <p>
 * PoolSubpage将一个页面（通常为8KB）划分为多个大小相等的小内存块。例如，一个页面可以被划分为：
 * <ul>
 *   <li>512个16字节的小块</li>
 *   <li>256个32字节的小块</li>
 *   <li>128个64字节的小块</li>
 *   <li>以此类推</li>
 * </ul>
 * 每个小块的分配状态通过位图中的单个位记录：0表示可用，1表示已分配。
 * </p>
 * 
 * <h3>链表管理</h3>
 * <p>
 * 所有相同大小的PoolSubpage通过双向链表连接，形成子页池。
 * 每个链表有一个特殊的头节点（prev和next指向自身表示空链表）。
 * </p>
 * 
 * <h3>生命周期</h3>
 * <p>
 * 1. 创建：当需要分配小内存且没有可用子页时创建
 * 2. 分配：通过allocate()分配小内存单元
 * 3. 释放：通过free()释放小内存单元
 * 4. 销毁：当所有小内存单元都被释放，且池中有其他子页时，可被销毁
 * </p>
 * 
 * @param <T> 内存类型参数，通常为ByteBuffer
 * 
 * @see PoolChunk
 * @see PoolArena
 */
final class PoolSubpage<T> implements PoolSubpageMetric {

    /**
     * 所属的内存块，包含实际的内存数据
     */
    final PoolChunk<T> chunk;
    
    /**
     * 子页中每个元素(小内存块)的大小，单位为字节
     */
    final int elemSize;
    
    /**
     * 页面大小的位移值，用于地址计算
     * 例如：如果页面大小为8KB(8192)，则pageShifts为13(2^13=8192)
     */
    private final int pageShifts;
    
    /**
     * 在chunk中的页面偏移量
     * 用于计算内存的实际物理地址
     */
    private final int runOffset;
    
    /**
     * 子页总大小，通常为一个页面大小(runSize)
     */
    private final int runSize;
    
    /**
     * 位图数组，用于跟踪每个小内存块的分配状态
     * 每个bit对应一个小内存块：0=可用，1=已分配
     */
    private final long[] bitmap;
    
    /**
     * 位图数组的长度
     * 由最大元素数量决定：bitmapLength = (maxNumElems + 63) / 64
     */
    private final int bitmapLength;
    
    /**
     * 子页能容纳的最大元素(小内存块)数量
     * maxNumElems = runSize / elemSize
     */
    private final int maxNumElems;
    
    /**
     * 在arena子页池数组中的索引位置
     * 用于快速找到对应大小的子页池
     */
    final int headIndex;

    /**
     * 双向链表前向指针，指向前一个PoolSubpage
     * 用于子页池的链表管理
     */
    PoolSubpage<T> prev;
    
    /**
     * 双向链表后向指针，指向后一个PoolSubpage
     * 用于子页池的链表管理
     */
    PoolSubpage<T> next;

    /**
     * 标志位，指示此子页不应被销毁
     * 当子页仍有分配的内存块时为true
     */
    boolean doNotDestroy;
    
    /**
     * 下一个可用的位图索引
     * 用于快速找到下一个可分配的位置
     * -1表示没有快速路径，需要遍历位图找寻
     */
    private int nextAvail;
    
    /**
     * 当前可用的元素(小内存块)数量
     * 初始值等于maxNumElems，每次分配减1，释放加1
     */
    private int numAvail;

    /**
     * 用于同步访问的锁
     * 仅在链表头节点(哑节点)上使用，实际子页实例的锁为null
     */
    final ReentrantLock lock;

    /** 
     * 创建一个特殊的链表头节点
     * 该构造函数用于在PoolArena中创建子页池的头节点
     * 头节点不包含实际内存，仅作为链表管理用途
     * 
     * @param headIndex 在子页池数组中的索引
     */
    PoolSubpage(int headIndex) {
        // 头节点不关联任何chunk
        chunk = null;
        // 头节点需要锁进行同步
        lock = new ReentrantLock();
        // 以下字段对头节点无意义，设为无效值
        pageShifts = -1;
        runOffset = -1;
        elemSize = -1;
        runSize = -1;
        bitmap = null;
        bitmapLength = -1;
        maxNumElems = 0;
        // 存储头节点索引
        this.headIndex = headIndex;
    }

    /**
     * 创建一个实际的子页实例
     * 该构造函数用于创建可分配内存的子页
     * 
     * @param head 所属子页池的头节点
     * @param chunk 所属的内存块
     * @param pageShifts 页面大小的位移值
     * @param runOffset 在chunk中的页面偏移量
     * @param runSize 子页总大小
     * @param elemSize 每个元素的大小
     */
    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int pageShifts, int runOffset, int runSize, int elemSize) {
        // 1. 初始化基本属性
        this.headIndex = head.headIndex; // 设置头索引，与所属子页池对应
        this.chunk = chunk; // 关联的内存块
        this.pageShifts = pageShifts; // 页面大小位移量
        this.runOffset = runOffset; // 在chunk中的偏移量
        this.runSize = runSize; // 子页总大小
        this.elemSize = elemSize; // 每个元素的大小

        doNotDestroy = true; // 标记该子页不可销毁(正在使用中)

        // 2. 计算可分配元素数量
        maxNumElems = numAvail = runSize / elemSize; // 计算可以分配的元素数量并初始化可用数量
        // 例如：如果runSize=8192(8KB), elemSize=16字节，则maxNumElems=512

        // 3. 计算位图数组长度
        int bitmapLength = maxNumElems >>> 6; // 右移6位，相当于除以64(一个long有64位)
        if ((maxNumElems & 63) != 0) { // 检查是否有余数(按位与63，相当于求余64)
            bitmapLength++; // 如果有余数，需要多一个long来存储
        }
        this.bitmapLength = bitmapLength;

        // 4. 初始化位图数组
        bitmap = new long[bitmapLength]; // 创建位图数组
        nextAvail = 0; // 下一个可用位置初始化为0

        lock = null; // 实际子页无需锁，使用头节点的锁
        addToPool(head); // 将当前子页添加到对应子页池的链表中
    }

    /**
     * 分配一个元素(小内存块)
     * 
     * @return 分配的内存句柄，失败时返回-1
     */
    long allocate() {
        // 检查是否有可用空间，或者子页是否标记为销毁
        if (numAvail == 0 || !doNotDestroy) {
            return -1;
        }

        // 获取下一个可用的位图索引
        final int bitmapIdx = getNextAvail();
        if (bitmapIdx < 0) {
            // 无法找到可用位置，子页状态异常
            removeFromPool(); // 从池中移除以防止重复错误
            throw new AssertionError("No next available bitmap index found (bitmapIdx = " + bitmapIdx + "), " +
                    "even though there are supposed to be (numAvail = " + numAvail + ") " +
                    "out of (maxNumElems = " + maxNumElems + ") available indexes.");
        }
        
        // 计算位图数组索引和位偏移
        int q = bitmapIdx >>> 6; // 数组索引 = bitmapIdx / 64
        int r = bitmapIdx & 63;  // 位偏移 = bitmapIdx % 64
        
        // 确保该位未被设置(未分配)
        assert (bitmap[q] >>> r & 1) == 0;
        
        // 设置对应位为1，标记为已分配
        bitmap[q] |= 1L << r;

        // 可用数量减1，如果变为0则从池中移除
        if (--numAvail == 0) {
            removeFromPool();
        }

        // 转换为内存句柄返回
        return toHandle(bitmapIdx);
    }

    /**
     * 释放一个元素(小内存块)
     * 
     * @param head 所属子页池的头节点
     * @param bitmapIdx 要释放的位图索引
     * @return true表示子页仍在使用，false表示子页可以被销毁
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        // 计算位图数组索引和位偏移
        int q = bitmapIdx >>> 6; // 数组索引
        int r = bitmapIdx & 63;  // 位偏移
        
        // 确保该位已被设置(已分配)
        assert (bitmap[q] >>> r & 1) != 0;
        
        // 翻转对应位，设置为0，标记为未分配
        bitmap[q] ^= 1L << r;

        // 设置为下一个可用位置，提高分配效率
        setNextAvail(bitmapIdx);

        // 可用数量加1，如果之前为0则重新加入池中
        if (numAvail++ == 0) {
            addToPool(head);
            /*
             * 当maxNumElems == 1时，最大numAvail也是1
             * 这些PoolSubpage在释放操作时都会进入这里
             * 如果从这里直接返回true，那么剩余代码将无法到达
             * 并且它们实际上不会被回收。所以只在maxNumElems > 1时才返回true
             */
            if (maxNumElems > 1) {
                return true;
            }
        }

        // 检查是否所有元素都已释放
        if (numAvail != maxNumElems) {
            return true; // 仍有元素在使用，保持子页
        } else {
            // 子页未被使用(numAvail == maxNumElems)
            if (prev == next) {
                // 如果这个子页是池中唯一剩余的，不要移除它
                return true;
            }

            // 如果池中还有其他子页，移除此子页
            doNotDestroy = false;
            removeFromPool();
            return false; // 表示此子页可以被销毁
        }
    }

    /**
     * 将当前子页添加到子页池链表中
     * 
     * @param head 链表头节点
     */
    private void addToPool(PoolSubpage<T> head) {
        // 确保当前节点未连接到任何链表
        assert prev == null && next == null;
        // 设置前向指针为头节点
        prev = head;
        // 设置后向指针为头节点的后一个节点
        next = head.next;
        // 更新头节点后一个节点的前向指针
        next.prev = this;
        // 更新头节点的后向指针为当前节点
        head.next = this;
    }

    /**
     * 从子页池链表中移除当前子页
     */
    private void removeFromPool() {
        // 确保当前节点已经在链表中
        assert prev != null && next != null;
        // 更新前一个节点的后向指针
        prev.next = next;
        // 更新后一个节点的前向指针
        next.prev = prev;
        // 清除指针，避免内存泄漏
        next = null;
        prev = null;
    }

    /**
     * 设置下一个可用位置索引
     * 
     * @param bitmapIdx 位图索引
     */
    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    /**
     * 获取下一个可用的位图索引
     * 
     * @return 下一个可用位置的位图索引，如果没有则返回-1
     */
    private int getNextAvail() {
        // 检查是否有已缓存的nextAvail值
        int nextAvail = this.nextAvail;
        if (nextAvail >= 0) {
            // 如果有，重置nextAvail并返回缓存值
            this.nextAvail = -1;
            return nextAvail;
        }
        // 否则，搜索下一个可用位置
        return findNextAvail();
    }

    /**
     * 在位图中查找下一个可用位置
     * 
     * @return 下一个可用位置的位图索引，如果没有则返回-1
     */
    private int findNextAvail() {
        // 遍历位图数组
        for (int i = 0; i < bitmapLength; i++) {
            long bits = bitmap[i];
            // ~bits != 0 表示有未设置的位(可用位置)
            if (~bits != 0) {
                return findNextAvail0(i, bits);
            }
        }
        return -1; // 没有可用位置
    }

    /**
     * 在指定位图块中查找第一个可用位置
     * 
     * @param i 位图数组索引
     * @param bits 位图值
     * @return 找到的位图索引，如果没有则返回-1
     */
    private int findNextAvail0(int i, long bits) {
        // 计算基础值(本块的起始索引)
        final int baseVal = i << 6;
        // 遍历64位
        for (int j = 0; j < 64; j++) {
            // 检查当前位是否为0(未分配)
            if ((bits & 1) == 0) {
                // 计算位图索引
                int val = baseVal | j;
                // 确保索引在有效范围内
                if (val < maxNumElems) {
                    return val;
                } else {
                    // 超出范围，退出循环
                    break;
                }
            }
            // 右移一位，检查下一位
            bits >>>= 1;
        }
        return -1; // 没有找到可用位置
    }

    /**
     * 将位图索引转换为内存句柄
     * 
     * @param bitmapIdx 位图索引
     * @return 内存句柄，编码了内存位置和类型信息
     */
    private long toHandle(int bitmapIdx) {
        // 计算页数
        int pages = runSize >> pageShifts;
        // 构建内存句柄，包含:
        // 1. 运行块偏移量
        // 2. 页数
        // 3. 已使用标志(1)
        // 4. 子页标志(1)
        // 5. 位图索引
        return (long) runOffset << RUN_OFFSET_SHIFT
                | (long) pages << SIZE_SHIFT
                | 1L << IS_USED_SHIFT
                | 1L << IS_SUBPAGE_SHIFT
                | bitmapIdx;
    }

    /**
     * 返回子页的字符串表示
     * 
     * @return 描述子页状态的字符串
     */
    @Override
    public String toString() {
        final int numAvail;
        if (chunk == null) {
            // 这是头节点，不需要同步因为这些值从不改变
            numAvail = 0;
        } else {
            final boolean doNotDestroy;
            // 获取子页池头节点
            PoolSubpage<T> head = chunk.arena.smallSubpagePools[headIndex];
            // 加锁确保线程安全
            head.lock();
            try {
                doNotDestroy = this.doNotDestroy;
                numAvail = this.numAvail;
            } finally {
                head.unlock();
            }
            if (!doNotDestroy) {
                // 标记为销毁的子页显示简化信息
                return "(" + runOffset + ": not in use)";
            }
        }

        // 返回详细状态信息
        return "(" + this.runOffset + ": " + (this.maxNumElems - numAvail) + '/' + this.maxNumElems +
                ", offset: " + this.runOffset + ", length: " + this.runSize + ", elemSize: " + this.elemSize + ')';
    }

    /**
     * 返回子页可容纳的最大元素数量
     * 
     * @return 最大元素数量
     */
    @Override
    public int maxNumElements() {
        return maxNumElems;
    }

    /**
     * 返回子页当前可用的元素数量
     * 
     * @return 可用元素数量
     */
    @Override
    public int numAvailable() {
        if (chunk == null) {
            // 头节点无可用元素
            return 0;
        }
        // 获取子页池头节点
        PoolSubpage<T> head = chunk.arena.smallSubpagePools[headIndex];
        // 加锁确保线程安全
        head.lock();
        try {
            return numAvail;
        } finally {
            head.unlock();
        }
    }

    /**
     * 返回子页中每个元素的大小
     * 
     * @return 元素大小(字节)
     */
    @Override
    public int elementSize() {
        return elemSize;
    }

    /**
     * 返回一个页的大小
     * 
     * @return 页大小(字节)
     */
    @Override
    public int pageSize() {
        return 1 << pageShifts;
    }

    /**
     * 检查子页是否标记为不可销毁
     * 
     * @return true表示不可销毁，false表示可以销毁
     */
    boolean isDoNotDestroy() {
        if (chunk == null) {
            // 头节点永远不销毁
            return true;
        }
        // 获取子页池头节点
        PoolSubpage<T> head = chunk.arena.smallSubpagePools[headIndex];
        // 加锁确保线程安全
        head.lock();
        try {
            return doNotDestroy;
        } finally {
            head.unlock();
        }
    }

    /**
     * 销毁当前子页及其所属的chunk
     */
    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }

    /**
     * 加锁，只有头节点才有实际锁
     */
    void lock() {
        lock.lock();
    }

    /**
     * 解锁，只有头节点才有实际锁
     */
    void unlock() {
        lock.unlock();
    }
}
