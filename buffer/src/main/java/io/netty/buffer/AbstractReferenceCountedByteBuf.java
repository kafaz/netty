/*
 * Copyright 2013 The Netty Project
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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.util.internal.ReferenceCountUpdater;


/**
 * 引用计数ByteBuf的抽象基类。
 * 
 * <h2>类描述</h2>
 * <p>
 * 提供了引用计数管理的实现，包括retain和release操作。
 * 所有池化和需要跟踪生命周期的ByteBuf都继承自此类。
 * </p>
 * 
 * <h2>线程安全性</h2>
 * <h3>1. 引用计数操作</h3>
 * <p>引用计数操作（retain/release）是线程安全的：</p>
 * <ul>
 *   <li>使用AtomicIntegerFieldUpdater和Unsafe实现原子操作</li>
 *   <li>引用计数的增减操作是原子的</li>
 *   <li>引用计数的读取操作是volatile的，保证可见性</li>
 * </ul>
 * 
 * <h3>2. 非线程安全操作</h3>
 * <p>ByteBuf的其他操作不是线程安全的：</p>
 * <ul>
 *   <li>读写索引（readerIndex/writerIndex）的修改不是原子的</li>
 *   <li>数据内容的读写操作不是原子的</li>
 *   <li>多个线程并发访问可能导致数据不一致</li>
 * </ul>
 * 
 * <h2>最佳实践</h2>
 * <ul>
 *   <li>避免在多个线程间共享同一个可写的ByteBuf</li>
 *   <li>如果需要共享，应该创建ByteBuf的副本</li>
 *   <li>使用消息传递机制而不是共享内存</li>
 *   <li>在Handler链中明确资源的所有权</li>
 * </ul>
 * 
 * <h2>引用计数管理</h2>
 * <ul>
 *   <li>每个ByteBuf创建时引用计数为1</li>
 *   <li>{@link #retain()} 增加引用计数</li>
 *   <li>{@link #release()} 减少引用计数</li>
 *   <li>当引用计数为0时，自动释放资源</li>
 *   <li>引用计数溢出会抛出{@link IllegalReferenceCountException}</li>
 * </ul>
 * 
 * <h2>性能优化</h2>
 * <ul>
 *   <li>使用Unsafe直接内存操作提高性能</li>
 *   <li>引用计数使用位运算优化存储</li>
 *   <li>提供非volatile的快速路径检查</li>
 * </ul>
 * 
 * <h2>调试支持</h2>
 * <ul>
 *   <li>{@link #touch()} 方法记录访问者</li>
 *   <li>支持附加调试信息</li>
 *   <li>引用计数异常提供详细的错误信息</li>
 * </ul>
 * 
 * @see ByteBuf
 * @see ReferenceCounted
 * @see IllegalReferenceCountException
 * 
 * @since 4.0
 */
public abstract class AbstractReferenceCountedByteBuf extends AbstractByteBuf {
    /**
     * refCnt字段在内存中的偏移量，用于Unsafe直接内存操作
     * 通过unsafe可以实现更高效的内存访问，避免AtomicIntegerFieldUpdater的开销
     */
    private static final long REFCNT_FIELD_OFFSET = ReferenceCountUpdater
            .getUnsafeOffset(AbstractReferenceCountedByteBuf.class, "refCnt");

    /**
     * 原子整数字段更新器，用于以线程安全方式操作refCnt字段
     * 当unsafe不可用时，会使用此更新器进行引用计数操作
     */
    private static final AtomicIntegerFieldUpdater<AbstractReferenceCountedByteBuf> AIF_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(AbstractReferenceCountedByteBuf.class, "refCnt");

    /**
     * 封装了引用计数更新的通用逻辑的更新器
     * 此更新器处理引用计数变更的所有细节，包括溢出检查和线程安全性
     */
    private static final ReferenceCountUpdater<AbstractReferenceCountedByteBuf> updater = new ReferenceCountUpdater<AbstractReferenceCountedByteBuf>() {
        /**
         * 提供原子字段更新器
         * 
         * @return 用于refCnt字段的原子更新器
         */
        @Override
        protected AtomicIntegerFieldUpdater<AbstractReferenceCountedByteBuf> updater() {
            return AIF_UPDATER;
        }

        /**
         * 提供unsafe内存偏移量
         * 
         * @return refCnt字段在内存中的偏移量
         */
        @Override
        protected long unsafeOffset() {
            return REFCNT_FIELD_OFFSET;
        }
    };

    /**
     * 真实的引用计数值存储在此字段中
     * 注意: 真实引用计数 = refCnt >>> 1 (如果为偶数)
     * 如果此值为奇数，则表示对象已被释放(真实引用计数为0)
     *
     * 该字段不应直接访问，所有访问都应通过updater进行
     * volatile确保跨线程的可见性，这对于正确的内存管理至关重要
     */
    @SuppressWarnings({ "unused", "FieldMayBeFinal" })
    private volatile int refCnt;

    /**
     * 构造函数，初始化引用计数为1(实际存储为2)
     * 
     * @param maxCapacity 缓冲区的最大容量
     */
    protected AbstractReferenceCountedByteBuf(int maxCapacity) {
        super(maxCapacity);
        // 设置初始引用计数值，实际存储为2，表示真实引用计数为1
        updater.setInitialValue(this);
    }

    /**
     * 检查此缓冲区是否可访问(引用计数>0)
     * 这是一个性能优化的非volatile读取，因为ensureAccessible()本身就是尽力而为的检查
     * 
     * @return 如果引用计数>0则返回true，否则返回false
     */
    @Override
    boolean isAccessible() {
        // 非volatile读取以提高性能，因为ensureAccessible()本身就是racy的，只提供尽力而为的检查
        return updater.isLiveNonVolatile(this);
    }

    /**
     * 获取当前引用计数
     * 
     * @return 当前引用计数值
     */
    @Override
    public int refCnt() {
        return updater.refCnt(this);
    }

    /**
     * 直接设置引用计数(不安全操作)
     * 此方法仅供子类在特殊情况下使用，如创建新视图时重置引用计数
     * 
     * @param refCnt 要设置的引用计数值
     */
    protected final void setRefCnt(int refCnt) {
        updater.setRefCnt(this, refCnt);
    }

    /**
     * 将引用计数重置为1(不安全操作)
     * 主要用于派生缓冲区创建时的引用计数初始化
     */
    protected final void resetRefCnt() {
        updater.resetRefCnt(this);
    }

    /**
     * 增加引用计数，默认增加1
     * 
     * @return this，支持链式调用
     * @throws IllegalReferenceCountException 如果缓冲区已被释放或引用计数将溢出
     */
    @Override
    public ByteBuf retain() {
        return updater.retain(this);
    }

    /**
     * 增加引用计数指定数量
     * 
     * @param increment 要增加的引用计数值
     * @return this，支持链式调用
     * @throws IllegalReferenceCountException 如果缓冲区已被释放、increment不为正数或引用计数将溢出
     */
    @Override
    public ByteBuf retain(int increment) {
        return updater.retain(this, increment);
    }

    /**
     * 记录缓冲区被谁触碰过，用于调试
     * 默认实现不做任何操作，子类可以重写此方法提供记录功能
     * 
     * @return this，支持链式调用
     */
    @Override
    public ByteBuf touch() {
        return this;
    }

    /**
     * 记录缓冲区被谁触碰过，并附加额外信息，用于调试
     * 默认实现不做任何操作，子类可以重写此方法提供记录功能
     * 
     * @param hint 附加的调试信息
     * @return this，支持链式调用
     */
    @Override
    public ByteBuf touch(Object hint) {
        return this;
    }

    /**
     * 减少引用计数，默认减少1
     * 当引用计数变为0时，缓冲区将被释放
     * 
     * @return 如果引用计数变为0则返回true，否则返回false
     * @throws IllegalReferenceCountException 如果缓冲区已被释放
     */
    @Override
    public boolean release() {
        return handleRelease(updater.release(this));
    }

    /**
     * 减少引用计数指定数量
     * 当引用计数变为0时，缓冲区将被释放
     * 
     * @param decrement 要减少的引用计数值
     * @return 如果引用计数变为0则返回true，否则返回false
     * @throws IllegalReferenceCountException 如果缓冲区已被释放、decrement不为正数或大于当前引用计数
     */
    @Override
    public boolean release(int decrement) {
        return handleRelease(updater.release(this, decrement));
    }

    /**
     * 处理release操作的结果
     * 如果引用计数变为0，则调用deallocate()方法释放资源
     * 
     * @param result release操作的结果
     * @return 与入参相同的值
     */
    private boolean handleRelease(boolean result) {
        if (result) {
            // 引用计数变为0，需要释放资源
            deallocate();
        }
        return result;
    }

    /**
     * 当引用计数变为0时调用，用于释放底层资源
     * 这是一个抽象方法，必须由具体子类实现，执行特定的资源释放逻辑
     */
    protected abstract void deallocate();
}
