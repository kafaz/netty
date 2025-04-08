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

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakTracker;
import io.netty.util.internal.MathUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;

/**
 * Skeletal {@link ByteBufAllocator} implementation to extend.
 * <p>
 * 这个抽象类提供了{@link ByteBufAllocator}接口的骨架实现，实现了大部分创建
 * 各种类型ByteBuf的通用逻辑，同时将实际内存分配的细节留给子类去实现。
 * 主要采用了模板方法设计模式，子类只需要实现{@link #newHeapBuffer(int, int)}
 * 和{@link #newDirectBuffer(int, int)}两个核心方法即可。
 * <p>
 * 该类提供以下主要功能：
 * <ul>
 * <li>提供统一的缓冲区创建API，包括堆内存缓冲区、直接内存缓冲区和复合缓冲区</li>
 * <li>处理容量验证、默认值和最大值计算</li>
 * <li>支持内存泄漏检测（通过包装返回的缓冲区）</li>
 * <li>提供缓冲区扩容算法</li>
 * </ul>
 * <p>
 * 继承此类的实现通常有两种类型：
 * <ul>
 * <li>池化分配器 - 如{@code PooledByteBufAllocator}，实现缓冲区对象和底层内存的重用</li>
 * <li>非池化分配器 - 如{@code UnpooledByteBufAllocator}，每次请求都分配新的内存</li>
 * </ul>
 * <p>
 * 使用示例：
 * 
 * <pre>
 * 创建一个堆内存缓冲区
 * ByteBuf heapBuf = alloc.heapBuffer(1024);
 * 
 * 创建一个直接内存缓冲区
 * ByteBuf directBuf = alloc.directBuffer(1024, 8192);
 * 
 * 创建一个复合缓冲区
 * CompositeByteBuf compositeBuf = alloc.compositeBuffer();
 * </pre>
 * <p>
 * 通常不推荐直接使用此类，而是应该使用其具体实现类{@code PooledByteBufAllocator}
 * 或{@code UnpooledByteBufAllocator}，或者通过{@code ByteBufAllocator.DEFAULT}获取
 * 默认的分配器实例。
 * 
 * @see ByteBufAllocator
 * @see ByteBuf
 * @see CompositeByteBuf
 */
public abstract class AbstractByteBufAllocator implements ByteBufAllocator {
    static final int DEFAULT_INITIAL_CAPACITY = 256;
    static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;
    static final int DEFAULT_MAX_COMPONENTS = 16;
    static final int CALCULATE_THRESHOLD = 1048576 * 4; // 4 MiB page

    static {
        ResourceLeakDetector.addExclusions(AbstractByteBufAllocator.class, "toLeakAwareBuffer");
    }

    /**
     * 将普通ByteBuf转换为具有内存泄漏检测功能的包装缓冲区。
     * <p>
     * 此方法是Netty内存泄漏检测系统的核心组件，会根据{@link ResourceLeakDetector}的当前配置级别，
     * 决定是否以及如何包装输入的ByteBuf，以实现不同级别的内存泄漏检测能力。
     * </p>
     * 
     * <h3>检测级别说明</h3>
     * <ul>
     *   <li><b>SIMPLE</b>: 基本检测级别，仅记录泄漏发生的位置</li>
     *   <li><b>ADVANCED/PARANOID</b>: 高级检测级别，不仅记录泄漏位置，还跟踪缓冲区的分配和使用记录</li>
     *   <li><b>DISABLED</b>: 禁用检测，不进行包装，直接返回原始缓冲区</li>
     * </ul>
     * 
     * <h3>工作流程</h3>
     * <ol>
     *   <li>获取当前系统配置的泄漏检测级别</li>
     *   <li>根据级别尝试为缓冲区创建对应的泄漏跟踪器</li>
     *   <li>如果成功创建跟踪器，则用对应级别的包装类包装原始缓冲区</li>
     *   <li>如果检测级别为DISABLED或创建跟踪器失败，则返回原始缓冲区</li>
     * </ol>
     * 
     * <p>
     * 注意：此方法对性能有一定影响，尤其在ADVANCED/PARANOID级别下。
     * 在生产环境中通常建议使用SIMPLE级别或完全禁用。
     * </p>
     *
     * @param buf 原始ByteBuf缓冲区
     * @return 包装后具有泄漏检测能力的ByteBuf实例，或在检测禁用时返回原始缓冲区
     * 
     * @see ResourceLeakDetector
     * @see SimpleLeakAwareByteBuf
     * @see AdvancedLeakAwareByteBuf
     */
    protected static ByteBuf toLeakAwareBuffer(ByteBuf buf) {
        // 声明泄漏跟踪器变量，用于关联和跟踪ByteBuf的生命周期
        ResourceLeakTracker<ByteBuf> leak;
        
        // 获取当前系统配置的泄漏检测级别，并根据不同级别采取不同处理
        switch (ResourceLeakDetector.getLevel()) {
            case SIMPLE:
                // 简单检测模式：仅记录泄漏发生的位置
                // 尝试为缓冲区创建泄漏跟踪器
                leak = AbstractByteBuf.leakDetector.track(buf);
                if (leak != null) {
                    // 如果成功创建跟踪器，用SimpleLeakAwareByteBuf包装原始缓冲区
                    // 这种包装类仅在缓冲区被GC但未正确释放时报告泄漏位置
                    buf = new SimpleLeakAwareByteBuf(buf, leak);
                }
                break;
            case ADVANCED:
            case PARANOID:
                // 高级检测模式：记录泄漏位置和缓冲区的分配、访问历史
                // 尝试为缓冲区创建泄漏跟踪器
                leak = AbstractByteBuf.leakDetector.track(buf);
                if (leak != null) {
                    // 如果成功创建跟踪器，用AdvancedLeakAwareByteBuf包装原始缓冲区
                    // 这种包装类会记录所有缓冲区操作，提供更详细的泄漏诊断信息
                    buf = new AdvancedLeakAwareByteBuf(buf, leak);
                }
                break;
            default:
                // DISABLED模式或其他未定义模式：不执行任何包装，直接使用原始缓冲区
                break;
        }
        
        // 返回处理后的缓冲区，可能是原始缓冲区或其包装类
        return buf;
    }

    /**
     * 将普通CompositeByteBuf转换为具有内存泄漏检测功能的包装缓冲区。
     * <p>
     * 此方法是Netty内存泄漏检测系统的核心组件，会根据{@link ResourceLeakDetector}的当前配置级别，
     * 决定是否以及如何包装输入的CompositeByteBuf，以实现不同级别的内存泄漏检测能力。
     * </p>
     * 
     * <h3>检测级别说明</h3>
     * <ul>
     *   <li><b>SIMPLE</b>: 基本检测级别，仅记录泄漏发生的位置</li>
     *   <li><b>ADVANCED/PARANOID</b>: 高级检测级别，不仅记录泄漏位置，还跟踪缓冲区的分配和使用记录</li>
     *   <li><b>DISABLED</b>: 禁用检测，不进行包装，直接返回原始缓冲区</li>
     * </ul>
     * 
     * <h3>工作流程</h3>
     * <ol>
     *   <li>获取当前系统配置的泄漏检测级别</li>
     *   <li>根据级别尝试为缓冲区创建对应的泄漏跟踪器</li>
     *   <li>如果成功创建跟踪器，则用对应级别的包装类包装原始缓冲区</li>
     *   <li>如果检测级别为DISABLED或创建跟踪器失败，则返回原始缓冲区</li>
     * </ol>
     * 
     * <p>
     * 注意：此方法对性能有一定影响，尤其在ADVANCED/PARANOID级别下。
     * 在生产环境中通常建议使用SIMPLE级别或完全禁用。
     * </p>
     *
     * @param buf 原始CompositeByteBuf缓冲区
     * @return 包装后具有泄漏检测能力的CompositeByteBuf实例，或在检测禁用时返回原始缓冲区
     * 
     * @see ResourceLeakDetector
     * @see SimpleLeakAwareCompositeByteBuf
     * @see AdvancedLeakAwareCompositeByteBuf
     */
    protected static CompositeByteBuf toLeakAwareBuffer(CompositeByteBuf buf) {
        // 声明泄漏跟踪器变量，用于关联和跟踪ByteBuf的生命周期
        ResourceLeakTracker<ByteBuf> leak;
        
        // 获取当前系统配置的泄漏检测级别，并根据不同级别采取不同处理
        switch (ResourceLeakDetector.getLevel()) {
            case SIMPLE:
                // 简单检测模式：仅记录泄漏发生的位置
                // 尝试为缓冲区创建泄漏跟踪器
                leak = AbstractByteBuf.leakDetector.track(buf);
                if (leak != null) {
                    // 如果成功创建跟踪器，用SimpleLeakAwareCompositeByteBuf包装原始缓冲区
                    // 这种包装类仅在缓冲区被GC但未正确释放时报告泄漏位置
                    buf = new SimpleLeakAwareCompositeByteBuf(buf, leak);
                }
                break;
            case ADVANCED:
            case PARANOID:
                // 高级检测模式：记录泄漏位置和缓冲区的分配、访问历史
                // 尝试为缓冲区创建泄漏跟踪器
                leak = AbstractByteBuf.leakDetector.track(buf);
                if (leak != null) {
                    // 如果成功创建跟踪器，用AdvancedLeakAwareCompositeByteBuf包装原始缓冲区
                    // 这种包装类会记录所有缓冲区操作，提供更详细的泄漏诊断信息
                    buf = new AdvancedLeakAwareCompositeByteBuf(buf, leak);
                }
                break;
            default:
                // DISABLED模式或其他未定义模式：不执行任何包装，直接使用原始缓冲区
                break;
        }
        
        // 返回处理后的缓冲区，可能是原始缓冲区或其包装类
        return buf;
    }

    private final boolean directByDefault;
    private final ByteBuf emptyBuf;

    /**
     * Instance use heap buffers by default
     */
    protected AbstractByteBufAllocator() {
        this(false);
    }

    /**
     * Create new instance
     *
     * @param preferDirect {@code true} if {@link #buffer(int)} should try to
     *                     allocate a direct buffer rather than
     *                     a heap buffer
     */
    protected AbstractByteBufAllocator(boolean preferDirect) {
        directByDefault = preferDirect && PlatformDependent.hasUnsafe();
        emptyBuf = new EmptyByteBuf(this);
    }

    @Override
    public ByteBuf buffer() {
        if (directByDefault) {
            return directBuffer();
        }
        return heapBuffer();
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        if (directByDefault) {
            return directBuffer(initialCapacity);
        }
        return heapBuffer(initialCapacity);
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        if (directByDefault) {
            return directBuffer(initialCapacity, maxCapacity);
        }
        return heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf ioBuffer() {
        if (PlatformDependent.hasUnsafe() || isDirectBufferPooled()) {
            return directBuffer(DEFAULT_INITIAL_CAPACITY);
        }
        return heapBuffer(DEFAULT_INITIAL_CAPACITY);
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity) {
        if (PlatformDependent.hasUnsafe() || isDirectBufferPooled()) {
            return directBuffer(initialCapacity);
        }
        return heapBuffer(initialCapacity);
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        if (PlatformDependent.hasUnsafe() || isDirectBufferPooled()) {
            return directBuffer(initialCapacity, maxCapacity);
        }
        return heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf heapBuffer() {
        return heapBuffer(DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY);
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        return heapBuffer(initialCapacity, DEFAULT_MAX_CAPACITY);
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        if (initialCapacity == 0 && maxCapacity == 0) {
            return emptyBuf;
        }
        validate(initialCapacity, maxCapacity);
        return newHeapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf directBuffer() {
        return directBuffer(DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        return directBuffer(initialCapacity, DEFAULT_MAX_CAPACITY);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        if (initialCapacity == 0 && maxCapacity == 0) {
            return emptyBuf;
        }
        validate(initialCapacity, maxCapacity);
        return newDirectBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        if (directByDefault) {
            return compositeDirectBuffer();
        }
        return compositeHeapBuffer();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        if (directByDefault) {
            return compositeDirectBuffer(maxNumComponents);
        }
        return compositeHeapBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        return compositeHeapBuffer(DEFAULT_MAX_COMPONENTS);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        return toLeakAwareBuffer(new CompositeByteBuf(this, false, maxNumComponents));
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        return compositeDirectBuffer(DEFAULT_MAX_COMPONENTS);
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        return toLeakAwareBuffer(new CompositeByteBuf(this, true, maxNumComponents));
    }

    private static void validate(int initialCapacity, int maxCapacity) {
        checkPositiveOrZero(initialCapacity, "initialCapacity");
        if (initialCapacity > maxCapacity) {
            throw new IllegalArgumentException(String.format(
                    "initialCapacity: %d (expected: not greater than maxCapacity(%d)",
                    initialCapacity, maxCapacity));
        }
    }

    /**
     * Create a heap {@link ByteBuf} with the given initialCapacity and maxCapacity.
     */
    protected abstract ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity);

    /**
     * Create a direct {@link ByteBuf} with the given initialCapacity and
     * maxCapacity.
     */
    protected abstract ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity);

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) + "(directByDefault: " + directByDefault + ')';
    }

    /**
     * 计算ByteBuf扩容时的新容量值。当ByteBuf需要扩容以容纳更多数据时，此方法根据特定算法确定合适的新容量。
     * <p>
     * 扩容策略遵循以下规则：
     * <ul>
     * <li>如果请求的最小新容量等于阈值(4 MiB)，则直接返回该值</li>
     * <li>如果请求的最小新容量大于阈值(4 MiB)，则按阈值对齐，即新容量为阈值的整数倍</li>
     * <li>如果请求的最小新容量小于阈值(4 MiB)，则返回大于等于最小新容量的最小2的幂次方值，且不小于64</li>
     * </ul>
     * </p>
     * <p>
     * 此算法设计目的是在小容量场景下快速扩容（使用2的幂次方），而在大容量场景下控制内存增长（使用固定增量）。
     * </p>
     *
     * @param minNewCapacity 要求的最小新容量
     * @param maxCapacity    允许的最大容量
     * @return 计算出的新容量，该值总是大于等于minNewCapacity且小于等于maxCapacity
     *
     * @throws IllegalArgumentException 如果minNewCapacity为负数或大于maxCapacity
     *
     * @see ByteBuf#capacity(int)
     * @see ByteBuf#ensureWritable(int)
     */
    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        // 检查minNewCapacity是否为非负数
        checkPositiveOrZero(minNewCapacity, "minNewCapacity");

        // 检查minNewCapacity是否超过了maxCapacity
        if (minNewCapacity > maxCapacity) {
            throw new IllegalArgumentException(String.format(
                    "minNewCapacity: %d (expected: not greater than maxCapacity(%d)",
                    minNewCapacity, maxCapacity));
        }

        // 定义阈值常量，4 MiB，用于区分不同的扩容策略
        final int threshold = CALCULATE_THRESHOLD; // 4 MiB page

        // 如果请求的容量恰好等于阈值，直接返回阈值
        if (minNewCapacity == threshold) {
            return threshold;
        }

        // 如果请求的容量大于阈值，采用阈值对齐的方式扩容
        if (minNewCapacity > threshold) {
            // 计算阈值的整数倍，向下取整
            int newCapacity = minNewCapacity / threshold * threshold;

            // 如果新容量超过了maxCapacity减去一个阈值的值，直接返回maxCapacity
            // 否则，增加一个阈值的大小，确保新容量足够大
            if (newCapacity > maxCapacity - threshold) {
                newCapacity = maxCapacity;
            } else {
                newCapacity += threshold;
            }

            return newCapacity;
        }

        // 处理小容量情况：
        // 1. 确保最小容量至少为64
        // 2. 找到大于等于minNewCapacity的最小2的幂次方值
        // 3. 确保不超过maxCapacity
        final int newCapacity = MathUtil.findNextPositivePowerOfTwo(Math.max(minNewCapacity, 64));
        return Math.min(newCapacity, maxCapacity);
    }
}
