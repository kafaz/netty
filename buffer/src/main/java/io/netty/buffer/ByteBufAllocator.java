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

/**
 * 实现此接口的类负责分配缓冲区。此接口的实现应该是线程安全的。
 * <p>
 * ByteBufAllocator是Netty内存管理系统的核心接口，负责创建和管理{@link ByteBuf}实例。
 * 它提供了各种内存分配策略，包括堆内存(heap)、直接内存(direct)和复合缓冲区(composite)。
 * </p>
 * 
 * <h3>内存类型</h3>
 * <ul>
 *   <li><b>堆内存(Heap)</b>: 由JVM管理的内存，位于Java堆中，受GC管理</li>
 *   <li><b>直接内存(Direct)</b>: 位于JVM堆外的本地内存，不受GC直接管理，适合IO操作</li>
 *   <li><b>复合缓冲区(Composite)</b>: 由多个缓冲区组成的虚拟缓冲区，减少内存复制</li>
 * </ul>
 * 
 * <h3>常用实现</h3>
 * <ul>
 *   <li>{@code PooledByteBufAllocator}: 默认实现，使用内存池提高性能并减少内存碎片</li>
 *   <li>{@code UnpooledByteBufAllocator}: 非池化实现，每次分配创建新的缓冲区</li>
 * </ul>
 * 
 * <h3>使用建议</h3>
 * <p>
 * 通常情况下应使用{@link #DEFAULT}实例，除非有特殊需求。使用内存池可以显著提高性能，
 * 特别是在高并发场景下。对于IO密集型应用，优先使用直接内存；对于CPU密集型或短生命周期应用，
 * 可考虑使用堆内存。
 * </p>
 * 
 * @see ByteBuf
 * @see CompositeByteBuf
 * @see PooledByteBufAllocator
 * @see UnpooledByteBufAllocator
 */
public interface ByteBufAllocator {

    /**
     * 默认的全局ByteBufAllocator实例。
     * <p>
     * 这是一个静态变量，通常是{@link PooledByteBufAllocator#DEFAULT}的实例。
     * 在大多数情况下，应用程序可以直接使用此实例而无需创建自己的分配器。
     * </p>
     */
    ByteBufAllocator DEFAULT = ByteBufUtil.DEFAULT_ALLOCATOR;

    /**
     * 分配一个{@link ByteBuf}。具体是直接内存还是堆内存取决于实现类。
     * <p>
     * 这个方法使用默认的初始容量和最大容量创建缓冲区。适用于不确定具体内存需求的场景。
     * </p>
     * 
     * @return 新分配的ByteBuf实例
     */
    ByteBuf buffer();

    /**
     * 使用指定的初始容量分配一个{@link ByteBuf}。具体是直接内存还是堆内存取决于实现类。
     * <p>
     * 初始容量指定了缓冲区初始分配的内存大小，可根据需要自动扩容直到默认的最大容量。
     * </p>
     * 
     * @param initialCapacity 初始容量（字节数）
     * @return 新分配的具有指定初始容量的ByteBuf实例
     * @throws IllegalArgumentException 如果initialCapacity为负数
     */
    ByteBuf buffer(int initialCapacity);

    /**
     * 使用指定的初始容量和最大容量分配一个{@link ByteBuf}。具体是直接内存还是堆内存取决于实现类。
     * <p>
     * 这个方法允许精确控制缓冲区的初始大小和增长限制，适用于对内存使用有严格要求的场景。
     * 当缓冲区需要增长超过maxCapacity时，会抛出异常。
     * </p>
     * 
     * @param initialCapacity 初始容量（字节数）
     * @param maxCapacity 最大容量（字节数）
     * @return 新分配的具有指定初始容量和最大容量的ByteBuf实例
     * @throws IllegalArgumentException 如果initialCapacity或maxCapacity为负数，或者initialCapacity大于maxCapacity
     */
    ByteBuf buffer(int initialCapacity, int maxCapacity);

    /**
     * 分配一个{@link ByteBuf}，优先使用适合I/O操作的直接内存。
     * <p>
     * 此方法创建的缓冲区特别适合网络传输和文件操作，因为直接内存可以避免在Java堆和本地内存之间的拷贝。
     * 具体实现会根据平台特性和配置来决定是否真正使用直接内存。
     * </p>
     * 
     * @return 新分配的适合I/O操作的ByteBuf实例
     */
    ByteBuf ioBuffer();

    /**
     * 使用指定的初始容量分配一个{@link ByteBuf}，优先使用适合I/O操作的直接内存。
     * <p>
     * 此方法创建的缓冲区特别适合网络传输和文件操作，并指定了初始分配的大小。
     * </p>
     * 
     * @param initialCapacity 初始容量（字节数）
     * @return 新分配的具有指定初始容量且适合I/O操作的ByteBuf实例
     * @throws IllegalArgumentException 如果initialCapacity为负数
     */
    ByteBuf ioBuffer(int initialCapacity);

    /**
     * 使用指定的初始容量和最大容量分配一个{@link ByteBuf}，优先使用适合I/O操作的直接内存。
     * <p>
     * 此方法创建的缓冲区特别适合网络传输和文件操作，并允许精确控制内存使用限制。
     * </p>
     * 
     * @param initialCapacity 初始容量（字节数）
     * @param maxCapacity 最大容量（字节数）
     * @return 新分配的具有指定初始容量和最大容量且适合I/O操作的ByteBuf实例
     * @throws IllegalArgumentException 如果initialCapacity或maxCapacity为负数，或者initialCapacity大于maxCapacity
     */
    ByteBuf ioBuffer(int initialCapacity, int maxCapacity);

    /**
     * 分配一个堆内存{@link ByteBuf}。
     * <p>
     * 堆内存缓冲区由JVM直接管理，适合需要频繁访问内容或生命周期较短的场景。
     * 它的读写速度通常比直接内存快，但进行I/O操作时需要额外的内存拷贝。
     * </p>
     * 
     * @return 新分配的堆内存ByteBuf实例
     */
    ByteBuf heapBuffer();

    /**
     * 使用指定的初始容量分配一个堆内存{@link ByteBuf}。
     * <p>
     * 此方法创建堆内存缓冲区并指定初始大小，适合已知大致数据量的场景。
     * </p>
     * 
     * @param initialCapacity 初始容量（字节数）
     * @return 新分配的具有指定初始容量的堆内存ByteBuf实例
     * @throws IllegalArgumentException 如果initialCapacity为负数
     */
    ByteBuf heapBuffer(int initialCapacity);

    /**
     * 使用指定的初始容量和最大容量分配一个堆内存{@link ByteBuf}。
     * <p>
     * 此方法创建堆内存缓冲区并精确控制其容量范围，适合对内存增长有严格限制的场景。
     * </p>
     * 
     * @param initialCapacity 初始容量（字节数）
     * @param maxCapacity 最大容量（字节数）
     * @return 新分配的具有指定初始容量和最大容量的堆内存ByteBuf实例
     * @throws IllegalArgumentException 如果initialCapacity或maxCapacity为负数，或者initialCapacity大于maxCapacity
     */
    ByteBuf heapBuffer(int initialCapacity, int maxCapacity);

    /**
     * 分配一个直接内存{@link ByteBuf}。
     * <p>
     * 直接内存缓冲区位于JVM堆外，不受GC直接管理，特别适合I/O操作。
     * 它可以避免在Java堆和本地内存之间的拷贝，提高I/O性能。
     * 但创建和销毁的开销较大，最适合长生命周期的场景。
     * </p>
     * 
     * @return 新分配的直接内存ByteBuf实例
     */
    ByteBuf directBuffer();

    /**
     * 使用指定的初始容量分配一个直接内存{@link ByteBuf}。
     * <p>
     * 此方法创建直接内存缓冲区并指定初始大小，适合已知大致数据量的I/O场景。
     * </p>
     * 
     * @param initialCapacity 初始容量（字节数）
     * @return 新分配的具有指定初始容量的直接内存ByteBuf实例
     * @throws IllegalArgumentException 如果initialCapacity为负数
     */
    ByteBuf directBuffer(int initialCapacity);

    /**
     * 使用指定的初始容量和最大容量分配一个直接内存{@link ByteBuf}。
     * <p>
     * 此方法创建直接内存缓冲区并精确控制其容量范围，适合对内存增长有严格限制的I/O场景。
     * </p>
     * 
     * @param initialCapacity 初始容量（字节数）
     * @param maxCapacity 最大容量（字节数）
     * @return 新分配的具有指定初始容量和最大容量的直接内存ByteBuf实例
     * @throws IllegalArgumentException 如果initialCapacity或maxCapacity为负数，或者initialCapacity大于maxCapacity
     */
    ByteBuf directBuffer(int initialCapacity, int maxCapacity);

    /**
     * 分配一个{@link CompositeByteBuf}复合缓冲区。具体是直接内存还是堆内存取决于实现类。
     * <p>
     * 复合缓冲区是一个虚拟缓冲区，由多个ByteBuf组合而成，可以避免不必要的内存复制。
     * 它特别适合需要将多个缓冲区逻辑上合并的场景，如HTTP消息头和消息体的合并。
     * </p>
     * 
     * @return 新分配的复合缓冲区实例
     */
    CompositeByteBuf compositeBuffer();

    /**
     * 分配一个具有指定最大组件数的{@link CompositeByteBuf}复合缓冲区。具体是直接内存还是堆内存取决于实现类。
     * <p>
     * 此方法创建复合缓冲区并限制其可包含的最大组件数量，适合已知组件数上限的场景。
     * </p>
     * 
     * @param maxNumComponents 可以存储的最大组件数量
     * @return 新分配的具有指定最大组件数的复合缓冲区实例
     * @throws IllegalArgumentException 如果maxNumComponents为负数或零
     */
    CompositeByteBuf compositeBuffer(int maxNumComponents);

    /**
     * 分配一个堆内存{@link CompositeByteBuf}复合缓冲区。
     * <p>
     * 此方法创建的复合缓冲区使用堆内存存储组件，适合需要频繁访问或生命周期较短的场景。
     * </p>
     * 
     * @return 新分配的堆内存复合缓冲区实例
     */
    CompositeByteBuf compositeHeapBuffer();

    /**
     * 分配一个具有指定最大组件数的堆内存{@link CompositeByteBuf}复合缓冲区。
     * <p>
     * 此方法创建堆内存复合缓冲区并限制其可包含的最大组件数量。
     * </p>
     * 
     * @param maxNumComponents 可以存储的最大组件数量
     * @return 新分配的具有指定最大组件数的堆内存复合缓冲区实例
     * @throws IllegalArgumentException 如果maxNumComponents为负数或零
     */
    CompositeByteBuf compositeHeapBuffer(int maxNumComponents);

    /**
     * 分配一个直接内存{@link CompositeByteBuf}复合缓冲区。
     * <p>
     * 此方法创建的复合缓冲区使用直接内存存储组件，特别适合I/O操作密集场景。
     * </p>
     * 
     * @return 新分配的直接内存复合缓冲区实例
     */
    CompositeByteBuf compositeDirectBuffer();

    /**
     * 分配一个具有指定最大组件数的直接内存{@link CompositeByteBuf}复合缓冲区。
     * <p>
     * 此方法创建直接内存复合缓冲区并限制其可包含的最大组件数量。
     * </p>
     * 
     * @param maxNumComponents 可以存储的最大组件数量
     * @return 新分配的具有指定最大组件数的直接内存复合缓冲区实例
     * @throws IllegalArgumentException 如果maxNumComponents为负数或零
     */
    CompositeByteBuf compositeDirectBuffer(int maxNumComponents);

    /**
     * 返回直接内存{@link ByteBuf}是否使用内存池。
     * <p>
     * 池化的直接内存分配器可以显著提高性能并减少内存碎片，特别是在高频率分配和释放的场景下。
     * 此方法可用于确定当前分配器的实现是否对直接内存进行池化管理。
     * </p>
     * 
     * @return 如果直接内存缓冲区是池化的则返回{@code true}，否则返回{@code false}
     */
    boolean isDirectBufferPooled();

    /**
     * 计算{@link ByteBuf}需要扩容时的新容量。
     * <p>
     * 当ByteBuf需要扩容至少到minNewCapacity大小，且不超过maxCapacity上限时，
     * 此方法用于计算实际的新容量值。不同的实现可能有不同的扩容策略，如倍增或固定增量。
     * </p>
     * 
     * @param minNewCapacity 最小新容量（字节数）
     * @param maxCapacity 最大允许容量（字节数）
     * @return 计算得到的新容量值
     * @throws IllegalArgumentException 如果minNewCapacity为负数，或者minNewCapacity大于maxCapacity
     */
    int calculateNewCapacity(int minNewCapacity, int maxCapacity);
 }
