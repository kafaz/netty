/*
 * Copyright 2014 The Netty Project
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
package io.netty.util.concurrent;

import io.netty.util.internal.InternalThreadLocalMap;
import io.netty.util.internal.PlatformDependent;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import static io.netty.util.internal.InternalThreadLocalMap.UNSET;
import static io.netty.util.internal.InternalThreadLocalMap.VARIABLES_TO_REMOVE_INDEX;

/**
 * 一个特殊的 {@link ThreadLocal} 变体，当从 {@link FastThreadLocalThread} 访问时提供更高的访问性能。
 * <p>
 * 内部实现中，{@link FastThreadLocal} 使用数组中的常量索引而非哈希码和哈希表来查找变量。
 * 尽管这种优化看似微小，但它提供了比使用哈希表更好的性能优势，特别是在频繁访问的场景下。
 * </p><p>
 * 为了充分利用这种线程局部变量，您的线程必须是 {@link FastThreadLocalThread} 或其子类型。
 * 默认情况下，所有由 {@link DefaultThreadFactory} 创建的线程都是 {@link FastThreadLocalThread}，
 * 正是因为这个原因。
 * </p><p>
 * 注意，快速路径只适用于扩展了 {@link FastThreadLocalThread} 的线程，因为它需要一个特殊字段来
 * 存储必要的状态。任何其他类型线程的访问会回退到常规的 {@link ThreadLocal}。
 * </p>
 * <p>
 * 与标准 {@link ThreadLocal} 相比的主要优势：
 * <ul>
 *   <li>使用数组索引而非哈希表，减少了查找开销</li>
 *   <li>减少了内存使用，特别是在大量线程局部变量的情况下</li>
 *   <li>提供了额外的工具方法，如 {@link #removeAll()} 和 {@link #destroy()}</li>
 *   <li>支持批量清理机制，方便在容器环境中管理资源</li>
 * </ul>
 * </p>
 *
 * @param <V> 线程局部变量的类型
 * @see ThreadLocal
 * @see FastThreadLocalThread
 * @see InternalThreadLocalMap
 * @since 4.1.0
 */
public class FastThreadLocal<V> {

    /**
     * 移除绑定到当前线程的所有 {@link FastThreadLocal} 变量。
     * <p>
     * 此操作在容器环境中特别有用，当您不想在不受管理的线程中留下线程局部变量时。
     * 例如，在应用服务器环境中，线程可能被重用于不同的应用程序，需要确保不会泄漏线程局部状态。
     * </p>
     * <p>
     * 该方法会：
     * <ol>
     *   <li>获取当前线程的 InternalThreadLocalMap</li>
     *   <li>检索并清理所有注册的 FastThreadLocal 变量</li>
     *   <li>最后移除整个 ThreadLocalMap</li>
     * </ol>
     * </p>
     */
    public static void removeAll() {
        // 获取当前线程的 ThreadLocalMap，如果不存在则直接返回
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return;
        }

        try {
            // 获取需要移除的变量集合
            Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
            if (v != null && v != InternalThreadLocalMap.UNSET) {
                @SuppressWarnings("unchecked")
                Set<FastThreadLocal<?>> variablesToRemove = (Set<FastThreadLocal<?>>) v;
                // 转换为数组以防止在迭代过程中的并发修改异常
                FastThreadLocal<?>[] variablesToRemoveArray =
                        variablesToRemove.toArray(new FastThreadLocal[0]);
                // 逐个移除所有注册的 FastThreadLocal 变量
                for (FastThreadLocal<?> tlv: variablesToRemoveArray) {
                    tlv.remove(threadLocalMap);
                }
            }
        } finally {
            // 确保最终移除整个 ThreadLocalMap，释放所有资源
            InternalThreadLocalMap.remove();
        }
    }

    /**
     * 返回绑定到当前线程的线程局部变量数量。
     * <p>
     * 此方法可用于诊断目的，帮助识别潜在的线程局部变量泄漏。
     * 如果特定线程的变量数量异常增长，可能表明存在资源管理问题。
     * </p>
     *
     * @return 当前线程中线程局部变量的数量，如果没有设置则返回0
     */
    public static int size() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return 0;
        } else {
            return threadLocalMap.size();
        }
    }

    /**
     * 销毁保存所有非 {@link FastThreadLocalThread} 线程访问的 {@link FastThreadLocal} 变量的数据结构。
     * <p>
     * 此操作在容器环境中特别有用，当您不想在不受管理的线程中留下线程局部变量时。
     * 当应用程序从容器中卸载时调用此方法。
     * </p>
     * <p>
     * 注意：此方法会清理所有非FastThreadLocalThread线程的全局数据结构，应谨慎使用，
     * 通常只在应用关闭时调用。
     * </p>
     */
    public static void destroy() {
        // 销毁内部数据结构，释放所有资源
        InternalThreadLocalMap.destroy();
    }

    /**
     * 将FastThreadLocal变量添加到"需要移除"的变量集合中。
     * <p>
     * 这是一个内部方法，用于在设置变量时将其注册到清理机制中，
     * 确保线程终止或显式调用removeAll()时能够清理所有变量。
     * </p>
     *
     * @param threadLocalMap 当前线程的ThreadLocalMap
     * @param variable 需要添加到移除集合的FastThreadLocal变量
     */
    @SuppressWarnings("unchecked")
    private static void addToVariablesToRemove(InternalThreadLocalMap threadLocalMap, FastThreadLocal<?> variable) {
        // 获取保存"待移除变量"的集合
        Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
        Set<FastThreadLocal<?>> variablesToRemove;
        
        // 如果集合不存在，创建一个新的基于IdentityHashMap的集合
        // IdentityHashMap使用==而非equals()比较键，更适合此场景
        if (v == InternalThreadLocalMap.UNSET || v == null) {
            variablesToRemove = Collections.newSetFromMap(new IdentityHashMap<FastThreadLocal<?>, Boolean>());
            threadLocalMap.setIndexedVariable(VARIABLES_TO_REMOVE_INDEX, variablesToRemove);
        } else {
            variablesToRemove = (Set<FastThreadLocal<?>>) v;
        }

        // 将变量添加到待移除集合
        variablesToRemove.add(variable);
    }

    /**
     * 从"需要移除"的变量集合中移除特定的FastThreadLocal变量。
     * <p>
     * 当变量被显式移除时调用此方法，确保不会在线程终止时尝试再次移除。
     * </p>
     *
     * @param threadLocalMap 当前线程的ThreadLocalMap
     * @param variable 需要从移除集合中删除的FastThreadLocal变量
     */
    private static void removeFromVariablesToRemove(
            InternalThreadLocalMap threadLocalMap, FastThreadLocal<?> variable) {

        // 获取保存"待移除变量"的集合
        Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);

        if (v == InternalThreadLocalMap.UNSET || v == null) {
            // 如果集合不存在，无需执行任何操作
            return;
        }

        @SuppressWarnings("unchecked")
        Set<FastThreadLocal<?>> variablesToRemove = (Set<FastThreadLocal<?>>) v;
        // 从集合中移除变量
        variablesToRemove.remove(variable);
    }

    /**
     * 此FastThreadLocal实例在InternalThreadLocalMap中的唯一索引。
     * 这是实现高性能访问的关键，允许O(1)时间复杂度的查找。
     */
    private final int index;

    /**
     * 创建一个新的FastThreadLocal实例。
     * <p>
     * 构造时会从InternalThreadLocalMap获取一个唯一的索引，
     * 该索引将用于所有后续的访问操作。
     * </p>
     */
    public FastThreadLocal() {
        // 分配一个唯一的索引，此索引在所有线程间共享
        index = InternalThreadLocalMap.nextVariableIndex();
    }

    /**
     * 返回当前线程的线程局部变量值。
     * <p>
     * 如果变量尚未初始化，将调用{@link #initialValue()}方法初始化变量。
     * 对于FastThreadLocalThread线程，此方法提供优化的访问路径。
     * </p>
     *
     * @return 当前线程的线程局部变量值
     */
    @SuppressWarnings("unchecked")
    public final V get() {
        // 获取当前线程的ThreadLocalMap
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
        // 使用索引直接访问变量，避免哈希查找
        Object v = threadLocalMap.indexedVariable(index);
        if (v != InternalThreadLocalMap.UNSET) {
            // 已初始化，直接返回
            return (V) v;
        }

        // 未初始化，调用initialize方法
        return initialize(threadLocalMap);
    }

    /**
     * 返回当前线程的线程局部变量值，如果存在；否则返回{@code null}。
     * <p>
     * 与{@link #get()}不同，此方法不会初始化变量，
     * 且当ThreadLocalMap尚未创建时不会创建它。
     * 适用于需要检查变量是否存在但不希望触发初始化的场景。
     * </p>
     *
     * @return 当前线程的线程局部变量值，如果不存在则返回null
     */
    @SuppressWarnings("unchecked")
    public final V getIfExists() {
        // 获取当前线程的ThreadLocalMap，但不创建
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap != null) {
            // 检查变量是否已初始化
            Object v = threadLocalMap.indexedVariable(index);
            if (v != InternalThreadLocalMap.UNSET) {
                return (V) v;
            }
        }
        // ThreadLocalMap不存在或变量未初始化
        return null;
    }

    /**
     * 返回指定线程局部映射的当前值。
     * <p>
     * 此方法用于内部优化，允许在已有ThreadLocalMap实例的情况下重用它。
     * 注意：指定的线程局部映射必须属于当前线程。
     * </p>
     *
     * @param threadLocalMap 要访问的线程局部映射，必须属于当前线程
     * @return 线程局部变量的值
     */
    @SuppressWarnings("unchecked")
    public final V get(InternalThreadLocalMap threadLocalMap) {
        // 使用索引直接访问变量
        Object v = threadLocalMap.indexedVariable(index);
        if (v != InternalThreadLocalMap.UNSET) {
            // 已初始化，直接返回
            return (V) v;
        }

        // 未初始化，调用initialize方法
        return initialize(threadLocalMap);
    }

    /**
     * 初始化当前线程的线程局部变量。
     * <p>
     * 此方法会调用{@link #initialValue()}获取初始值，然后将其存储在线程局部映射中，
     * 并将此变量添加到需要清理的变量集合中。
     * </p>
     *
     * @param threadLocalMap 当前线程的线程局部映射
     * @return 初始化后的变量值
     */
    private V initialize(InternalThreadLocalMap threadLocalMap) {
        V v = null;
        try {
            // 调用用户定义的初始化方法获取初始值
            v = initialValue();
            // 确保初始值不是UNSET标记
            if (v == InternalThreadLocalMap.UNSET) {
                throw new IllegalArgumentException("InternalThreadLocalMap.UNSET can not be initial value.");
            }
        } catch (Exception e) {
            // 将checked异常转换为unchecked异常重新抛出
            PlatformDependent.throwException(e);
        }

        // 设置初始值并注册到清理机制
        threadLocalMap.setIndexedVariable(index, v);
        addToVariablesToRemove(threadLocalMap, this);
        return v;
    }

    /**
     * 为当前线程设置线程局部变量的值。
     * <p>
     * 如果之前已设置过值，旧值将被覆盖。
     * </p>
     *
     * @param value 要设置的新值
     */
    public final void set(V value) {
        getAndSet(value);
    }

    /**
     * 为指定的线程局部映射设置线程局部变量的值。
     * <p>
     * 注意：指定的线程局部映射必须属于当前线程。
     * </p>
     *
     * @param threadLocalMap 要修改的线程局部映射，必须属于当前线程
     * @param value 要设置的新值
     */
    public final void set(InternalThreadLocalMap threadLocalMap, V value) {
        getAndSet(threadLocalMap, value);
    }

    /**
     * 设置当前线程的线程局部变量值，并返回旧值。
     * <p>
     * 如果新值等于UNSET特殊标记，将移除此变量。
     * </p>
     *
     * @param value 要设置的新值
     * @return 更新前的旧值，如果之前没有设置则返回null
     */
    public V getAndSet(V value) {
        if (value != InternalThreadLocalMap.UNSET) {
            // 正常设置新值
            InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
            return setKnownNotUnset(threadLocalMap, value);
        }
        // 如果值为UNSET，执行移除操作
        return removeAndGet(InternalThreadLocalMap.getIfSet());
    }

    /**
     * 设置指定线程局部映射的线程局部变量值，并返回旧值。
     * <p>
     * 注意：指定的线程局部映射必须属于当前线程。
     * </p>
     *
     * @param threadLocalMap 要修改的线程局部映射，必须属于当前线程
     * @param value 要设置的新值
     * @return 更新前的旧值，如果之前没有设置则返回null
     */
    public V getAndSet(InternalThreadLocalMap threadLocalMap, V value) {
        if (value != InternalThreadLocalMap.UNSET) {
            // 正常设置新值
            return setKnownNotUnset(threadLocalMap, value);
        }
        // 如果值为UNSET，执行移除操作
        return removeAndGet(threadLocalMap);
    }

    /**
     * 设置非UNSET值到线程局部映射中，并返回旧值。
     * <p>
     * 此方法是一个内部优化，用于处理已知不是UNSET的值。
     * 参见 {@link InternalThreadLocalMap#setIndexedVariable(int, Object)}。
     * </p>
     *
     * @param threadLocalMap 要修改的线程局部映射
     * @param value 要设置的新值，已确保不是UNSET
     * @return 更新前的旧值，如果之前没有设置则返回null
     */
    @SuppressWarnings("unchecked")
    private V setKnownNotUnset(InternalThreadLocalMap threadLocalMap, V value) {
        // 原子地设置新值并返回旧值
        V old = (V) threadLocalMap.getAndSetIndexedVariable(index, value);
        if (old == UNSET) {
            // 如果这是首次设置，将变量添加到清理机制
            addToVariablesToRemove(threadLocalMap, this);
            return null;
        }
        return old;
    }

    /**
     * 检查当前线程的线程局部变量是否已设置。
     * <p>
     * 此方法可用于在不触发初始化的情况下检查变量是否存在。
     * </p>
     *
     * @return 如果当前线程的线程局部变量已设置，则返回true
     */
    public final boolean isSet() {
        return isSet(InternalThreadLocalMap.getIfSet());
    }

    /**
     * 检查指定线程局部映射中的线程局部变量是否已设置。
     * <p>
     * 注意：指定的线程局部映射必须属于当前线程。
     * </p>
     *
     * @param threadLocalMap 要检查的线程局部映射，必须属于当前线程
     * @return 如果变量已设置，则返回true
     */
    public final boolean isSet(InternalThreadLocalMap threadLocalMap) {
        // 检查ThreadLocalMap是否存在，以及变量是否已设置
        return threadLocalMap != null && threadLocalMap.isIndexedVariableSet(index);
    }
    
    /**
     * 将当前线程的线程局部变量值设置为未初始化状态，并返回旧值。
     * <p>
     * 调用此方法后，任何对{@link #get()}的后续调用都将触发{@link #initialValue()}的新调用。
     * </p>
     */
    public final void remove() {
        remove(InternalThreadLocalMap.getIfSet());
    }

    /**
     * 将指定线程局部映射中的线程局部变量值设置为未初始化状态。
     * <p>
     * 调用此方法后，任何对{@link #get()}的后续调用都将触发{@link #initialValue()}的新调用。
     * 注意：指定的线程局部映射必须属于当前线程。
     * </p>
     *
     * @param threadLocalMap 要修改的线程局部映射，必须属于当前线程
     */
    @SuppressWarnings("unchecked")
    public final void remove(InternalThreadLocalMap threadLocalMap) {
        removeAndGet(threadLocalMap);
    }

    /**
     * 从线程局部映射中移除变量并返回其值。
     * <p>
     * 此方法会从线程局部映射中移除变量，从清理机制中注销它，
     * 并调用{@link #onRemoval(Object)}方法，允许资源清理。
     * </p>
     *
     * @param threadLocalMap 要修改的线程局部映射
     * @return 移除前的值，如果没有设置则返回null
     */
    @SuppressWarnings("unchecked")
    private V removeAndGet(InternalThreadLocalMap threadLocalMap) {
        if (threadLocalMap == null) {
            // ThreadLocalMap不存在，无需操作
            return null;
        }

        // 移除变量并获取旧值
        Object v = threadLocalMap.removeIndexedVariable(index);
        if (v != InternalThreadLocalMap.UNSET) {
            // 从清理机制中移除
            removeFromVariablesToRemove(threadLocalMap, this);
            try {
                // 调用用户定义的清理回调
                onRemoval((V) v);
            } catch (Exception e) {
                // 将checked异常转换为unchecked异常重新抛出
                PlatformDependent.throwException(e);
            }
            return (V) v;
        }
        return null;
    }

    /**
     * 返回此线程局部变量的初始值。
     * <p>
     * 此方法在第一次调用{@link #get()}时调用，或在调用{@link #remove()}后再次调用{@link #get()}时调用。
     * 默认实现返回null，子类应该根据需要重写此方法。
     * </p>
     *
     * @return 线程局部变量的初始值
     * @throws Exception 初始化过程中可能抛出的异常
     */
    protected V initialValue() throws Exception {
        return null;
    }

    /**
     * 当通过{@link #remove()}移除此线程局部变量时调用。
     * <p>
     * 注意：{@link #remove()}不保证在线程完成时调用，
     * 这意味着您不能依赖此方法来清理线程完成时的资源。
     * 如需在线程终止时确保资源清理，应考虑其他机制如try-with-resources或显式关闭。
     * </p>
     *
     * @param value 被移除的值
     * @throws Exception 清理过程中可能抛出的异常
     */
    protected void onRemoval(@SuppressWarnings("UnusedParameters") V value) throws Exception { }
}
