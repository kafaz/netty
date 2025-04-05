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
package io.netty.util.concurrent;

/**
 * 一个特殊的 {@link ProgressiveFuture} 接口，提供可写入操作结果和进度更新的能力。
 * <p>
 * ProgressivePromise 将 {@link Promise} 的可写特性与 {@link ProgressiveFuture} 的进度报告功能结合在一起，
 * 形成了一个完整的可写进度式异步结果容器。此接口适用于需要报告执行过程中间进度的长时间运行异步操作，
 * 如文件传输、大型数据处理等场景。
 * <p>
 * 通过 ProgressivePromise，操作的执行者可以：
 * <ul>
 *   <li>设置操作的进度和总量信息</li>
 *   <li>最终标记操作为成功或失败</li>
 *   <li>安全地通知所有已注册的监听器有关进度更新和最终完成事件</li>
 * </ul>
 * <p>
 * 监听器可以分为两类：
 * <ul>
 *   <li>普通 {@link GenericFutureListener} - 仅在操作最终完成时收到通知</li>
 *   <li>{@link GenericProgressiveFutureListener} - 不仅在操作完成时收到通知，还在进度更新时收到通知</li>
 * </ul>
 *
 * @param <V> 异步操作最终结果的类型
 *
 * @see Promise
 * @see ProgressiveFuture
 * @see GenericProgressiveFutureListener
 */
public interface ProgressivePromise<V> extends Promise<V>, ProgressiveFuture<V> {

    /**
     * 设置异步操作的当前进度并通知实现了 {@link GenericProgressiveFutureListener} 的监听器。
     * <p>
     * 此方法用于更新异步操作的当前执行进度，并触发所有已注册的进度监听器。它适用于
     * 需要报告中间状态的长时间运行操作，如文件上传、下载或数据处理。
     * <p>
     * 如果操作已经完成（成功、失败或取消），此方法将抛出 {@link IllegalStateException}。
     * <p>
     * 关于参数值的规则：
     * <ul>
     *   <li>当 total &lt; 0 时（通常为 -1），表示总进度未知，此时 progress 必须 &gt;= 0</li>
     *   <li>当 total &gt;= 0 时，progress 必须在 0 到 total 的范围内（包含边界值）</li>
     * </ul>
     *
     * @param progress 当前已完成的进度值
     * @param total 总进度值，如果未知则使用负数（通常为 -1）
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     * @throws IllegalStateException 如果此 Promise 已经完成
     * @throws IllegalArgumentException 如果提供的进度值无效
     */
    ProgressivePromise<V> setProgress(long progress, long total);

    /**
     * 尝试设置异步操作的当前进度并通知实现了 {@link GenericProgressiveFutureListener} 的监听器。
     * <p>
     * 与 {@link #setProgress(long, long)} 不同，此方法在操作已经完成或进度值超出范围时不会抛出异常，
     * 而是返回 {@code false} 表示进度设置失败。这使得在不确定当前状态是否允许设置进度的情况下可以安全地调用此方法。
     * <p>
     * 如果方法返回 {@code true}，表示进度已成功更新并且相关监听器已被通知。
     * <p>
     * 关于参数值的规则：
     * <ul>
     *   <li>当 total &lt; 0 时（通常为 -1），表示总进度未知，此时 progress 必须 &gt;= 0</li>
     *   <li>当 total &gt;= 0 时，progress 必须在 0 到 total 的范围内（包含边界值）</li>
     * </ul>
     *
     * @param progress 当前已完成的进度值
     * @param total 总进度值，如果未知则使用负数（通常为 -1）
     * @return {@code true} 如果进度更新成功；{@code false} 如果此 Promise 已完成或进度值无效
     */
    boolean tryProgress(long progress, long total);

    /**
     * 将此 Promise 标记为成功完成，并设置结果值。
     * <p>
     * 此方法会通知所有已注册的监听器操作已成功完成。调用此方法后，进度更新将不再被接受。
     *
     * @param result 异步操作的结果值，可以为 {@code null}
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     * @throws IllegalStateException 如果此 Promise 已经完成
     */
    @Override
    ProgressivePromise<V> setSuccess(V result);

    /**
     * 将此 Promise 标记为失败完成，并设置失败原因。
     * <p>
     * 此方法会通知所有已注册的监听器操作已失败。调用此方法后，进度更新将不再被接受。
     *
     * @param cause 异步操作失败的原因，不能为 {@code null}
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     * @throws IllegalStateException 如果此 Promise 已经完成
     * @throws NullPointerException 如果 cause 为 null
     */
    @Override
    ProgressivePromise<V> setFailure(Throwable cause);

    /**
     * 添加一个监听器，当进度更新或操作完成时将收到通知。
     * <p>
     * 如果添加的监听器实现了 {@link GenericProgressiveFutureListener} 接口，则在进度更新时也会收到通知。
     * 否则，监听器只会在操作最终完成时收到通知。
     *
     * @param listener 要添加的监听器
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     */
    @Override
    ProgressivePromise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener);

    /**
     * 添加多个监听器，当进度更新或操作完成时将收到通知。
     * <p>
     * 如果添加的监听器实现了 {@link GenericProgressiveFutureListener} 接口，则在进度更新时也会收到通知。
     * 否则，监听器只会在操作最终完成时收到通知。
     *
     * @param listeners 要添加的监听器数组
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     */
    @Override
    ProgressivePromise<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

    /**
     * 移除一个之前添加的监听器。
     *
     * @param listener 要移除的监听器
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     */
    @Override
    ProgressivePromise<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener);

    /**
     * 移除多个之前添加的监听器。
     *
     * @param listeners 要移除的监听器数组
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     */
    @Override
    ProgressivePromise<V> removeListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

    /**
     * 等待此 Promise 完成，如果未完成则阻塞当前线程直到完成。
     * <p>
     * 此方法不会等待进度更新，只会等待最终完成状态。
     *
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     * @throws InterruptedException 如果当前线程在等待时被中断
     */
    @Override
    ProgressivePromise<V> await() throws InterruptedException;

    /**
     * 等待此 Promise 完成，如果未完成则阻塞当前线程直到完成。不响应中断。
     * <p>
     * 此方法不会等待进度更新，只会等待最终完成状态。
     *
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     */
    @Override
    ProgressivePromise<V> awaitUninterruptibly();

    /**
     * 等待此 Promise 完成，如果未完成则阻塞当前线程直到完成。
     * <p>
     * 如果 Promise 因异常而失败，此方法将抛出包装了失败原因的异常。
     * 此方法不会等待进度更新，只会等待最终完成状态。
     *
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     * @throws InterruptedException 如果当前线程在等待时被中断
     */
    @Override
    ProgressivePromise<V> sync() throws InterruptedException;

    /**
     * 等待此 Promise 完成，如果未完成则阻塞当前线程直到完成。不响应中断。
     * <p>
     * 如果 Promise 因异常而失败，此方法将抛出包装了失败原因的异常。
     * 此方法不会等待进度更新，只会等待最终完成状态。
     *
     * @return 此 ProgressivePromise 实例，支持方法链式调用
     */
    @Override
    ProgressivePromise<V> syncUninterruptibly();
}
