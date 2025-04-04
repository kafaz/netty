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

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

/**
 * 异步操作的结果表示。
 * <p>
 * Future接口是Netty异步编程模型的核心组件，代表一个尚未完成但将来会完成的操作。
 * 相比Java标准库的Future，Netty的Future提供了更丰富的功能，包括链式调用、监听器注册、
 * 同步等待以及非阻塞结果获取等增强特性。
 * </p>
 * 
 * <h2>主要特性</h2>
 * <ul>
 * <li>监听器支持 - 注册回调而非阻塞等待</li>
 * <li>链式调用 - 流畅的API设计</li>
 * <li>同步原语 - 在需要时可以阻塞等待完成</li>
 * <li>完成状态检查 - 详细的操作结果状态</li>
 * </ul>
 * 
 * <h2>典型用法</h2>
 * 
 * <pre>
 * // 非阻塞方式处理结果
 * Future&lt;Object&gt; future = doSomethingAsync();
 * future.addListener(f -> {
 *     if (f.isSuccess()) {
 *         // 处理成功结果
 *         Object result = f.getNow();
 *         processResult(result);
 *     } else {
 *         // 处理失败
 *         Throwable cause = f.cause();
 *         handleError(cause);
 *     }
 * });
 * 
 * // 阻塞方式等待结果
 * Future&lt;Object&gt; future = doSomethingElseAsync();
 * try {
 *     future.await(); // 等待完成
 *     if (future.isSuccess()) {
 *         Object result = future.getNow();
 *         processResult(result);
 *     }
 * } catch (InterruptedException e) {
 *     Thread.currentThread().interrupt();
 * }
 * </pre>
 * 
 * @param <V> 异步操作结果的类型
 */
@SuppressWarnings("ClassNameSameAsAncestorName")
public interface Future<V> extends java.util.concurrent.Future<V> {

    /**
     * 判断I/O操作是否成功完成。
     * <p>
     * 此方法用于检查异步操作是否成功，它仅在操作完成后才有意义。
     * 如果操作尚未完成，总是返回false。
     * </p>
     * 
     * @return 如果操作成功完成则返回true，否则返回false
     */
    boolean isSuccess();

    /**
     * 判断操作是否可以通过{@link #cancel(boolean)}方法取消。
     * <p>
     * 某些操作一旦开始就不能被取消，此方法可用于在尝试取消前检查操作是否支持取消。
     * </p>
     * 
     * @return 如果操作可以被取消则返回true，否则返回false
     */
    boolean isCancellable();

    /**
     * 返回I/O操作失败的原因（如果操作已失败）。
     * <p>
     * 此方法用于获取异步操作失败的详细原因，便于错误处理和日志记录。
     * 如果操作成功或尚未完成，则返回null。
     * </p>
     * 
     * @return 失败的原因。如果操作成功或尚未完成，则返回null
     */
    Throwable cause();

    /**
     * 向此Future添加指定的监听器。当Future {@linkplain #isDone() 完成}时，
     * 指定的监听器将被通知。如果此Future已经完成，指定的监听器将立即被通知。
     * <p>
     * 这是Netty异步编程模型的核心方法，允许以非阻塞方式处理操作完成事件。
     * 监听器总是在Future完成的同一个事件循环中被调用，这保证了线程安全。
     * </p>
     * 
     * @param listener 要添加的监听器
     * @return this Future，用于链式调用
     */
    Future<V> addListener(GenericFutureListener<? extends Future<? super V>> listener);

    /**
     * 向此Future添加指定的多个监听器。当Future {@linkplain #isDone() 完成}时，
     * 指定的监听器将被通知。如果此Future已经完成，指定的监听器将立即被通知。
     * <p>
     * 监听器的通知顺序与它们添加的顺序相同。
     * </p>
     * 
     * @param listeners 要添加的监听器数组
     * @return this Future，用于链式调用
     */
    Future<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

    /**
     * 从此Future中移除指定监听器的第一个匹配项。被移除的监听器将不再在此Future
     * {@linkplain #isDone() 完成}时收到通知。如果指定的监听器未与此Future关联，
     * 此方法不执行任何操作并静默返回。
     * <p>
     * 此方法用于取消之前注册的回调，通常用于资源清理或取消操作。
     * </p>
     * 
     * @param listener 要移除的监听器
     * @return this Future，用于链式调用
     */
    Future<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener);

    /**
     * 从此Future中移除每个指定监听器的第一个匹配项。被移除的监听器将不再在此Future
     * {@linkplain #isDone() 完成}时收到通知。如果指定的监听器未与此Future关联，
     * 此方法不执行任何操作并静默返回。
     * 
     * @param listeners 要移除的监听器数组
     * @return this Future，用于链式调用
     */
    Future<V> removeListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

    /**
     * 等待此Future直到完成，如果Future失败，则重新抛出失败原因。
     * <p>
     * 此方法提供了一种同步等待操作完成的便捷方式，同时能自动处理异常情况。
     * 如果操作失败，异常会被重新抛出，简化了错误处理。
     * </p>
     * 
     * @return this Future，用于链式调用
     * @throws InterruptedException 如果当前线程在等待过程中被中断
     */
    Future<V> sync() throws InterruptedException;

    /**
     * 等待此Future直到完成，如果Future失败，则重新抛出失败原因。
     * <p>
     * 与{@link #sync()}类似，但会忽略线程中断，适用于不能处理InterruptedException的场景。
     * </p>
     * 
     * @return this Future，用于链式调用
     */
    Future<V> syncUninterruptibly();

    /**
     * 等待此Future完成。
     * <p>
     * 此方法会阻塞调用线程直到异步操作完成，无论成功还是失败。
     * 与sync()方法不同，此方法不会在操作失败时抛出异常，需要调用者自行检查结果状态。
     * </p>
     * 
     * @return this Future，用于链式调用
     * @throws InterruptedException 如果当前线程在等待过程中被中断
     */
    Future<V> await() throws InterruptedException;

    /**
     * 等待此Future完成，不响应中断。
     * <p>
     * 此方法捕获{@link InterruptedException}并静默忽略。
     * 对于必须等待操作完成且不能处理中断的场景非常有用。
     * </p>
     * 
     * @return this Future，用于链式调用
     */
    Future<V> awaitUninterruptibly();

    /**
     * 在指定的时间限制内等待此Future完成。
     * <p>
     * 此方法允许设置最大等待时间，避免长时间阻塞。
     * 如果在超时前完成，返回true；否则返回false。
     * </p>
     * 
     * @param timeout 最大等待时间
     * @param unit    时间单位
     * @return 如果且仅当Future在指定时间限制内完成，则返回true
     * @throws InterruptedException 如果当前线程在等待过程中被中断
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 在指定的时间限制（毫秒）内等待此Future完成。
     * <p>
     * 此方法是{@link #await(long, TimeUnit)}的便捷形式，直接使用毫秒作为时间单位。
     * </p>
     * 
     * @param timeoutMillis 最大等待时间（毫秒）
     * @return 如果且仅当Future在指定时间限制内完成，则返回true
     * @throws InterruptedException 如果当前线程在等待过程中被中断
     */
    boolean await(long timeoutMillis) throws InterruptedException;

    /**
     * 在指定的时间限制内等待此Future完成，不响应中断。
     * <p>
     * 此方法捕获{@link InterruptedException}并静默忽略，适合不能处理中断的场景。
     * </p>
     * 
     * @param timeout 最大等待时间
     * @param unit    时间单位
     * @return 如果且仅当Future在指定时间限制内完成，则返回true
     */
    boolean awaitUninterruptibly(long timeout, TimeUnit unit);

    /**
     * 在指定的时间限制（毫秒）内等待此Future完成，不响应中断。
     * <p>
     * 此方法是{@link #awaitUninterruptibly(long, TimeUnit)}的便捷形式，
     * 直接使用毫秒作为时间单位。
     * </p>
     * 
     * @param timeoutMillis 最大等待时间（毫秒）
     * @return 如果且仅当Future在指定时间限制内完成，则返回true
     */
    boolean awaitUninterruptibly(long timeoutMillis);

    /**
     * 不阻塞地返回结果。如果Future尚未完成，则返回{@code null}。
     * <p>
     * 由于可能使用{@code null}值标记Future成功，因此还需要使用{@link #isDone()}
     * 检查Future是否真正完成，而不只是依赖返回的{@code null}值判断。
     * </p>
     * <p>
     * 此方法适用于需要立即获取结果而不能等待的场景，如UI线程或非阻塞设计。
     * </p>
     * 
     * @return 操作结果，如果尚未完成则返回null
     */
    V getNow();

    /**
     * {@inheritDoc}
     * 
     * <p>
     * 如果取消成功，将使用{@link CancellationException}使Future失败。
     * 这与标准Java Future的行为一致，但Netty提供了更丰富的机制来处理取消结果。
     * </p>
     * 
     * @param mayInterruptIfRunning 如果执行该操作的线程应该被中断，则为true；
     *                              否则允许正在进行的任务完成
     * @return 如果任务无法取消（通常是因为已经完成），则返回false；
     *         否则返回true
     */
    @Override
    boolean cancel(boolean mayInterruptIfRunning);
}
