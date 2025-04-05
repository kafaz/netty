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
 * 一个特殊的 {@link Future} 接口，提供可写入操作结果的能力。
 * <p>
 * 与仅提供读取能力的 {@link Future} 不同，{@link Promise} 允许设置操作结果（成功或失败），
 * 并在结果设置后通知所有注册的监听器。这使其成为创建和完成异步操作的理想工具。
 * <p>
 * Promise 可以被视为异步操作的"写入端"，而 Future 是"读取端"。通常，执行异步操作的
 * 代码会持有 Promise 引用以设置结果，而等待操作完成的代码则使用 Future 接口来获取结果。
 * <p>
 * 每个 Promise 只能被完成一次，无论是成功、失败还是取消。任何尝试多次完成 Promise 的操作
 * 都将被忽略或抛出异常。
 *
 * @param <V> 异步操作返回的结果类型
 */
public interface Promise<V> extends Future<V> {

    /**
     * 将此 Promise 标记为成功完成并通知所有监听器。
     * <p>
     * 此方法设置操作结果并立即通知所有已注册的监听器。如果 Promise
     * 已经被标记为成功或失败，此方法将抛出 {@link IllegalStateException}。
     *
     * @param result 异步操作的结果，可以为 {@code null}
     * @return 此 Promise 实例，允许方法链式调用
     * @throws IllegalStateException 如果此 Promise 已被完成（成功、失败或取消）
     */
    Promise<V> setSuccess(V result);

    /**
     * 尝试将此 Promise 标记为成功完成并通知所有监听器。
     * <p>
     * 与 {@link #setSuccess(Object)} 不同，此方法不会在 Promise 已完成时抛出异常，
     * 而是返回一个布尔值表示操作是否成功。
     *
     * @param result 异步操作的结果，可以为 {@code null}
     * @return {@code true} 当且仅当成功地将此 Promise 标记为成功；
     *         {@code false} 表示此 Promise 已被标记为成功或失败
     */
    boolean trySuccess(V result);

    /**
     * 将此 Promise 标记为失败并通知所有监听器。
     * <p>
     * 此方法设置操作失败的原因并立即通知所有已注册的监听器。如果 Promise
     * 已经被标记为成功或失败，此方法将抛出 {@link IllegalStateException}。
     *
     * @param cause 异步操作失败的原因，不能为 {@code null}
     * @return 此 Promise 实例，允许方法链式调用
     * @throws IllegalStateException 如果此 Promise 已被完成（成功、失败或取消）
     * @throws NullPointerException 如果 cause 为 null
     */
    Promise<V> setFailure(Throwable cause);

    /**
     * 尝试将此 Promise 标记为失败并通知所有监听器。
     * <p>
     * 与 {@link #setFailure(Throwable)} 不同，此方法不会在 Promise 已完成时抛出异常，
     * 而是返回一个布尔值表示操作是否成功。
     *
     * @param cause 异步操作失败的原因，不能为 {@code null}
     * @return {@code true} 当且仅当成功地将此 Promise 标记为失败；
     *         {@code false} 表示此 Promise 已被标记为成功或失败
     * @throws NullPointerException 如果 cause 为 null
     */
    boolean tryFailure(Throwable cause);

    /**
     * 使此 Promise 不可取消。
     * <p>
     * 调用此方法后，{@link Future#cancel(boolean)} 将无法取消此 Promise。
     * 这对于需要保证异步操作不被外部干扰的场景非常有用。
     *
     * @return {@code true} 当且仅当成功地将此 Promise 标记为不可取消，或者它已经
     *         完成且没有被取消；{@code false} 如果此 Promise 已被取消
     */
    boolean setUncancellable();

    @Override
    Promise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener);

    @Override
    Promise<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

    @Override
    Promise<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener);

    @Override
    Promise<V> removeListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

    @Override
    Promise<V> await() throws InterruptedException;

    @Override
    Promise<V> awaitUninterruptibly();

    @Override
    Promise<V> sync() throws InterruptedException;

    @Override
    Promise<V> syncUninterruptibly();
}