/*
 * Copyright 2016 The Netty Project
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

import io.netty.util.internal.ObjectUtil;

/**
 * <p>
 * Promise组合器，用于监控多个独立Future的完成状态，并在所有被组合的Future都完成时通知一个最终的聚合Promise。
 * 当且仅当所有被组合的Future都成功完成时，聚合Promise才会成功。如果任何一个被组合的Future失败，
 * 聚合Promise也会失败。如果有多个被组合的Future失败，聚合Promise的失败原因将是其中一个失败Future的原因，
 * 但具体是哪一个失败Future的原因则是不确定的。
 * </p>
 *
 * <p>
 * 调用者可以通过{@link PromiseCombiner#add(Future)}和{@link PromiseCombiner#addAll(Future[])}方法
 * 向Promise组合器中添加任意数量的Future。当所有需要组合的Future都已添加后，调用者必须通过
 * {@link PromiseCombiner#finish(Promise)}方法提供一个聚合Promise，该Promise将在所有被组合的Promise
 * 完成时收到通知。
 * </p>
 *
 * <p>
 * 此实现<strong>不是</strong>线程安全的，所有方法调用必须在{@link EventExecutor}线程中进行。
 * </p>
 */
public final class PromiseCombiner {
    /**
     * 期望完成的Future总数。
     * 每添加一个Future到组合器中，此计数器就会增加。
     */
    private int expectedCount;

    /**
     * 已完成的Future数量。
     * 当一个被组合的Future完成时（成功或失败），此计数器会增加。
     */
    private int doneCount;

    /**
     * 聚合Promise，将在所有被组合的Future完成时被通知。
     * 此字段在调用{@link #finish(Promise)}方法时设置。
     */
    private Promise<Void> aggregatePromise;

    /**
     * 记录第一个失败的Future的失败原因。
     * 如果所有Future都成功，此字段保持为null。
     */
    private Throwable cause;

    /**
     * 监听器，用于监控每个被添加的Future的完成情况。
     * 当一个Future完成时，此监听器会更新内部状态并在所有Future都完成时通知聚合Promise。
     */
    private final GenericFutureListener<Future<?>> listener = new GenericFutureListener<Future<?>>() {
        // 当被监听的Future完成时（无论成功还是失败）会调用此方法
        @Override
        public void operationComplete(final Future<?> future) {
            // 检查当前执行线程是否是事件循环线程
            if (executor.inEventLoop()) {
                // 如果是事件循环线程，直接处理
                operationComplete0(future);
            } else {
                // 如果不是事件循环线程，提交任务到事件循环线程中执行
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        operationComplete0(future);
                    }
                });
            }
        }
    
        /**
         * 实际处理Future完成事件的内部方法
         */
        private void operationComplete0(Future<?> future) {
            // 断言确保此方法在事件循环线程中执行
            assert executor.inEventLoop();
            
            // 增加已完成Future的计数
            ++doneCount;
            
            // 如果当前Future失败且尚未记录失败原因，则记录此失败原因
            if (!future.isSuccess() && cause == null) {
                cause = future.cause();
            }
            
            // 检查是否所有Future都已完成且聚合Promise已设置
            if (doneCount == expectedCount && aggregatePromise != null) {
                // 尝试完成聚合Promise
                tryPromise();
            }
        }
    };

    /**
     * 用于执行操作的事件执行器。
     * 确保所有操作都在同一个事件循环线程中执行，保证线程安全。
     */
    private final EventExecutor executor;

    /**
     * 已废弃的构造函数，使用{@link PromiseCombiner#PromiseCombiner(EventExecutor)}代替。
     * 
     * @deprecated 使用{@link PromiseCombiner#PromiseCombiner(EventExecutor)}
     */
    @Deprecated
    public PromiseCombiner() {
        this(ImmediateEventExecutor.INSTANCE);
    }

    /**
     * 创建一个新的Promise组合器，使用指定的事件执行器进行通知。
     * 必须在该事件执行器的线程中调用{@link #add(Future)}、{@link #addAll(Future[])}
     * 和{@link #finish(Promise)}方法。
     *
     * @param executor 用于通知的事件执行器
     * @throws NullPointerException 如果executor为null
     */
    public PromiseCombiner(EventExecutor executor) {
        this.executor = ObjectUtil.checkNotNull(executor, "executor");
    }

    /**
     * 添加一个新的promise到组合器中。
     * 在通过{@link PromiseCombiner#finish(Promise)}方法添加聚合promise之前，
     * 可以继续添加新的promise。
     *
     * @param promise 要添加到此promise组合器的promise
     * @deprecated 已被{@link PromiseCombiner#add(Future)}替代
     */
    @Deprecated
    public void add(Promise promise) {
        add((Future) promise);
    }

    /**
     * 添加一个新的future到组合器中。
     * 在通过{@link PromiseCombiner#finish(Promise)}方法添加聚合promise之前，
     * 可以继续添加新的future。
     *
     * @param future 要添加到此promise组合器的future
     * @throws IllegalStateException 如果已经调用了finish方法
     * @throws IllegalStateException 如果不是在EventExecutor线程中调用
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void add(Future future) {
        checkAddAllowed();
        checkInEventLoop();
        ++expectedCount;
        future.addListener(listener);
    }

    /**
     * 添加多个promise到组合器中。
     * 在通过{@link PromiseCombiner#finish(Promise)}方法添加聚合promise之前，
     * 可以继续添加新的promise。
     *
     * @param promises 要添加到此promise组合器的promise数组
     * @deprecated 已被{@link PromiseCombiner#addAll(Future[])}替代
     */
    @Deprecated
    public void addAll(Promise... promises) {
        addAll((Future[]) promises);
    }

    /**
     * 添加多个future到组合器中。
     * 在通过{@link PromiseCombiner#finish(Promise)}方法添加聚合promise之前，
     * 可以继续添加新的future。
     *
     * @param futures 要添加到此promise组合器的future数组
     * @throws IllegalStateException 如果已经调用了finish方法
     * @throws IllegalStateException 如果不是在EventExecutor线程中调用
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void addAll(Future... futures) {
        for (Future future : futures) {
            this.add(future);
        }
    }

    /**
     * <p>
     * 设置当所有被组合的future完成时要通知的promise。
     * 如果所有被组合的future都成功完成，则聚合promise也会成功。
     * 如果一个或多个被组合的future失败，则聚合promise将以其中一个失败future的原因而失败。
     * 如果多个被组合的future失败，则聚合promise将会失败，但具体使用哪个失败的原因是不确定的。
     * </p>
     *
     * <p>
     * 调用此方法后，不能再通过{@link PromiseCombiner#add(Future)}或
     * {@link PromiseCombiner#addAll(Future[])}方法添加更多的future。
     * </p>
     *
     * @param aggregatePromise 当所有被组合的future完成时要通知的promise
     * @throws NullPointerException  如果aggregatePromise为null
     * @throws IllegalStateException 如果不是在EventExecutor线程中调用
     * @throws IllegalStateException 如果已经调用过此方法
     */
    public void finish(Promise<Void> aggregatePromise) {
        ObjectUtil.checkNotNull(aggregatePromise, "aggregatePromise");
        checkInEventLoop();
        if (this.aggregatePromise != null) {
            throw new IllegalStateException("Already finished");
        }
        this.aggregatePromise = aggregatePromise;
        if (doneCount == expectedCount) {
            tryPromise();
        }
    }

    /**
     * 检查当前调用是否在事件执行器的线程中。
     * 如果不是，则抛出异常。
     *
     * @throws IllegalStateException 如果不是在EventExecutor线程中调用
     */
    private void checkInEventLoop() {
        if (!executor.inEventLoop()) {
            throw new IllegalStateException("Must be called from EventExecutor thread");
        }
    }

    /**
     * 尝试设置聚合promise的结果。
     * 如果没有失败（cause为null），则尝试将聚合promise设置为成功；
     * 否则，尝试将聚合promise设置为失败，失败原因为记录的第一个失败原因。
     *
     * @return 如果设置成功返回true，否则返回false
     */
    private boolean tryPromise() {
        return (cause == null) ? aggregatePromise.trySuccess(null) : aggregatePromise.tryFailure(cause);
    }

    /**
     * 检查是否还允许添加更多的future。
     * 如果已经调用了finish方法，则不允许添加更多future。
     *
     * @throws IllegalStateException 如果已经调用了finish方法
     */
    private void checkAddAllowed() {
        if (aggregatePromise != null) {
            throw new IllegalStateException("Adding promises is not allowed after finished adding");
        }
    }
}
