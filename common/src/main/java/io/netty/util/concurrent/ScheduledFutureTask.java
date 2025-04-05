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

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.PriorityQueueNode;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Netty 调度系统的核心实现类，用于在指定时间点或按固定周期执行任务。
 * <p>
 * ScheduledFutureTask 实现了 {@link ScheduledFuture} 接口和 {@link PriorityQueueNode} 接口，
 * 使其既可以表示延迟或周期性任务的异步结果，又可以被存储在优先级队列中进行调度管理。
 * <p>
 * 此类继承自 {@link PromiseTask}，同时提供了定时调度功能，支持以下执行模式：
 * <ul>
 *   <li>一次性延迟执行 - 在指定的延迟后执行一次任务</li>
 *   <li>固定速率重复执行 - 以固定的时间间隔重复执行任务，不考虑任务的实际执行时间</li>
 *   <li>固定延迟重复执行 - 在前一次任务执行完成后等待固定时间再执行下一次任务</li>
 * </ul>
 * <p>
 * 该类不应由用户直接实例化，而应通过 {@link AbstractScheduledEventExecutor} 的调度方法创建。
 *
 * @param <V> 任务执行结果的类型
 *
 * @see ScheduledFuture
 * @see AbstractScheduledEventExecutor
 * @see PriorityQueueNode
 */
@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
final class ScheduledFutureTask<V> extends PromiseTask<V> implements ScheduledFuture<V>, PriorityQueueNode {
    /**
     * 任务的唯一标识符，在添加到优先级队列时设置。
     * 用于在截止时间相同的情况下确保任务的执行顺序。
     */
    private long id;

    /**
     * 任务的截止时间，以纳秒为单位。
     * 表示任务应该被执行的绝对时间点。
     */
    private long deadlineNanos;
    
    /**
     * 任务的执行周期，以纳秒为单位。
     * <ul>
     *   <li>0 - 非重复任务，仅执行一次</li>
     *   <li>&gt;0 - 固定速率重复，不考虑任务执行时间</li>
     *   <li>&lt;0 - 固定延迟重复，在前一次执行完成后等待固定时间</li>
     * </ul>
     */
    private final long periodNanos;

    /**
     * 任务在优先级队列中的索引位置。
     * 当任务不在队列中时，值为 {@link PriorityQueueNode#INDEX_NOT_IN_QUEUE}。
     */
    private int queueIndex = INDEX_NOT_IN_QUEUE;

    /**
     * 创建一个一次性延迟执行的任务。
     *
     * @param executor 负责执行此任务的调度执行器
     * @param runnable 要执行的任务
     * @param nanoTime 任务的预期执行时间点（纳秒）
     */
    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Runnable runnable, long nanoTime) {
        super(executor, runnable);
        deadlineNanos = nanoTime;
        periodNanos = 0;
    }

    /**
     * 创建一个周期性执行的任务。
     *
     * @param executor 负责执行此任务的调度执行器
     * @param runnable 要执行的任务
     * @param nanoTime 任务首次执行的时间点（纳秒）
     * @param period 任务的执行周期（纳秒）；正值表示固定速率，负值表示固定延迟
     * @throws IllegalArgumentException 如果 period 为 0
     */
    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Runnable runnable, long nanoTime, long period) {
        super(executor, runnable);
        deadlineNanos = nanoTime;
        periodNanos = validatePeriod(period);
    }

    /**
     * 创建一个周期性执行且返回结果的任务。
     *
     * @param executor 负责执行此任务的调度执行器
     * @param callable 要执行的可返回结果的任务
     * @param nanoTime 任务首次执行的时间点（纳秒）
     * @param period 任务的执行周期（纳秒）；正值表示固定速率，负值表示固定延迟
     * @throws IllegalArgumentException 如果 period 为 0
     */
    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Callable<V> callable, long nanoTime, long period) {
        super(executor, callable);
        deadlineNanos = nanoTime;
        periodNanos = validatePeriod(period);
    }

    /**
     * 创建一个一次性延迟执行且返回结果的任务。
     *
     * @param executor 负责执行此任务的调度执行器
     * @param callable 要执行的可返回结果的任务
     * @param nanoTime 任务的预期执行时间点（纳秒）
     */
    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Callable<V> callable, long nanoTime) {
        super(executor, callable);
        deadlineNanos = nanoTime;
        periodNanos = 0;
    }

    /**
     * 验证周期值的有效性。
     *
     * @param period 要验证的周期值
     * @return 验证通过的周期值
     * @throws IllegalArgumentException 如果周期值为 0
     */
    private static long validatePeriod(long period) {
        if (period == 0) {
            throw new IllegalArgumentException("period: 0 (expected: != 0)");
        }
        return period;
    }

    /**
     * 设置任务的唯一标识符。
     * <p>
     * 任务的标识符在首次设置后不会改变，确保了在截止时间相同的情况下任务的稳定排序。
     *
     * @param id 要设置的唯一标识符
     * @return 此任务实例
     */
    ScheduledFutureTask<V> setId(long id) {
        if (this.id == 0L) {
            this.id = id;
        }
        return this;
    }

    /**
     * 获取此任务关联的事件执行器。
     *
     * @return 关联的事件执行器
     */
    @Override
    protected EventExecutor executor() {
        return super.executor();
    }

    /**
     * 获取任务的截止时间（纳秒）。
     *
     * @return 截止时间，表示任务应该执行的时间点
     */
    public long deadlineNanos() {
        return deadlineNanos;
    }

    /**
     * 标记任务已被消费（执行）。
     * <p>
     * 对于非重复任务，此方法将截止时间设置为 0，作为一种优化，
     * 避免在任务已执行并从队列中移除后再次检查系统时钟。
     */
    void setConsumed() {
        // Optimization to avoid checking system clock again
        // after deadline has passed and task has been dequeued
        if (periodNanos == 0) {
            assert scheduledExecutor().getCurrentTimeNanos() >= deadlineNanos;
            deadlineNanos = 0L;
        }
    }

    /**
     * 计算距离任务执行还需等待的时间（纳秒）。
     * <p>
     * 此方法基于当前系统时间计算剩余延迟。
     *
     * @return 剩余延迟时间（纳秒），如果任务已经到期或已被消费，则返回 0
     */
    public long delayNanos() {
        if (deadlineNanos == 0L) {
            return 0L;
        }
        return delayNanos(scheduledExecutor().getCurrentTimeNanos());
    }

    /**
     * 根据指定的当前时间计算截止时间对应的延迟。
     *
     * @param currentTimeNanos 当前时间（纳秒）
     * @param deadlineNanos 截止时间（纳秒）
     * @return 剩余延迟时间（纳秒），如果截止时间已过，则返回 0
     */
    static long deadlineToDelayNanos(long currentTimeNanos, long deadlineNanos) {
        return deadlineNanos == 0L ? 0L : Math.max(0L, deadlineNanos - currentTimeNanos);
    }

    /**
     * 基于指定的当前时间计算距离任务执行的延迟。
     *
     * @param currentTimeNanos 当前时间（纳秒）
     * @return 剩余延迟时间（纳秒）
     */
    public long delayNanos(long currentTimeNanos) {
        return deadlineToDelayNanos(currentTimeNanos, deadlineNanos);
    }

    /**
     * 获取以指定时间单位表示的任务延迟。
     * <p>
     * 此方法由 {@link ScheduledFuture} 接口定义，用于与 JDK 的调度框架兼容。
     *
     * @param unit 要转换成的时间单位
     * @return 以指定单位表示的剩余延迟
     */
    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(delayNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * 比较此任务与另一个延迟任务的执行顺序。
     * <p>
     * 比较基于以下优先级：
     * <ol>
     *   <li>首先比较截止时间，较早的截止时间优先</li>
     *   <li>如果截止时间相同，则比较任务 ID，较小的 ID 优先</li>
     * </ol>
     * 这确保了具有相同截止时间的任务按添加顺序执行。
     *
     * @param o 要比较的另一个延迟任务
     * @return 负值表示此任务应先执行，正值表示另一个任务应先执行，0 表示相等
     */
    @Override
    public int compareTo(Delayed o) {
        if (this == o) {
            return 0;
        }

        ScheduledFutureTask<?> that = (ScheduledFutureTask<?>) o;
        long d = deadlineNanos() - that.deadlineNanos();
        if (d < 0) {
            return -1;
        } else if (d > 0) {
            return 1;
        } else if (id < that.id) {
            return -1;
        } else {
            assert id != that.id;
            return 1;
        }
    }

    /**
     * 执行任务的核心方法。
     * <p>
     * 此方法在事件循环线程中被调用，负责：
     * <ul>
     *   <li>检查任务是否已到期，如未到期则重新调度</li>
     *   <li>执行一次性任务或周期性任务</li>
     *   <li>对于周期性任务，计算下次执行时间并重新入队</li>
     *   <li>处理任务执行过程中的异常</li>
     * </ul>
     */
    @Override
    public void run() {
        assert executor().inEventLoop();
        try {
            if (delayNanos() > 0L) {
                // Not yet expired, need to add or remove from queue
                if (isCancelled()) {
                    scheduledExecutor().scheduledTaskQueue().removeTyped(this);
                } else {
                    scheduledExecutor().scheduleFromEventLoop(this);
                }
                return;
            }
            if (periodNanos == 0) {
                if (setUncancellableInternal()) {
                    V result = runTask();
                    setSuccessInternal(result);
                }
            } else {
                // check if is done as it may was cancelled
                if (!isCancelled()) {
                    runTask();
                    if (!executor().isShutdown()) {
                        if (periodNanos > 0) {
                            deadlineNanos += periodNanos;
                        } else {
                            deadlineNanos = scheduledExecutor().getCurrentTimeNanos() - periodNanos;
                        }
                        if (!isCancelled()) {
                            scheduledExecutor().scheduledTaskQueue().add(this);
                        }
                    }
                }
            }
        } catch (Throwable cause) {
            setFailureInternal(cause);
        }
    }

    /**
     * 获取关联的调度执行器。
     *
     * @return 负责调度此任务的执行器
     */
    private AbstractScheduledEventExecutor scheduledExecutor() {
        return (AbstractScheduledEventExecutor) executor();
    }

    /**
     * 取消此任务的执行。
     * <p>
     * 如果成功取消，任务将从调度队列中移除。
     *
     * @param mayInterruptIfRunning 此参数在本实现中无效
     * @return 如果任务成功取消，则返回 true；如果任务已完成、已取消或无法取消，则返回 false
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean canceled = super.cancel(mayInterruptIfRunning);
        if (canceled) {
            scheduledExecutor().removeScheduled(this);
        }
        return canceled;
    }

    /**
     * 仅取消任务而不从调度队列中移除。
     * <p>
     * 此方法用于调度器内部调用，当调度器需要取消任务但由调度器本身负责队列管理时使用。
     *
     * @param mayInterruptIfRunning 此参数在本实现中无效
     * @return 如果任务成功取消，则返回 true
     */
    boolean cancelWithoutRemove(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning);
    }

    /**
     * 构建包含任务详细信息的字符串表示。
     * <p>
     * 扩展基类的 toString 实现，添加截止时间和周期信息。
     *
     * @return 包含任务信息的 StringBuilder
     */
    @Override
    protected StringBuilder toStringBuilder() {
        StringBuilder buf = super.toStringBuilder();
        buf.setCharAt(buf.length() - 1, ',');

        return buf.append(" deadline: ")
                  .append(deadlineNanos)
                  .append(", period: ")
                  .append(periodNanos)
                  .append(')');
    }

    // PriorityQueueNode 接口的实现方法，用于在优先级队列中存储和检索

    /**
     * 获取此任务在指定优先级队列中的索引。
     *
     * @param queue 包含此节点的优先级队列
     * @return 在队列中的索引位置
     */
    @Override
    public int priorityQueueIndex(DefaultPriorityQueue<?> queue) {
        return queueIndex;
    }

    /**
     * 设置此任务在指定优先级队列中的索引。
     *
     * @param queue 包含此节点的优先级队列
     * @param i 在队列中的新索引位置
     */
    @Override
    public void priorityQueueIndex(DefaultPriorityQueue<?> queue, int i) {
        queueIndex = i;
    }
}
