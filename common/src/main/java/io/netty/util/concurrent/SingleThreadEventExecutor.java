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
package io.netty.util.concurrent;

import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThreadExecutorMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jetbrains.annotations.Async.Schedule;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Abstract base class for {@link OrderedEventExecutor}'s that execute all its
 * submitted tasks in a single thread.
 *
 */
public abstract class SingleThreadEventExecutor extends AbstractScheduledEventExecutor implements OrderedEventExecutor {

    /**
     * 单线程事件执行器的默认最大挂起任务数量。
     * 该值通过系统属性"io.netty.eventexecutor.maxPendingTasks"配置，
     * 默认为Integer.MAX_VALUE，但至少为16。
     */
    static final int DEFAULT_MAX_PENDING_EXECUTOR_TASKS = Math.max(16,
            SystemPropertyUtil.getInt("io.netty.eventexecutor.maxPendingTasks", Integer.MAX_VALUE));

    /**
     * 类日志记录器实例，用于记录SingleThreadEventExecutor的内部状态和错误信息。
     */
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SingleThreadEventExecutor.class);

    /**
     * 执行器尚未启动的状态常量。
     */
    private static final int ST_NOT_STARTED = 1;

    /**
     * 执行器已启动且正在运行的状态常量。
     */
    private static final int ST_STARTED = 2;

    /**
     * 执行器正在关闭过程中的状态常量，此时不再接受新任务但会处理完队列中现有任务。
     */
    private static final int ST_SHUTTING_DOWN = 3;

    /**
     * 执行器已关闭的状态常量，不再接受新任务，队列中任务也已处理完毕。
     */
    private static final int ST_SHUTDOWN = 4;

    /**
     * 执行器已终止的状态常量，所有资源已释放，完全停止运行。
     */
    private static final int ST_TERMINATED = 5;

    /**
     * 空操作任务，不执行任何操作。
     * 通常用于唤醒执行线程或作为占位符使用。
     */
    private static final Runnable NOOP_TASK = new Runnable() {
        @Override
        public void run() {
            // Do nothing.
        }
    };

    /**
     * 用于原子更新执行器状态的更新器。
     * 使用AtomicIntegerFieldUpdater避免创建额外的AtomicInteger对象，提高性能。
     */
    private static final AtomicIntegerFieldUpdater<SingleThreadEventExecutor> STATE_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(SingleThreadEventExecutor.class, "state");

    /**
     * 用于原子更新线程属性的更新器。
     * 使用AtomicReferenceFieldUpdater避免创建额外的AtomicReference对象，提高性能。
     */
    private static final AtomicReferenceFieldUpdater<SingleThreadEventExecutor, ThreadProperties> PROPERTIES_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(
                    SingleThreadEventExecutor.class, ThreadProperties.class, "threadProperties");

    /**
     * 任务队列，存储待执行的任务。
     * 由于执行器是单线程的，所有提交的任务都会放入此队列等待执行。
     */
    private final Queue<Runnable> taskQueue;

    /**
     * 执行任务的线程。
     * volatile修饰确保多线程环境下的可见性。
     */
    private volatile Thread thread;

    /**
     * 线程属性对象，包含了执行线程的各种属性信息。
     * 使用@SuppressWarnings("unused")是因为该字段通过PROPERTIES_UPDATER进行更新，
     * 而不是直接访问。
     */
    @SuppressWarnings("unused")
    private volatile ThreadProperties threadProperties;

    /**
     * 用于执行任务的执行器。
     * 此执行器负责创建和启动实际执行任务的线程。
     */
    private final Executor executor;

    /**
     * 标识线程是否被中断的标志。
     */
    private volatile boolean interrupted;

    /**
     * 用于等待线程启动完成的倒计时锁。
     * 初始计数为1，当线程启动完成后会调用countDown()方法。
     */
    private final CountDownLatch threadLock = new CountDownLatch(1);

    /**
     * 关闭钩子集合，当执行器关闭时会执行这些钩子任务。
     * 使用LinkedHashSet保证钩子按添加顺序执行。
     */
    private final Set<Runnable> shutdownHooks = new LinkedHashSet<Runnable>();

    /**
     * 标识添加任务是否会唤醒执行线程的标志。
     * 如果为true，当有新任务添加到队列时会唤醒执行线程。
     */
    private final boolean addTaskWakesUp;

    /**
     * 任务队列允许的最大待处理任务数量。
     * 当队列中任务数超过此值时，新提交的任务将被拒绝。
     */
    private final int maxPendingTasks;

    /**
     * 任务被拒绝时的处理策略。
     * 当任务队列已满或执行器已关闭时，会使用此处理器处理被拒绝的任务。
     */
    private final RejectedExecutionHandler rejectedExecutionHandler;

    /**
     * 记录最后一次执行任务的时间戳。
     * 用于监控执行器的活动状态和性能分析。
     */
    private long lastExecutionTime;

    /**
     * 执行器当前状态。
     * 使用volatile确保多线程环境下的可见性，通过STATE_UPDATER进行原子更新。
     * 初始状态为ST_NOT_STARTED。
     */
    @SuppressWarnings({ "FieldMayBeFinal", "unused" })
    private volatile int state = ST_NOT_STARTED;

    /**
     * 优雅关闭的静默期，单位为纳秒。
     * 在此期间如果没有新任务，则认为可以安全关闭。
     */
    private volatile long gracefulShutdownQuietPeriod;

    /**
     * 优雅关闭的超时时间，单位为纳秒。
     * 如果超过此时间仍未完成关闭，则强制关闭。
     */
    private volatile long gracefulShutdownTimeout;

    /**
     * 优雅关闭开始的时间戳，单位为纳秒。
     */
    private long gracefulShutdownStartTime;

    /**
     * 终止完成的Promise对象。
     * 当执行器完全终止时，此Promise会被标记为成功。
     * 使用GlobalEventExecutor.INSTANCE作为执行器确保回调在合适的线程上执行。
     */
    private final Promise<?> terminationFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE);

    /**
     * Create a new instance
     *
     * @param parent         the {@link EventExecutorGroup} which is the parent of
     *                       this instance and belongs to it
     * @param threadFactory  the {@link ThreadFactory} which will be used for the
     *                       used {@link Thread}
     * @param addTaskWakesUp {@code true} if and only if invocation of
     *                       {@link #addTask(Runnable)} will wake up the
     *                       executor thread
     */
    protected SingleThreadEventExecutor(
            EventExecutorGroup parent, ThreadFactory threadFactory, boolean addTaskWakesUp) {
        this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp);
    }

    /**
     * Create a new instance
     *
     * @param parent          the {@link EventExecutorGroup} which is the parent of
     *                        this instance and belongs to it
     * @param threadFactory   the {@link ThreadFactory} which will be used for the
     *                        used {@link Thread}
     * @param addTaskWakesUp  {@code true} if and only if invocation of
     *                        {@link #addTask(Runnable)} will wake up the
     *                        executor thread
     * @param maxPendingTasks the maximum number of pending tasks before new tasks
     *                        will be rejected.
     * @param rejectedHandler the {@link RejectedExecutionHandler} to use.
     */
    protected SingleThreadEventExecutor(
            EventExecutorGroup parent, ThreadFactory threadFactory,
            boolean addTaskWakesUp, int maxPendingTasks, RejectedExecutionHandler rejectedHandler) {
        this(parent, new ThreadPerTaskExecutor(threadFactory), addTaskWakesUp, maxPendingTasks, rejectedHandler);
    }

    /**
     * Create a new instance
     *
     * @param parent         the {@link EventExecutorGroup} which is the parent of
     *                       this instance and belongs to it
     * @param executor       the {@link Executor} which will be used for executing
     * @param addTaskWakesUp {@code true} if and only if invocation of
     *                       {@link #addTask(Runnable)} will wake up the
     *                       executor thread
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakesUp) {
        this(parent, executor, addTaskWakesUp, DEFAULT_MAX_PENDING_EXECUTOR_TASKS, RejectedExecutionHandlers.reject());
    }

    /**
     * Create a new instance
     *
     * @param parent          the {@link EventExecutorGroup} which is the parent of
     *                        this instance and belongs to it
     * @param executor        the {@link Executor} which will be used for executing
     * @param addTaskWakesUp  {@code true} if and only if invocation of
     *                        {@link #addTask(Runnable)} will wake up the
     *                        executor thread
     * @param maxPendingTasks the maximum number of pending tasks before new tasks
     *                        will be rejected.
     * @param rejectedHandler the {@link RejectedExecutionHandler} to use.
     */
    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor,
            boolean addTaskWakesUp, int maxPendingTasks,
            RejectedExecutionHandler rejectedHandler) {
        super(parent);
        this.addTaskWakesUp = addTaskWakesUp;
        this.maxPendingTasks = Math.max(16, maxPendingTasks);
        this.executor = ThreadExecutorMap.apply(executor, this);
        taskQueue = newTaskQueue(this.maxPendingTasks);
        rejectedExecutionHandler = ObjectUtil.checkNotNull(rejectedHandler, "rejectedHandler");
    }

    protected SingleThreadEventExecutor(EventExecutorGroup parent, Executor executor,
            boolean addTaskWakesUp, Queue<Runnable> taskQueue,
            RejectedExecutionHandler rejectedHandler) {
        super(parent);
        this.addTaskWakesUp = addTaskWakesUp;
        this.maxPendingTasks = DEFAULT_MAX_PENDING_EXECUTOR_TASKS;
        this.executor = ThreadExecutorMap.apply(executor, this);
        this.taskQueue = ObjectUtil.checkNotNull(taskQueue, "taskQueue");
        this.rejectedExecutionHandler = ObjectUtil.checkNotNull(rejectedHandler, "rejectedHandler");
    }

    /**
     * @deprecated Please use and override {@link #newTaskQueue(int)}.
     */
    @Deprecated
    protected Queue<Runnable> newTaskQueue() {
        return newTaskQueue(maxPendingTasks);
    }

    /**
     * Create a new {@link Queue} which will holds the tasks to execute. This
     * default implementation will return a
     * {@link LinkedBlockingQueue} but if your sub-class of
     * {@link SingleThreadEventExecutor} will not do any blocking
     * calls on the this {@link Queue} it may make sense to {@code @Override} this
     * and return some more performant
     * implementation that does not support blocking operations at all.
     */
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return new LinkedBlockingQueue<Runnable>(maxPendingTasks);
    }

    /**
     * Interrupt the current running {@link Thread}.
     */
    protected void interruptThread() {
        Thread currentThread = thread;
        if (currentThread == null) {
            interrupted = true;
        } else {
            currentThread.interrupt();
        }
    }

    /**
     * @see Queue#poll()
     */
    protected Runnable pollTask() {
        assert inEventLoop();
        return pollTaskFrom(taskQueue);
    }

    protected static Runnable pollTaskFrom(Queue<Runnable> taskQueue) {
        for (;;) {
            Runnable task = taskQueue.poll();
            if (task != WAKEUP_TASK) {
                return task;
            }
        }
    }

    /**
     * 从任务队列中获取下一个{@link Runnable}任务，如果当前没有任务，将阻塞等待。
     * <p>
     * 该方法实现了任务获取的核心逻辑，包括以下特性：
     * <ul>
     * <li>在无任务可用时会阻塞线程，直到有新任务加入队列</li>
     * <li>支持调度任务的优先级处理，确保定时任务能够按时执行</li>
     * <li>处理特殊的唤醒任务（WAKEUP_TASK），用于唤醒阻塞的事件循环线程</li>
     * <li>在有定时任务需要执行时，最多等待到定时任务的执行时间</li>
     * </ul>
     * </p>
     * <p>
     * 请注意，如果任务队列不是{@link BlockingQueue}类型（通过{@link #newTaskQueue()}创建），
     * 此方法将抛出{@link UnsupportedOperationException}异常。这是因为阻塞操作需要BlockingQueue
     * 提供的特性支持。
     * </p>
     * <p>
     * 此方法内部实现了一个优先级逻辑：
     * <ol>
     * <li>当没有定时任务时，直接阻塞等待普通任务</li>
     * <li>当有定时任务但尚未到执行时间时，等待直到定时任务到期或有普通任务到达</li>
     * <li>当定时任务到期时，将其加入普通任务队列执行</li>
     * </ol>
     * 这种设计确保了定时任务不会因为普通任务过多而被"饿死"。
     * </p>
     * <p>
     * 此方法只能在EventLoop线程中调用，由assert语句强制执行此约束。
     * </p>
     *
     * @return 下一个需要执行的{@link Runnable}任务；如果执行器线程被中断或被唤醒（收到WAKEUP_TASK），
     *         则返回{@code null}
     * @throws UnsupportedOperationException 如果任务队列不是{@link BlockingQueue}类型
     * @see BlockingQueue#take()
     * @see BlockingQueue#poll(long, TimeUnit)
     */
    protected Runnable takeTask() {
        // 确保当前方法在EventLoop线程中调用
        assert inEventLoop();

        // 检查任务队列是否实现了BlockingQueue接口
        if (!(taskQueue instanceof BlockingQueue)) {
            throw new UnsupportedOperationException();
        }

        // 将taskQueue转换为BlockingQueue类型
        BlockingQueue<Runnable> taskQueue = (BlockingQueue<Runnable>) this.taskQueue;

        // 无限循环，直到找到有效任务或被中断
        for (;;) {
            // 查看（不移除）定时任务队列中最近要执行的任务
            ScheduledFutureTask<?> scheduledTask = peekScheduledTask();

            if (scheduledTask == null) {
                // 没有定时任务需要执行
                Runnable task = null;
                try {
                    // 阻塞等待直到任务队列中有任务可用
                    task = taskQueue.take();
                    if (task == WAKEUP_TASK) {
                        // 如果是唤醒任务，返回null
                        task = null;
                    }
                } catch (InterruptedException e) {
                    // 忽略中断异常
                }
                return task;
            } else {
                // 有定时任务需要执行
                long delayNanos = scheduledTask.delayNanos();
                Runnable task = null;

                if (delayNanos > 0) {
                    // 定时任务还没到执行时间，等待直到最近的定时任务到期或有新任务
                    try {
                        task = taskQueue.poll(delayNanos, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        // 被唤醒，返回null
                        return null;
                    }
                }

                if (task == null) {
                    // 没有普通任务，需要检查和执行定时任务
                    // 将到期的定时任务移到普通任务队列
                    fetchFromScheduledTaskQueue();
                    task = taskQueue.poll();
                }

                if (task != null) {
                    if (task == WAKEUP_TASK) {
                        // 如果是唤醒任务，返回null
                        return null;
                    }
                    return task;
                }
                // 如果没找到任务，继续循环
            }
        }
    }

    /**
     * 尝试将已到期的调度任务从调度任务队列转移到主任务队列中。
     * <p>
     * 该方法负责将已经到期的调度任务（{@code scheduledTaskQueue}）转移到常规任务队列（{@code taskQueue}）中以便执行。
     * 方法执行流程如下：
     * <ol>
     *   <li>检查调度任务队列是否为空，如果为空则直接返回{@code true}</li>
     *   <li>获取当前时间以确定哪些调度任务已到期</li>
     *   <li>循环从调度任务队列中取出已到期的任务：
     *     <ul>
     *       <li>如果没有更多已到期的任务，则返回{@code true}</li>
     *       <li>尝试将任务添加到主任务队列中</li>
     *       <li>如果主任务队列已满，将任务放回调度队列并返回{@code false}</li>
     *     </ul>
     *   </li>
     * </ol>
     * </p>
     * <p>
     * 此方法与{@link #executeExpiredScheduledTasks()}的区别在于：本方法只是将任务从调度队列移到主队列，
     * 而不直接执行任务。这样设计使得调度任务最终通过同一个任务执行路径处理，简化了任务执行逻辑。
     * </p>
     * 
     * @return {@code true} 如果所有到期的调度任务都成功转移到主任务队列，或者没有到期的调度任务；
     *         {@code false} 如果主任务队列已满，无法接收更多任务
     */
    private boolean fetchFromScheduledTaskQueue() {
        if (scheduledTaskQueue == null || scheduledTaskQueue.isEmpty()) {
            return true;
        }
        long nanoTime = getCurrentTimeNanos();
        for (;;) {
            Runnable scheduledTask = pollScheduledTask(nanoTime);
            if (scheduledTask == null) {
                return true;
            }
            if (!taskQueue.offer(scheduledTask)) {
                // No space left in the task queue add it back to the scheduledTaskQueue so we
                // pick it up again.
                scheduledTaskQueue.add((ScheduledFutureTask<?>) scheduledTask);
                return false;
            }
        }
    }

    /**
     * @return {@code true} if at least one scheduled task was executed.
     */
    private boolean executeExpiredScheduledTasks() {
        if (scheduledTaskQueue == null || scheduledTaskQueue.isEmpty()) {
            return false;
        }
        long nanoTime = getCurrentTimeNanos();
        Runnable scheduledTask = pollScheduledTask(nanoTime);
        if (scheduledTask == null) {
            return false;
        }
        do {
            safeExecute(scheduledTask);
        } while ((scheduledTask = pollScheduledTask(nanoTime)) != null);
        return true;
    }

    /**
     * @see Queue#peek()
     */
    protected Runnable peekTask() {
        assert inEventLoop();
        return taskQueue.peek();
    }

    /**
     * @see Queue#isEmpty()
     */
    protected boolean hasTasks() {
        assert inEventLoop();
        return !taskQueue.isEmpty();
    }

    /**
     * Return the number of tasks that are pending for processing.
     */
    public int pendingTasks() {
        return taskQueue.size();
    }

    /**
     * Add a task to the task queue, or throws a {@link RejectedExecutionException}
     * if this instance was shutdown
     * before.
     */
    protected void addTask(Runnable task) {
        ObjectUtil.checkNotNull(task, "task");
        if (!offerTask(task)) {
            reject(task);
        }
    }

    final boolean offerTask(Runnable task) {
        if (isShutdown()) {
            reject();
        }
        return taskQueue.offer(task);
    }

    /**
     * @see Queue#remove(Object)
     */
    protected boolean removeTask(Runnable task) {
        return taskQueue.remove(ObjectUtil.checkNotNull(task, "task"));
    }

    /**
     * Poll all tasks from the task queue and run them via {@link Runnable#run()}
     * method.
     *
     * @return {@code true} if and only if at least one task was run
     */
    protected boolean runAllTasks() {
        assert inEventLoop();
        boolean fetchedAll;
        boolean ranAtLeastOne = false;

        do {
            fetchedAll = fetchFromScheduledTaskQueue();
            if (runAllTasksFrom(taskQueue)) {
                ranAtLeastOne = true;
            }
        } while (!fetchedAll); // keep on processing until we fetched all scheduled tasks.

        if (ranAtLeastOne) {
            lastExecutionTime = getCurrentTimeNanos();
        }
        afterRunningAllTasks();
        return ranAtLeastOne;
    }

    /**
     * Execute all expired scheduled tasks and all current tasks in the executor
     * queue until both queues are empty,
     * or {@code maxDrainAttempts} has been exceeded.
     * 
     * @param maxDrainAttempts The maximum amount of times this method attempts to
     *                         drain from queues. This is to prevent
     *                         continuous task execution and scheduling from
     *                         preventing the EventExecutor thread to
     *                         make progress and return to the selector mechanism to
     *                         process inbound I/O events.
     * @return {@code true} if at least one task was run.
     */
    protected final boolean runScheduledAndExecutorTasks(final int maxDrainAttempts) {
        assert inEventLoop();
        boolean ranAtLeastOneTask;
        int drainAttempt = 0;
        do {
            // We must run the taskQueue tasks first, because the scheduled tasks from
            // outside the EventLoop are queued
            // 因为taskQueue是线程安全的，而scheduledTaskQueue不是线程安全的，
            // 所以我们必须先运行taskQueue中的任务
            ranAtLeastOneTask = runAllTasksFrom(taskQueue) | executeExpiredScheduledTasks();
        } while (ranAtLeastOneTask && ++drainAttempt < maxDrainAttempts);

        if (drainAttempt > 0) {
            lastExecutionTime = getCurrentTimeNanos();
        }
        afterRunningAllTasks();

        return drainAttempt > 0;
    }

    /**
     * 执行指定任务队列中的所有任务。
     * <p>
     * 该方法会不断从提供的任务队列中取出任务并执行，直到队列中没有更多任务为止。
     * 方法的执行流程如下：
     * <ol>
     *   <li>从队列中取出第一个任务</li>
     *   <li>如果队列为空（没有任务），立即返回{@code false}</li>
     *   <li>通过{@link #safeExecute(Runnable)}安全地执行取出的任务</li>
     *   <li>继续从队列中取出下一个任务</li>
     *   <li>如果还有任务，重复执行步骤3和4</li>
     *   <li>如果队列中没有更多任务，返回{@code true}</li>
     * </ol>
     * </p>
     * <p>
     * 该方法使用{@link #pollTaskFrom(Queue)}从队列中获取任务，这允许子类可以定制任务获取的行为。
     * 同时，方法使用{@link #safeExecute(Runnable)}来执行任务，确保即使任务执行过程中抛出异常，
     * 也不会中断整个任务处理循环，提高了系统的健壮性。
     * </p>
     * <p>
     * 注意：此方法仅在当前EventLoop线程中调用才是安全的，不建议从其他线程调用此方法。
     * </p>
     *
     * @param taskQueue 要处理的任务队列，方法将从此队列中取出并执行所有任务
     * @return {@code true} 如果至少执行了一个任务；{@code false} 如果队列中没有任务可执行
     * 
     * @see #pollTaskFrom(Queue)
     * @see #safeExecute(Runnable)
     */
    protected final boolean runAllTasksFrom(Queue<Runnable> taskQueue) {
        Runnable task = pollTaskFrom(taskQueue);
        if (task == null) {
            return false;
        }
        for (;;) {
            safeExecute(task);
            task = pollTaskFrom(taskQueue);
            if (task == null) {
                return true;
            }
        }
    }

    /**
     * Poll all tasks from the task queue and run them via {@link Runnable#run()}
     * method. This method stops running
     * the tasks in the task queue and returns if it ran longer than
     * {@code timeoutNanos}.
     */
    protected boolean runAllTasks(long timeoutNanos) {
        fetchFromScheduledTaskQueue();
        Runnable task = pollTask();
        if (task == null) {
            afterRunningAllTasks();
            return false;
        }

        final long deadline = timeoutNanos > 0 ? getCurrentTimeNanos() + timeoutNanos : 0;
        long runTasks = 0;
        long lastExecutionTime;
        for (;;) {
            safeExecute(task);

            runTasks++;

            // Check timeout every 64 tasks because nanoTime() is relatively expensive.
            // XXX: Hard-coded value - will make it configurable if it is really a problem.
            if ((runTasks & 0x3F) == 0) {
                lastExecutionTime = getCurrentTimeNanos();
                if (lastExecutionTime >= deadline) {
                    break;
                }
            }

            task = pollTask();
            if (task == null) {
                lastExecutionTime = getCurrentTimeNanos();
                break;
            }
        }

        afterRunningAllTasks();
        this.lastExecutionTime = lastExecutionTime;
        return true;
    }

    /**
     * Invoked before returning from {@link #runAllTasks()} and
     * {@link #runAllTasks(long)}.
     */
    protected void afterRunningAllTasks() {
    }

    /**
     * Returns the amount of time left until the scheduled task with the closest
     * dead line is executed.
     */
    protected long delayNanos(long currentTimeNanos) {
        currentTimeNanos -= initialNanoTime();

        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        if (scheduledTask == null) {
            return SCHEDULE_PURGE_INTERVAL;
        }

        return scheduledTask.delayNanos(currentTimeNanos);
    }

    /**
     * Returns the absolute point in time (relative to
     * {@link #getCurrentTimeNanos()}) at which the next
     * closest scheduled task should run.
     */
    protected long deadlineNanos() {
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        if (scheduledTask == null) {
            return getCurrentTimeNanos() + SCHEDULE_PURGE_INTERVAL;
        }
        return scheduledTask.deadlineNanos();
    }

    /**
     * Updates the internal timestamp that tells when a submitted task was executed
     * most recently.
     * {@link #runAllTasks()} and {@link #runAllTasks(long)} updates this timestamp
     * automatically, and thus there's
     * usually no need to call this method. However, if you take the tasks manually
     * using {@link #takeTask()} or
     * {@link #pollTask()}, you have to call this method at the end of task
     * execution loop for accurate quiet period
     * checks.
     */
    protected void updateLastExecutionTime() {
        lastExecutionTime = getCurrentTimeNanos();
    }

    /**
     * Run the tasks in the {@link #taskQueue}
     */
    protected abstract void run();

    /**
     * Do nothing, sub-classes may override
     */
    protected void cleanup() {
        // NOOP
    }

    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop) {
            // Use offer as we actually only need this to unblock the thread and if offer
            // fails we do not care as there
            // is already something in the queue.
            taskQueue.offer(WAKEUP_TASK);
        }
    }

    @Override
    public boolean inEventLoop(Thread thread) {
        return thread == this.thread;
    }

    /**
     * Add a {@link Runnable} which will be executed on shutdown of this instance
     */
    public void addShutdownHook(final Runnable task) {
        if (inEventLoop()) {
            shutdownHooks.add(task);
        } else {
            execute(new Runnable() {
                @Override
                public void run() {
                    shutdownHooks.add(task);
                }
            });
        }
    }

    /**
     * Remove a previous added {@link Runnable} as a shutdown hook
     */
    public void removeShutdownHook(final Runnable task) {
        if (inEventLoop()) {
            shutdownHooks.remove(task);
        } else {
            execute(new Runnable() {
                @Override
                public void run() {
                    shutdownHooks.remove(task);
                }
            });
        }
    }

    private boolean runShutdownHooks() {
        boolean ran = false;
        // Note shutdown hooks can add / remove shutdown hooks.
        while (!shutdownHooks.isEmpty()) {
            List<Runnable> copy = new ArrayList<Runnable>(shutdownHooks);
            shutdownHooks.clear();
            for (Runnable task : copy) {
                try {
                    runTask(task);
                } catch (Throwable t) {
                    logger.warn("Shutdown hook raised an exception.", t);
                } finally {
                    ran = true;
                }
            }
        }

        if (ran) {
            lastExecutionTime = getCurrentTimeNanos();
        }

        return ran;
    }

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        ObjectUtil.checkPositiveOrZero(quietPeriod, "quietPeriod");
        if (timeout < quietPeriod) {
            throw new IllegalArgumentException(
                    "timeout: " + timeout + " (expected >= quietPeriod (" + quietPeriod + "))");
        }
        ObjectUtil.checkNotNull(unit, "unit");

        if (isShuttingDown()) {
            return terminationFuture();
        }

        boolean inEventLoop = inEventLoop();
        boolean wakeup;
        int oldState;
        for (;;) {
            if (isShuttingDown()) {
                return terminationFuture();
            }
            int newState;
            wakeup = true;
            oldState = state;
            if (inEventLoop) {
                newState = ST_SHUTTING_DOWN;
            } else {
                switch (oldState) {
                    case ST_NOT_STARTED:
                    case ST_STARTED:
                        newState = ST_SHUTTING_DOWN;
                        break;
                    default:
                        newState = oldState;
                        wakeup = false;
                }
            }
            if (STATE_UPDATER.compareAndSet(this, oldState, newState)) {
                break;
            }
        }
        gracefulShutdownQuietPeriod = unit.toNanos(quietPeriod);
        gracefulShutdownTimeout = unit.toNanos(timeout);

        if (ensureThreadStarted(oldState)) {
            return terminationFuture;
        }

        if (wakeup) {
            taskQueue.offer(WAKEUP_TASK);
            if (!addTaskWakesUp) {
                wakeup(inEventLoop);
            }
        }

        return terminationFuture();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationFuture;
    }

    @Override
    @Deprecated
    public void shutdown() {
        if (isShutdown()) {
            return;
        }

        boolean inEventLoop = inEventLoop();
        boolean wakeup;
        int oldState;
        for (;;) {
            if (isShuttingDown()) {
                return;
            }
            int newState;
            wakeup = true;
            oldState = state;
            if (inEventLoop) {
                newState = ST_SHUTDOWN;
            } else {
                switch (oldState) {
                    case ST_NOT_STARTED:
                    case ST_STARTED:
                    case ST_SHUTTING_DOWN:
                        newState = ST_SHUTDOWN;
                        break;
                    default:
                        newState = oldState;
                        wakeup = false;
                }
            }
            if (STATE_UPDATER.compareAndSet(this, oldState, newState)) {
                break;
            }
        }

        if (ensureThreadStarted(oldState)) {
            return;
        }

        if (wakeup) {
            taskQueue.offer(WAKEUP_TASK);
            if (!addTaskWakesUp) {
                wakeup(inEventLoop);
            }
        }
    }

    @Override
    public boolean isShuttingDown() {
        return state >= ST_SHUTTING_DOWN;
    }

    @Override
    public boolean isShutdown() {
        return state >= ST_SHUTDOWN;
    }

    @Override
    public boolean isTerminated() {
        return state == ST_TERMINATED;
    }

    /**
     * Confirm that the shutdown if the instance should be done now!
     */
    protected boolean confirmShutdown() {
        if (!isShuttingDown()) {
            return false;
        }

        if (!inEventLoop()) {
            throw new IllegalStateException("must be invoked from an event loop");
        }

        cancelScheduledTasks();

        if (gracefulShutdownStartTime == 0) {
            gracefulShutdownStartTime = getCurrentTimeNanos();
        }

        if (runAllTasks() || runShutdownHooks()) {
            if (isShutdown()) {
                // Executor shut down - no new tasks anymore.
                return true;
            }

            // There were tasks in the queue. Wait a little bit more until no tasks are
            // queued for the quiet period or
            // terminate if the quiet period is 0.
            // See https://github.com/netty/netty/issues/4241
            if (gracefulShutdownQuietPeriod == 0) {
                return true;
            }
            taskQueue.offer(WAKEUP_TASK);
            return false;
        }

        final long nanoTime = getCurrentTimeNanos();

        if (isShutdown() || nanoTime - gracefulShutdownStartTime > gracefulShutdownTimeout) {
            return true;
        }

        if (nanoTime - lastExecutionTime <= gracefulShutdownQuietPeriod) {
            // Check if any tasks were added to the queue every 100ms.
            // TODO: Change the behavior of takeTask() so that it returns on timeout.
            taskQueue.offer(WAKEUP_TASK);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // Ignore
            }

            return false;
        }

        // No tasks were added for last quiet period - hopefully safe to shut down.
        // (Hopefully because we really cannot make a guarantee that there will be no
        // execute() calls by a user.)
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        ObjectUtil.checkNotNull(unit, "unit");
        if (inEventLoop()) {
            throw new IllegalStateException("cannot await termination of the current thread");
        }

        threadLock.await(timeout, unit);

        return isTerminated();
    }

    @Override
    public void execute(Runnable task) {
        execute0(task);
    }

    @Override
    public void lazyExecute(Runnable task) {
        lazyExecute0(task);
    }

    private void execute0(@Schedule Runnable task) {
        ObjectUtil.checkNotNull(task, "task");
        execute(task, wakesUpForTask(task));
    }

    private void lazyExecute0(@Schedule Runnable task) {
        execute(ObjectUtil.checkNotNull(task, "task"), false);
    }

    private void execute(Runnable task, boolean immediate) {
        boolean inEventLoop = inEventLoop();
        addTask(task);
        if (!inEventLoop) {
            startThread();
            if (isShutdown()) {
                boolean reject = false;
                try {
                    if (removeTask(task)) {
                        reject = true;
                    }
                } catch (UnsupportedOperationException e) {
                    // The task queue does not support removal so the best thing we can do is to
                    // just move on and
                    // hope we will be able to pick-up the task before its completely terminated.
                    // In worst case we will log on termination.
                }
                if (reject) {
                    reject();
                }
            }
        }

        if (!addTaskWakesUp && immediate) {
            wakeup(inEventLoop);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throwIfInEventLoop("invokeAny");
        return super.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throwIfInEventLoop("invokeAny");
        return super.invokeAny(tasks, timeout, unit);
    }

    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        throwIfInEventLoop("invokeAll");
        return super.invokeAll(tasks);
    }

    @Override
    public <T> List<java.util.concurrent.Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throwIfInEventLoop("invokeAll");
        return super.invokeAll(tasks, timeout, unit);
    }

    private void throwIfInEventLoop(String method) {
        if (inEventLoop()) {
            throw new RejectedExecutionException("Calling " + method + " from within the EventLoop is not allowed");
        }
    }

    /**
     * Returns the {@link ThreadProperties} of the {@link Thread} that powers the
     * {@link SingleThreadEventExecutor}.
     * If the {@link SingleThreadEventExecutor} is not started yet, this operation
     * will start it and block until
     * it is fully started.
     */
    public final ThreadProperties threadProperties() {
        ThreadProperties threadProperties = this.threadProperties;
        if (threadProperties == null) {
            Thread thread = this.thread;
            if (thread == null) {
                assert !inEventLoop();
                submit(NOOP_TASK).syncUninterruptibly();
                thread = this.thread;
                assert thread != null;
            }

            threadProperties = new DefaultThreadProperties(thread);
            if (!PROPERTIES_UPDATER.compareAndSet(this, null, threadProperties)) {
                threadProperties = this.threadProperties;
            }
        }

        return threadProperties;
    }

    /**
     * @deprecated override {@link SingleThreadEventExecutor#wakesUpForTask} to
     *             re-create this behaviour
     */
    @Deprecated
    protected interface NonWakeupRunnable extends LazyRunnable {
    }

    /**
     * Can be overridden to control which tasks require waking the
     * {@link EventExecutor} thread
     * if it is waiting so that they can be run immediately.
     */
    protected boolean wakesUpForTask(Runnable task) {
        return true;
    }

    protected static void reject() {
        throw new RejectedExecutionException("event executor terminated");
    }

    /**
     * Offers the task to the associated {@link RejectedExecutionHandler}.
     *
     * @param task to reject.
     */
    protected final void reject(Runnable task) {
        rejectedExecutionHandler.rejected(task, this);
    }

    // ScheduledExecutorService implementation

    private static final long SCHEDULE_PURGE_INTERVAL = TimeUnit.SECONDS.toNanos(1);

    private void startThread() {
        if (state == ST_NOT_STARTED) {
            if (STATE_UPDATER.compareAndSet(this, ST_NOT_STARTED, ST_STARTED)) {
                boolean success = false;
                try {
                    doStartThread();
                    success = true;
                } finally {
                    if (!success) {
                        STATE_UPDATER.compareAndSet(this, ST_STARTED, ST_NOT_STARTED);
                    }
                }
            }
        }
    }

    private boolean ensureThreadStarted(int oldState) {
        if (oldState == ST_NOT_STARTED) {
            try {
                doStartThread();
            } catch (Throwable cause) {
                STATE_UPDATER.set(this, ST_TERMINATED);
                terminationFuture.tryFailure(cause);

                if (!(cause instanceof Exception)) {
                    // Also rethrow as it may be an OOME for example
                    PlatformDependent.throwException(cause);
                }
                return true;
            }
        }
        return false;
    }

    private void doStartThread() {
        assert thread == null;
        executor.execute(new Runnable() {
            @Override
            public void run() {
                thread = Thread.currentThread();
                if (interrupted) {
                    thread.interrupt();
                }

                boolean success = false;
                Throwable unexpectedException = null;
                updateLastExecutionTime();
                try {
                    SingleThreadEventExecutor.this.run();
                    success = true;
                } catch (Throwable t) {
                    unexpectedException = t;
                    logger.warn("Unexpected exception from an event executor: ", t);
                } finally {
                    for (;;) {
                        int oldState = state;
                        if (oldState >= ST_SHUTTING_DOWN || STATE_UPDATER.compareAndSet(
                                SingleThreadEventExecutor.this, oldState, ST_SHUTTING_DOWN)) {
                            break;
                        }
                    }

                    // Check if confirmShutdown() was called at the end of the loop.
                    if (success && gracefulShutdownStartTime == 0) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Buggy " + EventExecutor.class.getSimpleName() + " implementation; " +
                                    SingleThreadEventExecutor.class.getSimpleName() + ".confirmShutdown() must " +
                                    "be called before run() implementation terminates.");
                        }
                    }

                    try {
                        // Run all remaining tasks and shutdown hooks. At this point the event loop
                        // is in ST_SHUTTING_DOWN state still accepting tasks which is needed for
                        // graceful shutdown with quietPeriod.
                        for (;;) {
                            if (confirmShutdown()) {
                                break;
                            }
                        }

                        // Now we want to make sure no more tasks can be added from this point. This is
                        // achieved by switching the state. Any new tasks beyond this point will be
                        // rejected.
                        for (;;) {
                            int oldState = state;
                            if (oldState >= ST_SHUTDOWN || STATE_UPDATER.compareAndSet(
                                    SingleThreadEventExecutor.this, oldState, ST_SHUTDOWN)) {
                                break;
                            }
                        }

                        // We have the final set of tasks in the queue now, no more can be added, run
                        // all remaining.
                        // No need to loop here, this is the final pass.
                        confirmShutdown();
                    } finally {
                        try {
                            cleanup();
                        } finally {
                            // Lets remove all FastThreadLocals for the Thread as we are about to terminate
                            // and notify
                            // the future. The user may block on the future and once it unblocks the JVM may
                            // terminate
                            // and start unloading classes.
                            // See https://github.com/netty/netty/issues/6596.
                            FastThreadLocal.removeAll();

                            STATE_UPDATER.set(SingleThreadEventExecutor.this, ST_TERMINATED);
                            threadLock.countDown();
                            int numUserTasks = drainTasks();
                            if (numUserTasks > 0 && logger.isWarnEnabled()) {
                                logger.warn("An event executor terminated with " +
                                        "non-empty task queue (" + numUserTasks + ')');
                            }
                            if (unexpectedException == null) {
                                terminationFuture.setSuccess(null);
                            } else {
                                terminationFuture.setFailure(unexpectedException);
                            }
                        }
                    }
                }
            }
        });
    }

    final int drainTasks() {
        int numTasks = 0;
        for (;;) {
            Runnable runnable = taskQueue.poll();
            if (runnable == null) {
                break;
            }
            // WAKEUP_TASK should be just discarded as these are added internally.
            // The important bit is that we not have any user tasks left.
            if (WAKEUP_TASK != runnable) {
                numTasks++;
            }
        }
        return numTasks;
    }

    private static final class DefaultThreadProperties implements ThreadProperties {
        private final Thread t;

        DefaultThreadProperties(Thread t) {
            this.t = t;
        }

        @Override
        public State state() {
            return t.getState();
        }

        @Override
        public int priority() {
            return t.getPriority();
        }

        @Override
        public boolean isInterrupted() {
            return t.isInterrupted();
        }

        @Override
        public boolean isDaemon() {
            return t.isDaemon();
        }

        @Override
        public String name() {
            return t.getName();
        }

        @Override
        public long id() {
            return t.getId();
        }

        @Override
        public StackTraceElement[] stackTrace() {
            return t.getStackTrace();
        }

        @Override
        public boolean isAlive() {
            return t.isAlive();
        }
    }
}
