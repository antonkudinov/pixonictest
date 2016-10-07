package ru.akudinov.test.pixonic;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Execute events based on its scheduling date and ordering
 */
@Slf4j
public class PixonicExecutor {

    public static final int DEFAULT_DELAY_SECONDS = 1;
    public static final String PIXONIC_EXECUTOR_THREAD = "Pixonic Executor Thread";
    public static final String PIXONIC_SCHEDULER_THREAD = "Pixonic Scheduler Thread";

    private PriorityBlockingQueue<PixonixEvent> pbq = new PriorityBlockingQueue<>();
    private ExecutorService executorThreadPool;

    private Lock lock = new ReentrantLock();

    public PixonicExecutor() {
        this(DEFAULT_DELAY_SECONDS);
    }

    /**
     *
     * @param schedulerDelay  delay for sheduledWithFixedDelay method. It's a tick
     */
    public PixonicExecutor(int schedulerDelay) {

        executorThreadPool = Executors.newCachedThreadPool(PixonixThreadFactoryCreator.getThreadFactory(PIXONIC_EXECUTOR_THREAD));

        Executors.newSingleThreadScheduledExecutor(PixonixThreadFactoryCreator.getThreadFactory(PIXONIC_SCHEDULER_THREAD)).scheduleWithFixedDelay(
                () -> {
                    lock.lock();
                    try {
                        PixonixEvent event = pbq.peek(); //take event without removing from Q
                        log.debug("Peek event {} from Q", event);
                        if (event != null) {
                            log.debug("First check event {} for ready", event);
                            boolean initialEventReadyStatus = checkEventIsReady(event);
                            if (initialEventReadyStatus) {
                                while (true) {
                                    log.debug("Poll event {} from Q at {} ", event, LocalDateTime.now());
                                    event = pbq.poll();  //take event with removing from Q
                                    log.debug("Check event {} for ready", event);
                                    if (initialEventReadyStatus || checkEventIsReady(event)) {
                                        log.debug("Execute event {} from Q", event);
                                        executorThreadPool.submit(event);  // try to execute task
                                        initialEventReadyStatus = false;
                                    } else {  // not ready or empty event
                                        if (event != null) {
                                            log.debug("Put event back {} to Q", event);
                                            pbq.put(event); //put event back to Q
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                },
                0,
                schedulerDelay,
                TimeUnit.SECONDS
        );
    }

    final static class PixonixThreadFactoryCreator {
        private final static ConcurrentHashMap<String, AtomicInteger> counter = new ConcurrentHashMap<>();

        static ThreadFactory getThreadFactory(String name) {
            return r -> new Thread(r, name + "-" + counter.computeIfAbsent(name, k -> new AtomicInteger(1)).getAndIncrement());
        }

    }


    private boolean checkEventIsReady(PixonixEvent event) {
        boolean result = event != null && event.getDateTime().compareTo(LocalDateTime.now()) <= 0;
        log.debug("Event ready status is {} ", result);
        return result;
    }

    public PixonixEvent execute(LocalDateTime dt, Callable<?> callable) {
        lock.lock();
        try {
            PixonixEvent<?> event = new PixonixEvent<>(dt, callable);
            log.debug("Try to put event {} to Q", event);
            pbq.put(event);
            return event;
        } finally {
            lock.unlock();
        }
    }


    @Data
    @RequiredArgsConstructor
    public static class State{
        private final Integer queueSize;
        private final Integer executorPoolSize;
    }

    public State getState(){
        return new State(
                pbq.size(),
                PixonixThreadFactoryCreator.counter.getOrDefault(PIXONIC_EXECUTOR_THREAD, new AtomicInteger(0)).get());
    }
}
