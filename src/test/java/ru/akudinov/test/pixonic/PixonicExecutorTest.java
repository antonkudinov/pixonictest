package ru.akudinov.test.pixonic;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.Assert.*;

/**
 * Created by akudinov on 06.10.16.
 */
@Slf4j
public class PixonicExecutorTest {
    private PixonicExecutor pixonicExecutor;

    @Before
    public void setUp() throws Exception {
        pixonicExecutor = new PixonicExecutor();
    }

    @Test
    public void testSimpleExecute() throws InterruptedException {
        log.info("testSimpleExecute");
        LocalDateTime dt = LocalDateTime.now();
        dt.plus(2, SECONDS);
        Callable<String> callable = () -> "OK";

        pixonicExecutor.execute(dt, callable);

        waitForAllTasksExecuted();
    }


    @Test
    public void testMultiplyExecute() throws InterruptedException {
        log.info("testMultiplyExecute");
        final LocalDateTime dt = LocalDateTime.now();
        Callable<String> callable = () -> "OK";

        List<Callable<String>> tasks = new ArrayList<>();
        for (int i = 0; i <= 20; i++) {
            tasks.add(() -> {
                PixonixEvent event = pixonicExecutor.execute(dt.plus((int)(Math.random() * 10), SECONDS), callable);
                return String.valueOf(event.getId());
            });
        }

        List<Future<String>> futures = Executors.newCachedThreadPool().invokeAll(tasks);

        futures.forEach((f) -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        waitForAllTasksExecuted();
    }

    @Test
    public void testScheduledExecute() throws InterruptedException {
        log.info("testScheduledExecute");
        final LocalDateTime dt = LocalDateTime.now();
        Callable<String> callable = () -> "OK";

        PixonixEvent pe1 = pixonicExecutor.execute(dt.plus(5, SECONDS), callable);
        PixonixEvent pe2 = pixonicExecutor.execute(dt.plus(10, SECONDS), callable);

        assertEquals("PBQ size", 2, pixonicExecutor.getState().getQueueSize().intValue());
        waitForAllTasksExecuted();
        assertEquals("PBQ size", 0, pixonicExecutor.getState().getQueueSize().intValue());
        assertNotNull("StartDateTime event1 is not null", pe1.getStartDateTime());
        assertNotNull("StartDateTime event2 is not null", pe2.getStartDateTime());
    }

    @Test
    public void testOrderedExecute() throws InterruptedException {
        log.info("testOrderedExecute");
        final LocalDateTime dt = LocalDateTime.now();
        Callable<String> callable = () -> "OK";

        PixonixEvent pe2 = pixonicExecutor.execute(dt.plus(10, SECONDS), callable);
        PixonixEvent pe1 = pixonicExecutor.execute(dt.plus(5, SECONDS), callable);

        assertEquals("PBQ size", 2, pixonicExecutor.getState().getQueueSize().intValue());

        waitForAllTasksExecuted();

        assertTrue("Event1 earlier than  Event2", pe1.getDateTime().isBefore(pe2.getDateTime()));
        assertTrue("Event executed in right order", pe1.getStartDateTime().isBefore(pe2.getStartDateTime()));
    }

    @Test
    public void testOrderedExecuteForSameDateTime() throws InterruptedException {
        log.info("testOrderedExecuteForSameDateTime");
        final LocalDateTime dt = LocalDateTime.now();
        Callable<String> callable = () -> "OK";

        PixonixEvent pe1 = pixonicExecutor.execute(dt.plus(1, SECONDS), callable);
        PixonixEvent pe2 = pixonicExecutor.execute(dt.plus(1, SECONDS), callable);

        assertEquals("PBQ size", 2, pixonicExecutor.getState().getQueueSize().intValue());

        waitForAllTasksExecuted();

        assertTrue("Event1 at same time with Event2", pe1.getDateTime().equals(pe2.getDateTime()));
        assertTrue("Event executed in right order [" + pe1 + " and " + pe2 + "]",
                pe1.getId() < pe2.getId() &&
                        (pe1.getStartDateTime().isBefore(pe2.getStartDateTime()) ||
                        pe1.getStartDateTime().isEqual(pe2.getStartDateTime()))
        );

    }

    private void waitForAllTasksExecuted() throws InterruptedException {
        while (pixonicExecutor.getState().getQueueSize() != 0) {
            TimeUnit.SECONDS.sleep(1);
        }
    }


}