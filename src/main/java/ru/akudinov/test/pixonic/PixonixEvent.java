package ru.akudinov.test.pixonic;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *Created by akudinov on 07.10.16.
 */
@Data
@Slf4j
@ToString(exclude = {"callable"})
public class PixonixEvent<T> implements Comparable<PixonixEvent>, Callable<T>{
    private final static AtomicInteger counter = new AtomicInteger(1);

    private final LocalDateTime dateTime;
    private LocalDateTime startDateTime;
    private final Callable<T> callable;
    private final Integer id;

    @java.beans.ConstructorProperties({"dateTime", "callable"})
    public PixonixEvent(LocalDateTime dateTime, Callable<T> callable) {
        if (dateTime == null){
            throw new IllegalArgumentException("Empty dateTime argument");
        }
        if (callable == null){
            throw new IllegalArgumentException("Empty callable argument");
        }

        this.dateTime = dateTime;
        this.callable = callable;
        this.id = counter.getAndIncrement();
    }


    @Override
    public int compareTo(PixonixEvent o) {
        if (dateTime == null || o.getDateTime() == null) return 1;
        int res = dateTime.compareTo(o.getDateTime());
        return res == 0 ? id.compareTo(o.getId()) : res;
    }

    @Override
    public T call() throws Exception {
        startDateTime = LocalDateTime.now();
        log.debug("Call event {}", this);
        T call = callable.call();
        log.debug("Call event {} result {}", this, call);
        return call;
    }
}
