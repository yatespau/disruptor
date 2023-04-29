package com.yates.training.disruptor;

import com.lmax.disruptor.EventHandler;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LongEventHandler  implements EventHandler<LongEvent> {

    public AtomicLong endTime;
    private int max;
    public AtomicInteger counter = new AtomicInteger();

    public LongEventHandler(int max) {
        this.max = max;
    }

    @Override
    public void onEvent(LongEvent event, long sequence, boolean endOfBath) {

        counter.incrementAndGet();

        if (event.getValue() == max) {
            endTime = new AtomicLong(System.currentTimeMillis());
        }
    }
}
