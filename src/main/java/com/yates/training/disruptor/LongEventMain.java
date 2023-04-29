package com.yates.training.disruptor;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LongEventMain {
    public static void main(String[] args) throws Exception {
        int max = 100000000;
        runDisruptorExample(max);
        arrayBlockingQueueExample(max);
    }

    private void runPerfExample() throws Exception {
        long start = System.currentTimeMillis();
        int counter = 0;
        for (; counter < 500000000; counter++) {

        }
        System.out.println("No Lock: Counter= " + counter + " time= " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        counter = 0;
        Object lock = new Object();
        while (counter < 500000000) {

            synchronized (lock) {
                counter++;
            }
        }
        System.out.println("Lock: Counter= " + counter + " time= " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        Thread t1 = new Thread(new CounterThread(lock));
        Thread t2 = new Thread(new CounterThread(lock));

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        System.out.println("2 threads: Counter= " + counter + " time= " + (System.currentTimeMillis() - start));
    }

    private static void runDisruptorExample(int max) throws Exception {

        LongEventFactory factory = new LongEventFactory();
        LongEventHandler longEventHandler  = new LongEventHandler(max);


        int bufferSize = 262144; //8sec
        Disruptor<LongEvent> disruptor =
                new Disruptor<>(factory,
                        bufferSize,
                        DaemonThreadFactory.INSTANCE,
                        ProducerType.SINGLE,
                        new BusySpinWaitStrategy());
        disruptor.handleEventsWith(longEventHandler);
        disruptor.start();

        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();
        LongEventProducer producer = new LongEventProducer(ringBuffer);
        ByteBuffer bb = ByteBuffer.allocate(8);
       long start = System.currentTimeMillis();
       long l = 1;

       int ops = 0;
       boolean opsRecorded = false;

        for (l = 1; l <= max ; l++) {
            bb.putLong(0, l);
            producer.onData(bb);

            if (!opsRecorded && System.currentTimeMillis() - start >= 1000) {
                opsRecorded = true;
                System.out.println("Disruptor ops= " + longEventHandler.counter.intValue());
            }
        }
        while (longEventHandler.endTime == null) {

            Thread.sleep(500);
        }

        System.out.println("Disruptor Duration[" + max + "]:" + (longEventHandler.endTime.longValue() - start));

    }

    private static void arrayBlockingQueueExample(int max) throws Exception {

        ArrayBlockingQueue<Long> queue = new ArrayBlockingQueue<>(1000000);
        ExecutorService service = Executors.newSingleThreadExecutor();
        ArrayBlockingQueueReader reader = new ArrayBlockingQueueReader(queue, max);
        service.submit(reader);
        Long start = System.currentTimeMillis();
        boolean opsRecorded = false;

        for (long l = 1; l <= max; l++) {

            queue.put(Long.valueOf(l));
            if (!opsRecorded && System.currentTimeMillis() - start >= 1000) {
                opsRecorded = true;
                System.out.println("ArrayBlockingQueue ops= " + reader.counter.intValue());
            }
        }

        while (reader.endTime == null) {
            Thread.sleep(500);
        }

        System.out.println("ArrayBlockingQueue Duration[" + max +"]" + (reader.endTime.longValue() - start));

        System.exit(0);
    }

    static class  ArrayBlockingQueueReader implements Runnable {

        private ArrayBlockingQueue<Long> queue;
        private AtomicLong endTime;
        private int max;
        AtomicInteger counter = new AtomicInteger(0);

        public ArrayBlockingQueueReader(ArrayBlockingQueue<Long> queue, int max) {
                this.queue = queue;
                this.max = max;
            }

        public void run() {
            while (!Thread.interrupted()) {
                try {
                    Long l = queue.take();

                    if (l == max) {
                        endTime = new AtomicLong(System.currentTimeMillis());
                    }
                    counter.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    }
    class CounterThread implements Runnable {

        private Object lock;

        public CounterThread(Object lock) {
            this.lock = lock;
        }


        public void run() {

            int counter = 0;
            while (counter < 500000000) {

                synchronized (lock) {
                    counter++;
                }
            }
        }
    }