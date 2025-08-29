import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageBroker {
    public static void main(String[] args) throws InterruptedException {

        //--------------EXTENSIONS--------------
        final boolean BOUNDED_BUFFER = true;
        final boolean RETENTION_POLICY = true;

        EventLogger.start();

        final int QUEUE_CAPACITY = BOUNDED_BUFFER ? 40 : 1000000;
        final int NUM_PRODUCERS = 4;
        final int NUM_CONSUMERS = 2;
        final int MESSAGES_PER_PRODUCER = 20;
        final long GC_INTERVAL_MS = 50L;

        MessageQueue queue1 = new MessageQueue(QUEUE_CAPACITY, "topic1");
        MessageQueue queue2 = new MessageQueue(QUEUE_CAPACITY, "topic2");
        List<MessageQueue> allQueues = Arrays.asList(queue1, queue2);

        AtomicInteger globalMessageId = new AtomicInteger(0);

        Thread[] producers = new Thread[NUM_PRODUCERS];
        Thread[] consumers = new Thread[NUM_CONSUMERS];

        GarbageCollector gc = new GarbageCollector(allQueues, GC_INTERVAL_MS);
        Thread gcThread = new Thread(gc, "GarbageCollector");
        if(RETENTION_POLICY) {
            gcThread.start();
        }

        for (int i = 0; i < NUM_CONSUMERS; i++) {
            MessageQueue q = (i < NUM_CONSUMERS / 2) ? queue1 : queue2;
            Consumer c = new Consumer(i + 1, q);
            Thread t = new Thread(c, "Consumer-" + (i + 1));
            consumers[i] = t;
            t.start();
        }

        for (int i = 0; i < NUM_PRODUCERS; i++) {
            MessageQueue q = (i < NUM_PRODUCERS / 2) ? queue1 : queue2;
            Producer p = new Producer(i + 1, q, MESSAGES_PER_PRODUCER, globalMessageId, RETENTION_POLICY);
            Thread t = new Thread(p, "Producer-" + (i + 1));
            producers[i] = t;
            t.start();
        }

        for (Thread p : producers) p.join();
        EventLogger.log("[Message Broker] All producers finished. Sending poison pills to consumers...");

        for (int i = 0; i < NUM_CONSUMERS / 2; i++) queue1.putAndLog(Message.POISON, 0);
        for (int i = 0; i < NUM_CONSUMERS / 2; i++) queue2.putAndLog(Message.POISON, 0);

        for (Thread c : consumers) c.join();
        EventLogger.log("[Message Broker] All consumers exited. Stopping GC...");

        if(RETENTION_POLICY) {
            gc.stop();
            gcThread.interrupt();
            gcThread.join();
        }

        EventLogger.stopAndFlush();
        System.out.println("Process finished.");
    }
}

