import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class Message implements Comparable<Message> {
    public final int id;
    public final String content;
    public final long deadline;
    public static final Message POISON = new Message(-1, "POISON", Long.MAX_VALUE);

    public Message(int id, String content, long deadline) {
        this.id = id;
        this.content = content;
        this.deadline = deadline;
    }

    @Override
    public String toString() {
        if (this == POISON) return "{POISON}";
        long remain = deadline - System.currentTimeMillis();
        if (remain <= 0) return "{id = " + id + ", expired}";
        return "{id = " + id + ", remain = " + remain + "}";
    }

    @Override
    public int compareTo(Message other) {
        return Long.compare(this.deadline, other.deadline);
    }
}

class MessageWithSize {
    public final Message message;
    public final int queueSize;
    public MessageWithSize(Message message, int queueSize) {
        this.message = message;
        this.queueSize = queueSize;
    }
}

class EventLogger {
    private static final BlockingQueue<String> logQueue = new LinkedBlockingQueue<>();
    private static final String POISON = "__LOGGER_POISON__";
    private static Thread loggerThread;

    public static void start() {
        loggerThread = new Thread(() -> {
            try {
                while (true) {
                    String line = logQueue.take();
                    if (POISON.equals(line)) break;
                    System.out.printf("%s%n", line);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                List<String> remaining = new ArrayList<>();
                logQueue.drainTo(remaining);
                for (String l : remaining) {
                    if (POISON.equals(l)) break;
                    System.out.printf("%s%n", l);
                }
                System.out.println("Logger stopped.");
            }
        }, "EventLogger");
        loggerThread.setDaemon(true);
        loggerThread.start();
    }

    public static void log(String s) {
        try {
            logQueue.put(s);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void stopAndFlush() throws InterruptedException {
        logQueue.put(POISON);
        if (loggerThread != null) loggerThread.join();
    }
}

class MessageQueue {
    private final PriorityQueue<Message> heap = new PriorityQueue<>();
    private final int capacity;
    private final String name;

    public MessageQueue(int capacity, String name) {
        this.capacity = capacity;
        this.name = name;
    }

    public synchronized void putAndLog(Message msg, int producerId) throws InterruptedException {
        while (heap.size() == capacity) wait();
        heap.add(msg);
        int sizeAfter = heap.size();
        EventLogger.log(String.format("[Producer %d] produced %s to %s (queue size = %d)", producerId, msg, name, sizeAfter));
        notifyAll();
    }

    public synchronized MessageWithSize takeAndLog(int consumerId) throws InterruptedException {
        while (true) {
            while (heap.isEmpty()) wait();
            long now = System.currentTimeMillis();
            Message top = heap.peek();
            if (top == null) { wait(); continue; }
            if (top == Message.POISON) {
                Message p = heap.poll();
                notifyAll();
                EventLogger.log(String.format("[Consumer %d] received POISON from %s (queue size = %d)", consumerId, name, heap.size()));
                return new MessageWithSize(p, heap.size());
            }
            now = System.currentTimeMillis();
            if (top.deadline > 0 && top.deadline < now) {
                Message removed = heap.poll();
                int sizeAfter = heap.size();
                EventLogger.log(String.format("[Consumer %d] consumed %s from %s (queue size = %d) (expired = true)", consumerId, removed, name, sizeAfter));
                notifyAll();
                continue;
            }
            Message msg = heap.poll();
            int sizeAfter = heap.size();
            EventLogger.log(String.format("[Consumer %d] consumed %s from %s (queue size = %d) (expired = false)", consumerId, msg, name, sizeAfter));
            notifyAll();
            return new MessageWithSize(msg, sizeAfter);
        }
    }

    public synchronized void purgeExpired() {
        while (true) {
            Message top = heap.peek();
            long now = System.currentTimeMillis();
            if (top == null) break;
            if (top == Message.POISON) break;
            if (top.deadline > 0 && top.deadline < now) {
                Message rem = heap.poll();
                EventLogger.log(String.format("[GC] purged expired %s from %s", rem, name));
            } else break;
        }
        notifyAll();
    }

    public synchronized int size() { return heap.size(); }
    public String getName() { return name; }
}

class Producer implements Runnable {
    private final int producerId;
    private final MessageQueue queue;
    private final int messagesToProduce;
    private final AtomicInteger globalMessageId;
    private final Random rnd = new Random();
    private final boolean RETENTION_POLICY;

    public Producer(int producerId, MessageQueue queue, int messagesToProduce, AtomicInteger globalMessageId, boolean RETENTION_POLICY) {
        this.producerId = producerId;
        this.queue = queue;
        this.messagesToProduce = messagesToProduce;
        this.globalMessageId = globalMessageId;
        this.RETENTION_POLICY = RETENTION_POLICY;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < messagesToProduce; i++) {
                int msgId = globalMessageId.incrementAndGet();
                long INFINITY = 1000_000;
                long ttl = RETENTION_POLICY ? 100 + rnd.nextInt(700) : INFINITY;
                long deadline = System.currentTimeMillis() + ttl;
                Message msg = new Message(msgId, "FromP" + producerId + "-msg" + (i + 1), deadline);
                queue.putAndLog(msg, producerId);
                Thread.sleep(10 + rnd.nextInt(40));
            }
            EventLogger.log(String.format("[Producer %d] finished producing.", producerId));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            EventLogger.log(String.format("[Producer %d] interrupted.", producerId));
        }
    }
}

class Consumer implements Runnable {
    private final int consumerId;
    private final MessageQueue queue;
    private final Random rnd = new Random();

    public Consumer(int consumerId, MessageQueue queue) {
        this.consumerId = consumerId;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                MessageWithSize result = queue.takeAndLog(consumerId);
                Message msg = result.message;
                if (msg == Message.POISON) break;
                Thread.sleep(100 + rnd.nextInt(300));
            }
            EventLogger.log(String.format("[Consumer %d] exiting run loop.", consumerId));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            EventLogger.log(String.format("[Consumer %d] interrupted.", consumerId));
        }
    }
}

class GarbageCollector implements Runnable {
    private final List<MessageQueue> queues;
    private final long intervalMs;
    private volatile boolean running = true;

    public GarbageCollector(List<MessageQueue> queues, long intervalMs) {
        this.queues = queues;
        this.intervalMs = intervalMs;
    }

    public void stop() { running = false; }

    @Override
    public void run() {
        try {
            while (running) {
                for (MessageQueue q : queues) q.purgeExpired();
                Thread.sleep(intervalMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            EventLogger.log("[GC] stopped.");
        }
    }
}

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

