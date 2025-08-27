import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

class Message {
    public final int id;
    public final String content;
    public final long deadline; // epoch millis

    // Poison sentinel uses MAX so GC never removes it and it sits at the end of heap
    public static final Message POISON = new Message(-1, "POISON", Long.MAX_VALUE);

    public Message(int id, String content, long deadline) {
        this.id = id;
        this.content = content;
        this.deadline = deadline;
    }

    @Override
    public String toString() {
        if (this == POISON) return "Message{POISON}";
        long remain = deadline - System.currentTimeMillis();
        if (remain <= 0) return "Message{id=" + id + ", expired}";
        return "Message{id=" + id + ", remain(ms)=" + remain + "}";
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

class PurgeResult {
    public final int removed;
    public final int sizeAfter;

    public PurgeResult(int removed, int sizeAfter) {
        this.removed = removed;
        this.sizeAfter = sizeAfter;
    }
}

class MessageQueue {
    private final PriorityQueue<Message> heap = new PriorityQueue<>(Comparator.comparingLong(m -> m.deadline));
    private final int capacity;
    private final String name;

    public MessageQueue(int capacity, String name) {
        this.capacity = capacity;
        this.name = name;
    }

    /**
     * Put a message into the heap. Blocks if capacity reached.
     * Returns size AFTER insertion (consistent for logging).
     */
    public synchronized int put(Message msg) throws InterruptedException {
        while (heap.size() == capacity) {
            wait();
        }
        heap.add(msg);
        notifyAll();
        return heap.size();
    }

    /**
     * Take the top (earliest deadline) message, discarding expired messages encountered at the top.
     * Blocks if heap is empty (or only contains expired messages which are cleaned here).
     * Returns MessageWithSize containing the removed message and size AFTER removal.
     */
    public synchronized MessageWithSize takeAndSize() throws InterruptedException {
        while (true) {
            while (heap.isEmpty()) {
                wait();
            }

            long now = System.currentTimeMillis();
            Message top = heap.peek();

            // If top is the POISON sentinel, deliver it (consumer will exit).
            if (top == Message.POISON) {
                Message polled = heap.poll();
                notifyAll();
                return new MessageWithSize(polled, heap.size());
            }

            // If top expired, remove it and continue (consumer won't get expired messages).
            if (top.deadline < now) {
                Message removed = heap.poll();
                System.out.printf("[Queue %s] removed expired message by consumer-check: %s (now=%d)%n",
                        name, removed, now);
                notifyAll();
                // loop to check the next top
                continue;
            }

            // top is valid -> remove and return
            Message msg = heap.poll();
            notifyAll();
            return new MessageWithSize(msg, heap.size());
        }
    }

    /**
     * Purge expired messages by repeatedly checking the top of heap.
     * Does NOT remove the POISON sentinel.
     */
    public synchronized PurgeResult purgeExpired(long now) {
        int removed = 0;
        while (true) {
            Message top = heap.peek();
            if (top == null) break;
            if (top == Message.POISON) break; // do not purge poison
            if (top.deadline < now) {
                Message rem = heap.poll();
                removed++;
            } else {
                break; // top is not expired
            }
        }
        if (removed > 0) notifyAll();
        return new PurgeResult(removed, heap.size());
    }

    public synchronized int size() {
        return heap.size();
    }

    public synchronized String getName() {
        return name;
    }
}

class Producer implements Runnable {
    private final int producerId;
    private final MessageQueue queue;
    private final int messagesToProduce;
    private final AtomicInteger globalMessageId;
    private final Random rnd = new Random();

    public Producer(int producerId, MessageQueue queue, int messagesToProduce, AtomicInteger globalMessageId) {
        this.producerId = producerId;
        this.queue = queue;
        this.messagesToProduce = messagesToProduce;
        this.globalMessageId = globalMessageId;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < messagesToProduce; i++) {
                int msgId = globalMessageId.incrementAndGet();
                String content = "FromP" + producerId + "-msg" + (i + 1);
                long ttl = 500 + rnd.nextInt(4500);
                long deadline = System.currentTimeMillis() + ttl;
                Message msg = new Message(msgId, content, deadline);

                int sizeAfterPut = queue.put(msg);
                System.out.printf("[Producer-%d] produced %s to %s (queue size=%d)%n",
                        producerId, msg, queue.getName(), sizeAfterPut);

                Thread.sleep(50 + rnd.nextInt(150));
            }
            System.out.printf("[Producer-%d] finished producing.%n", producerId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.printf("[Producer-%d] interrupted.%n", producerId);
        }
    }
}

class Consumer implements Runnable {
    private final int consumerId;
    private final MessageQueue queue;

    public Consumer(int consumerId, MessageQueue queue) {
        this.consumerId = consumerId;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                MessageWithSize result = queue.takeAndSize();
                Message msg = result.message;
                int sizeAfterTake = result.queueSize;

                // poison pill -> stop
                if (msg == Message.POISON) {
                    System.out.printf("[Consumer-%d] received POISON and is exiting.%n", consumerId);
                    break;
                }

                long now = System.currentTimeMillis();
                boolean expired = (msg.deadline > 0 && msg.deadline < now);
                System.out.printf("[Consumer-%d] consumed %s from %s (queue size=%d) (expired=%b)%n",
                        consumerId, msg, queue.getName(), sizeAfterTake, expired);

                Thread.sleep(80 + new Random().nextInt(120));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.printf("[Consumer-%d] interrupted.%n", consumerId);
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

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        try {
            while (running) {
                long now = System.currentTimeMillis();
                for (int i = 0; i < queues.size(); i++) {
                    MessageQueue q = queues.get(i);
                    PurgeResult r = q.purgeExpired(now);
                    if (r.removed > 0) {
                        System.out.printf("[GC] purged %d expired messages from %s (size after=%d)%n",
                                r.removed, q.getName(), r.sizeAfter);
                    }
                }
                Thread.sleep(intervalMs);
            }
        } catch (InterruptedException e) {
            // allow thread to exit
        }
        System.out.println("[GC] stopped.");
    }
}

public class ProducerConsumerMessagesDemo {
    public static void main(String[] args) throws InterruptedException {
        final int QUEUE_CAPACITY = 50;
        final int NUM_PRODUCERS = 6;
        final int NUM_CONSUMERS = 4;
        final int MESSAGES_PER_PRODUCER = 10;
        final long GC_INTERVAL_MS = 100L;

        MessageQueue queue1 = new MessageQueue(QUEUE_CAPACITY, "topic1");
        MessageQueue queue2 = new MessageQueue(QUEUE_CAPACITY, "topic2");
        List<MessageQueue> allQueues = new ArrayList<>();
        allQueues.add(queue1);
        allQueues.add(queue2);

        AtomicInteger globalMessageId = new AtomicInteger(0);

        Thread[] producers = new Thread[NUM_PRODUCERS];
        Thread[] consumers = new Thread[NUM_CONSUMERS];

        GarbageCollector gc = new GarbageCollector(allQueues, GC_INTERVAL_MS);
        Thread gcThread = new Thread(gc, "GarbageCollector");
        gcThread.start();

        // start consumers (split between two topics)
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            Consumer c;
            if (i < NUM_CONSUMERS / 2) c = new Consumer(i + 1, queue1);
            else c = new Consumer(i + 1, queue2);
            Thread t = new Thread(c, "Consumer-" + (i + 1));
            consumers[i] = t;
            t.start();
        }

        // start producers (split between two topics)
        for (int i = 0; i < NUM_PRODUCERS; i++) {
            Producer p;
            if (i < NUM_PRODUCERS / 2) p = new Producer(i + 1, queue1, MESSAGES_PER_PRODUCER, globalMessageId);
            else p = new Producer(i + 1, queue2, MESSAGES_PER_PRODUCER, globalMessageId);
            Thread t = new Thread(p, "Producer-" + (i + 1));
            producers[i] = t;
            t.start();
        }

        // wait for all producers to finish
        for (Thread p : producers) {
            p.join();
        }
        System.out.println("[Main] All producers finished. Sending poison pills to consumers...");

        // send one poison pill per consumer for each topic
        for (int i = 0; i < NUM_CONSUMERS / 2; i++) {
            queue1.put(Message.POISON);
        }
        for (int i = 0; i < NUM_CONSUMERS / 2; i++) {
            queue2.put(Message.POISON);
        }

        // wait for consumers to finish
        for (Thread c : consumers) {
            c.join();
        }

        System.out.println("[Main] All consumers exited. Stopping GC...");

        gc.stop();
        gcThread.interrupt();
        gcThread.join();

        System.out.println("[Main] Demo finished.");
    }
}
