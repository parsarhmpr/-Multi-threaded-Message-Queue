import java.util.PriorityQueue;

public class MessageQueue {
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
            Message top = heap.peek();
            if (top == null) { wait(); continue; }
            if (top == Message.POISON) {
                Message p = heap.poll();
                notifyAll();
                EventLogger.log(String.format("[Consumer %d] received POISON from %s (queue size = %d)", consumerId, name, heap.size()));
                return new MessageWithSize(p, heap.size());
            }
            long now = System.currentTimeMillis();
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
}