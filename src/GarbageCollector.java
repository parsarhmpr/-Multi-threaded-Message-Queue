import java.util.List;

public class GarbageCollector implements Runnable {
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