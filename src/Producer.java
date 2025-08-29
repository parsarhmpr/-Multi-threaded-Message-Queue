import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer implements Runnable {
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