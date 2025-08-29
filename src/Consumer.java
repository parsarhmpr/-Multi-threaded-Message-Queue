import java.util.Random;

public class Consumer implements Runnable {
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