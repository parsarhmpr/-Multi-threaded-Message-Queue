import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class EventLogger {
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