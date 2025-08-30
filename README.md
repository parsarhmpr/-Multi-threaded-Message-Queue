# README

## Project: MessageBroker (Producer-Consumer priority queue with GC)

### Overview
This Java project implements a priority-based producer-consumer message broker. Messages carry a deadline (epoch milliseconds) and queues are implemented as min-heaps (PriorityQueue) ordered by earliest deadline. A garbage collector thread periodically purges expired messages. Logging is centralized and ordered using a single logger thread.

### Files / Main classes
- `Message` — message structure (id, content, deadline, POISON sentinel).
- `MessageQueue` — priority queue (min-heap) with synchronized `putAndLog`, `takeAndLog`, and `purgeExpired` methods.
- `Producer` — produces messages with configurable TTL or infinite retention.
- `Consumer` — consumes messages from the top of heap (earliest deadline first).
- `GarbageCollector` — periodically calls `purgeExpired()` on all queues.
- `EventLogger` — single-threaded FIFO logger that preserves event order.
- `MessageBroker` (main class) — program entry point; starts producers, consumers, GC and logger.

### Compile
From the directory that contains the `MessageBroker.java` (or the single source file), run:

```bash
javac MessageBroker.java
```

If your code is split into multiple `.java` files with the classes above, compile all of them together:

```bash
javac *.java
```

### Run
Run the program with:

```bash
java MessageBroker
```

### Configuration
Inside `MessageBroker.main` you can change constants to tune behavior:
- `BOUNDED_BUFFER` (boolean) — if true, queue capacity is limited; otherwise very large capacity.
- `RETENTION_POLICY` (boolean) — whether messages have TTL and GC runs; if false messages live "forever" (simulated via a large deadline).
- `QUEUE_CAPACITY` — maximum messages per queue when bounded.
- `NUM_PRODUCERS`, `NUM_CONSUMERS`, `MESSAGES_PER_PRODUCER` — number of threads/messages.
- `GC_INTERVAL_MS` — how often GC checks for expired messages.

### Expected behavior
- Producers create messages with a deadline (now + TTL) and put them into a per-topic priority heap.
- Consumers take the message with the earliest deadline. If the top message has already expired, either GC or the consumer removes it (and logs the purge) and the next message is checked.
- GC continuously purges expired messages from the head of each queue so consumers don't process expired items.
- EventLogger centralizes logging so the printed log order corresponds to the actual sequence of state changes.

---
