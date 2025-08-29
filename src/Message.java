public class Message implements Comparable<Message> {
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