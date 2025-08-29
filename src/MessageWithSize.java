public class MessageWithSize {
    public final Message message;
    public final int queueSize;
    public MessageWithSize(Message message, int queueSize) {
        this.message = message;
        this.queueSize = queueSize;
    }
}