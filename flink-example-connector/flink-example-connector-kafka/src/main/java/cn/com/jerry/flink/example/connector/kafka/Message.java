package cn.com.jerry.flink.example.connector.kafka;

/**
 * @author GangW
 */
public class Message {
    private int partition;
    private long offset;
    private long time;
    private long count = 1;

    public Message() {}

    @Override
    public String toString() {
        return partition + "\t" + offset + "\t" + time;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
