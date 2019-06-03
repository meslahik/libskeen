package ch.usi.dslab.mojtaba.libskeen.rdma;

import java.nio.ByteBuffer;

public class Message { // TODO: change to buffer.capacity()

    public static int SIZE = 24 + 8 + 32;

    // STEP1 and STEP2
    private int msgType;
    private int clientId;
    private int msgId;
    private int destinationSize;
    private int[] destinations;
    private long sendTime;

    //STEP2 only
    private int nodeId;
    private int LC;

    private transient int ticket;

    ByteBuffer buffer = ByteBuffer.allocateDirect(SIZE);

    Message() {

    }

    Message(int msgType, int clientId, int msgId) {
        this.msgType = msgType;
        this.clientId = clientId;
        this.msgId = msgId;

        write(buffer);
    }

    Message(int msgType, int clientId, int msgId, int destinationSize, int[] destinations, long sendTime) {
        this.msgType = msgType;
        this.clientId = clientId;
        this.msgId = msgId;
        this.destinationSize = destinationSize;
        this.destinations = destinations;
        this.sendTime = sendTime;

        write(buffer);
    }

    Message(int msgType, int clientId, int msgId, int destinationSize, int[] destinations, long sendTime, int nodeId, int LC) {
        this.msgType = msgType;
        this.clientId = clientId;
        this.msgId = msgId;
        this.destinationSize = destinationSize;
        this.destinations = destinations;
        this.sendTime = sendTime;
        this.nodeId = nodeId;
        this.LC = LC;

        write(buffer);
    }

    public int write(ByteBuffer buffer) {
        buffer.putInt(msgType);
        buffer.putInt(clientId);
        buffer.putInt(msgId);
        buffer.putInt(destinationSize);
        if (destinations == null)
            destinations = new int[destinationSize];
        for (int i=0; i<destinationSize; i++)
            buffer.putInt(destinations[i]);
        buffer.putLong(sendTime);
        buffer.putInt(nodeId);
        buffer.putInt(LC);

        return buffer.capacity();
    }

    public void update(ByteBuffer buffer) {
        this.msgType = buffer.getInt();
        this.clientId = buffer.getInt();
        this.msgId = buffer.getInt();
        this.destinationSize = buffer.getInt();
        this.destinations = new int[destinationSize];
        for (int i=0; i<destinationSize; i++)
            this.destinations[i] = buffer.getInt();
        this.sendTime = buffer.getLong();
        this.nodeId = buffer.getInt();
        this.LC = buffer.getInt();
    }

    ByteBuffer getBuffer() {
        return buffer;
    }

    public void stamp(int ticket) {
        this.ticket = ticket;
    }

    public int getTicket() {
        return ticket;
    }

    public static int size() {
        return SIZE;
    }

    int getMsgType() {
        return msgType;
    }

    int getClientId() {
        return clientId;
    }

    int getMsgId() {
        return msgId;
    }

    int getDestinationSize() {
        return destinationSize;
    }

    int[] getDestinations() {
        return destinations;
    }

    long getSendTime() {
        return sendTime;
    }

    int getNodeId() {
        return nodeId;
    }

    int getLC() {
        return LC;
    }

    @Override
    public String toString() {
        return "[msgType=" + msgType + ", clientId=" + clientId + ", msgId=" + msgId + "]";

    }
}
