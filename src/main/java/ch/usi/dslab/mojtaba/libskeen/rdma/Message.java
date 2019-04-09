package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastMessage;

import java.nio.ByteBuffer;

public class Message implements RamcastMessage {

//    public static int SERIALIZED_SIZE = 24 + 4;
    public static int SERIALIZED_SIZE = 24;
//    public static int SERIALIZED_SIZE = 24 + 16;
//    public static int SERIALIZED_SIZE = 24 + 32;

    // STEP1 and STEP2
    private int msgType;
    private int clientId;
    private int msgId;
    private int destinationSize = 8;
    private int[] destinations;

    //STEP2 only
    private int nodeId;
    private int LC;

    private transient int ticket;

    Message() {

    }

    Message(int msgType, int clientId, int msgId) {
        this.msgType = msgType;
        this.clientId = clientId;
        this.msgId = msgId;
    }

    Message(int msgType, int clientId, int msgId, int destinationSize, int[] destinations) {
        this.msgType = msgType;
        this.clientId = clientId;
        this.msgId = msgId;
        this.destinationSize = destinationSize;
        this.destinations = destinations;
    }

    Message(int msgType, int clientId, int msgId, int destinationSize, int[] destinations, int nodeId, int LC) {
        this.msgType = msgType;
        this.clientId = clientId;
        this.msgId = msgId;
        this.destinationSize = destinationSize;
        this.destinations = destinations;
        this.nodeId = nodeId;
        this.LC = LC;
    }

    @Override
    public int write(ByteBuffer buffer) {
        buffer.putInt(msgType);
        buffer.putInt(clientId);
        buffer.putInt(msgId);
        buffer.putInt(destinationSize);
        if (destinations == null)
            destinations = new int[destinationSize];
        for (int i=0; i<destinationSize; i++)
            buffer.putInt(destinations[i]);
        buffer.putInt(nodeId);
        buffer.putInt(LC);

        return SERIALIZED_SIZE + 4*destinationSize;
    }

    @Override
    public void update(ByteBuffer buffer) {
        this.msgType = buffer.getInt();
        this.clientId = buffer.getInt();
        this.msgId = buffer.getInt();
        this.destinationSize = buffer.getInt();
        this.destinations = new int[destinationSize];
        for (int i=0; i<destinationSize; i++)
            this.destinations[i] = buffer.getInt();
        this.nodeId = buffer.getInt();
        this.LC = buffer.getInt();
    }

    @Override
    public void update(RamcastMessage buffer) {
        msgType = ((Message) buffer).getMsgType();
        clientId = ((Message) buffer).getClientId();
        msgId = ((Message) buffer).getMsgId();
        destinationSize = ((Message) buffer).getDestinationSize();
        destinations = ((Message) buffer).getDestinations();
        nodeId = ((Message) buffer).getNodeId();
        LC = ((Message) buffer).getLC();

    }

    @Override
    public void stamp(int ticket) {
        this.ticket = ticket;
    }

    @Override
    public int getTicket() {
        return ticket;
    }

    @Override
    public int size() {
        return SERIALIZED_SIZE + 4*destinationSize;
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

    int getNodeId() {
        return nodeId;
    }

    int getLC() {
        return LC;
    }

    @Override
    public String toString() {
        return clientId + ":" + msgId;
    }
}
