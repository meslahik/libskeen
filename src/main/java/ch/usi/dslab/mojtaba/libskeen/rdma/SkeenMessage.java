package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.messages.RamcastMessage;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class SkeenMessage {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SkeenMessage.class);

    private int SIZE = 24 + 32;
//    private int SIZE = 100;

    ByteBuffer buffer = ByteBuffer.allocateDirect(SIZE);

    // STEP1 and STEP2
    private int msgType;
    private int clientId;
    private int msgId;
    private int destinationSize;
    private int[] destinations;

    //STEP2 only
    private int nodeId;
    private int LC;

    private transient int ticket;

    SkeenMessage() {

    }


    SkeenMessage(int msgType, int clientId, int msgId) {
        this.msgType = msgType;
        this.clientId = clientId;
        this.msgId = msgId;

        write(buffer);
    }

    SkeenMessage(int msgType, int clientId, int msgId, int destinationSize, int[] destinations) {
        this.msgType = msgType;
        this.clientId = clientId;
        this.msgId = msgId;
        this.destinationSize = destinationSize;
        this.destinations = destinations;

        write(buffer);
    }

    SkeenMessage(int msgType, int clientId, int msgId, int destinationSize, int[] destinations, int nodeId, int LC) {
        this.msgType = msgType;
        this.clientId = clientId;
        this.msgId = msgId;
        this.destinationSize = destinationSize;
        this.destinations = destinations;
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
        buffer.putInt(nodeId);
        buffer.putInt(LC);

        return buffer.capacity();
    }

    public void update(ByteBuffer buffer) {
        this.msgType = buffer.getInt();
        logger.debug("SkeenMessage: msgType: {}", msgType);
        this.clientId = buffer.getInt();
        logger.debug("SkeenMessage: cliebtId: {}", clientId);
        this.msgId = buffer.getInt();
        logger.debug("SkeenMessage: msgId: {}", msgId);
        this.destinationSize = buffer.getInt();
        logger.debug("SkeenMessage: destinationSize: {}", destinationSize);
        this.destinations = new int[destinationSize];
        for (int i=0; i<destinationSize; i++) {
            logger.debug("SkeenMessage: update buffer size: {}", buffer);
            this.destinations[i] = buffer.getInt();
        }
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

    public int size() {
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

    int getNodeId() {
        return nodeId;
    }

    int getLC() {
        return LC;
    }

    @Override
    public String toString() {
        return "[SkeenMsg, msgType=" + msgType + " " + clientId + ":" + msgId + "]";
    }
}
