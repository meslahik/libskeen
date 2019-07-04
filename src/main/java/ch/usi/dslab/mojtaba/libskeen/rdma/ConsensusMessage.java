package ch.usi.dslab.mojtaba.libskeen.rdma;

import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ConsensusMessage {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConsensusMessage.class);

    static private int SIZE = 12 + 64;
//    static private int SIZE = 100;

    ByteBuffer buffer = ByteBuffer.allocateDirect(SIZE);

    // propose
    private int msgType;
    private int instanceId;
    private SkeenMessage skeenMessage = new SkeenMessage();

    // TWOA
    private int replicaId;

    private transient int ticket;

    ConsensusMessage() {

    }

    ConsensusMessage(int msgType) {
        this.msgType = msgType;

        write(buffer);
    }

    ConsensusMessage(int msgType, int instanceId, SkeenMessage skeenMessage) {
        this.msgType = msgType;
        this.instanceId = instanceId;
        this.replicaId = -1;
        this.skeenMessage = skeenMessage;

        write(buffer);
    }

    ConsensusMessage(int msgType, int instanceId, SkeenMessage skeenMessage, int replicaId) {
        this.msgType = msgType;
        this.instanceId = instanceId;
        this.replicaId = replicaId;
        this.skeenMessage = skeenMessage;

        write(buffer);
    }

    public int write(ByteBuffer buffer) {
        buffer.putInt(msgType);
        buffer.putInt(instanceId);
        buffer.putInt(replicaId);
        skeenMessage.write(buffer);

        return buffer.capacity();
    }

    public void update(ByteBuffer buffer)  {
        logger.debug("ConsensusMessage: update buffer size: {}", buffer);
        msgType = buffer.getInt();
        instanceId = buffer.getInt();
        replicaId = buffer.getInt();
        skeenMessage.update(buffer);

        write(this.buffer);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void stamp(int ticket) {
        this.ticket = ticket;
    }

    public int getTicket() {
        return ticket;
    }

    static public int size() {
        return SIZE;
    }

    int getMsgType() {
        return msgType;
    }

    int getInstanceId() {return instanceId; }

    SkeenMessage getSkeenMessage() { return skeenMessage; }

    int getReplicaId() {return replicaId; }

    @Override
    public String toString() {
        return "[ConsensusMsg, instId=" + instanceId + "]";
    }
}
