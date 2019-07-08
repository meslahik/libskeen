package ch.usi.dslab.mojtaba.libskeen.rdma;

public class Pending {
    int clientId;
    int msgId;
    int destinationSize;
    int[] destination;
    long sendTime;
    int nodeId;
    int LC;
    SkeenMessage message;

    public Pending(int clientId, int msgId, int nodeId, int LC, int destinationSize, int[] destination, SkeenMessage skeenMessage) {
        this.clientId = clientId;
        this.msgId = msgId;
        this.destinationSize = destinationSize;
        this.destination = destination;
        this.nodeId = nodeId;
        this.LC = LC;
        this.message = skeenMessage;
    }

    @Override
    public String toString() {
        String destsStr = "[";
        for (int i=0; i<destinationSize; i++)
            destsStr = destsStr + destination[i] + ",";
        destsStr = destsStr + "]";
        return "[pending: clientId=" + clientId + ", MsgId=" + msgId + ", nodeId=" + nodeId + ", LC=" + LC +
                ", destinationSize=" + destinationSize + ", destinations=" + destsStr + "]";
    }
}
