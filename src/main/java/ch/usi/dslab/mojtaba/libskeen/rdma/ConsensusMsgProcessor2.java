package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import ch.usi.dslab.lel.ramcast.ServerEventCallback;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ConsensusMsgProcessor2 implements ServerEventCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConsensusMsgProcessor2.class);

    Replica replica;

    // process consensus step3 message
    ConsensusMsgProcessor2(Replica replica) {
        this.replica = replica;
    }

    @Override
    public void call(RamcastServerEvent event) {
        ByteBuffer buffer = event.getReceiveBuffer();
        ConsensusMessage consensusMessage = new ConsensusMessage();
        consensusMessage.update(buffer);

        // reply is needed to be sent back to the coordinator
        ConsensusMessage reply = new ConsensusMessage(4);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }

        int lastDeliveredInstance = consensusMessage.getInstanceId();
        SkeenMessage deliverMessage = consensusMessage.getSkeenMessage();

        logger.debug("replica {} decide message {} lastDeliverdInstance {}", replica.pid, deliverMessage, lastDeliveredInstance);
        try {
//            replica.decidedQueue.put(deliverMessage);
            replica.decideCallback.call(deliverMessage);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
