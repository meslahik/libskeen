package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import ch.usi.dslab.lel.ramcast.ServerEventCallback;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ConsensusMsgProcessor implements ServerEventCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConsensusMsgProcessor.class);

    Replica replica;

    // process consensus step1 message
    ConsensusMsgProcessor(Replica replica) {
        this.replica = replica;
    }

    void processConsensusStep1Messaege(RamcastServerEvent event, ConsensusMessage consensusMessage) {
        int msgInstanceNum = consensusMessage.getInstanceId();
        SkeenMessage message = consensusMessage.getSkeenMessage();

        // reply is needed to be sent back to the coordinator
        ConsensusMessage reply = new ConsensusMessage(2, msgInstanceNum, message, replica.pid);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("replica {} reply sent back for consensus step1 message", replica.pid);
    }

    void processConsensusStep3Message(RamcastServerEvent event, ConsensusMessage consensusMessage) {
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

    @Override
    public void call(RamcastServerEvent event) {
        ByteBuffer buffer = event.getReceiveBuffer();
        ConsensusMessage consensusMessage = new ConsensusMessage();
        consensusMessage.update(buffer);

        processConsensusStep1Messaege(event, consensusMessage);

//        int type = consensusMessage.getMsgType();
//        switch (type) {
//            case 1:
//                processConsensusStep1Messaege(event, consensusMessage);
//                break;
//            case 3:
//                processConsensusStep3Message(event, consensusMessage);
//                break;
//        }
    }
}
