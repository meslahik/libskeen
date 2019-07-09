package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import ch.usi.dslab.lel.ramcast.ServerEventCallback;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ConsensusMsgProcessor implements ServerEventCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConsensusMsgProcessor.class);

    Replica replica;

    ConsensusMsgProcessor(Replica replica) {
        this.replica = replica;
    }

    void processConsensusStep1Message(RamcastServerEvent event, ConsensusMessage wrapperMessage) {
        int msgInstanceNum = wrapperMessage.getInstanceId();
        SkeenMessage message = wrapperMessage.getSkeenMessage();

        // reply is needed to be sent back to the server (leader) that sent this consensusMessage (server port)
//        SkeenMessage reply = new SkeenMessage(3, 0, 0);
        ConsensusMessage reply = new ConsensusMessage(2, msgInstanceNum, message, replica.pid);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("replica {} reply sent back for consensus step1 message", replica.pid);
    }

    void processConsensusStep3Message(RamcastServerEvent event, ConsensusMessage message) {
        ConsensusMessage reply = new ConsensusMessage(4);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }

        int lastDeliveredInstance = message.getInstanceId();
        SkeenMessage deliverMessage = message.getSkeenMessage();

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
        ConsensusMessage m = new ConsensusMessage();
        m.update(buffer);

        int type = m.getMsgType();
        switch (type) {
            case 1:
                logger.debug("replica {} received consensus step1 message {} ", replica.pid, m);
                processConsensusStep1Message(event, m);
                break;
//            case 2:
//                processConsensusStep2Message(m);
//                break;
            case 3:
                processConsensusStep3Message(event, m);
                break;
        }
    }
}
