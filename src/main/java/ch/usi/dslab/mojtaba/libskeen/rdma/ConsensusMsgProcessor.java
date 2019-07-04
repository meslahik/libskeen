package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import ch.usi.dslab.lel.ramcast.ServerEventCallback;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsensusMsgProcessor implements ServerEventCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConsensusMsgProcessor.class);

    Replica replica;

    int lastDeliveredInstance = 0;

    ConsensusMsgProcessor(Replica replica) {
        this.replica = replica;
    }

    BlockingQueue<SkeenMessage> decidedQueue = new LinkedBlockingQueue<>();

    // Learner
    Map<Integer, ArrayList<Integer>> pendingMessages = new HashMap<>();
    TreeMap<Integer, SkeenMessage> pendingDeliverMessages = new TreeMap<>();

    // Acceptor
    void processConsensusStep1Message(RamcastServerEvent event, ConsensusMessage wrapperMessage) {
        int msgInstanceNum = wrapperMessage.getInstanceId();
        SkeenMessage message = wrapperMessage.getSkeenMessage();

        // reply is needed to be sent back to the server (leader) that sent this consensusMessage (server port)
        SkeenMessage reply = new SkeenMessage(3, 0, 0);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("ACK sent back for consensus step1 message");

        ConsensusMessage newWrapperMessage = new ConsensusMessage(2, msgInstanceNum, message, replica.pid);
        replica.accept(newWrapperMessage);
    }

    // Learner
    void processConsensusStep2Message(RamcastServerEvent event, ConsensusMessage wrapperMessage) {
        logger.debug("process message {}", wrapperMessage);

        // reply is needed to be sent back to the replica that sent this consensusMessage (replica port)
        ConsensusMessage reply = new ConsensusMessage(3);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("ACK sent back for consensus step2 message");

        int msgInstanceNum = wrapperMessage.getInstanceId();
        if (lastDeliveredInstance >= msgInstanceNum) {
            logger.debug("ignore message {}", wrapperMessage);
            return;
        }

        SkeenMessage message = wrapperMessage.getSkeenMessage();
        int replicaId = wrapperMessage.getReplicaId();

        if (pendingMessages.containsKey(msgInstanceNum))
            pendingMessages.get(msgInstanceNum).add(replicaId);
        else {
            ArrayList<Integer> array = new ArrayList<>();
            array.add(replicaId);
            pendingMessages.put(msgInstanceNum, array);
        }

        ArrayList<Integer> arrayList = pendingMessages.get(msgInstanceNum);
        if (arrayList.size() > Group.getGroup(replica.gid).replicaList.size() / 2) {
            logger.debug("put into pending message for decision delivery. received {} votes for decision instance {}.",
                    arrayList.size(), msgInstanceNum);
            pendingDeliverMessages.put(msgInstanceNum, message);
        }

        while(pendingDeliverMessages.size() != 0) {
            if (pendingDeliverMessages.firstEntry().getKey() == lastDeliveredInstance + 1) {
                SkeenMessage deliverMessage = pendingDeliverMessages.pollFirstEntry().getValue();
                lastDeliveredInstance++;
                try {
                    logger.debug("replica {} decide message {}; lastDeliverdInstance {}", replica.pid, deliverMessage, lastDeliveredInstance);
                    decidedQueue.put(deliverMessage);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else break;
        }
    }

    @Override
    public void call(RamcastServerEvent event) {
        ByteBuffer buffer = event.getReceiveBuffer();
        ConsensusMessage m = new ConsensusMessage();
        m.update(buffer);

        logger.debug("received replica message {} ", m);
        int type = m.getMsgType();
        switch (type) {
            case 1:
                processConsensusStep1Message(event, m);
                break;
            case 2:
                processConsensusStep2Message(event, m);
                break;
            case 3:
                //ack message for consensus step2 message
                logger.debug("ConsensusMessageProcessor: ACK received");
        }
//        event.setFree();
    }
}
