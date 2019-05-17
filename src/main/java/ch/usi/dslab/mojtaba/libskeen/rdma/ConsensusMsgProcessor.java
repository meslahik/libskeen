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

    void send(ConsensusMessage message, int nodeId) {
        replica.senders.get(nodeId).send(message.getBuffer(), false);
    }

    // Acceptor
    void processTWOAMessage(ConsensusMessage wrapperMessage) {
        int msgInstanceNum = wrapperMessage.getInstanceId();
        SkeenMessage message = wrapperMessage.getSkeenMessage();

        ConsensusMessage newWrapperMessage = new ConsensusMessage(2, msgInstanceNum, message, replica.pid);
        for (Replica replica: Group.getGroup(replica.gid).replicaList)
            send(newWrapperMessage, replica.pid);
    }

    // Learner
    void processTWOBMessage(ConsensusMessage wrapperMessage) {
        logger.debug("process message {}", wrapperMessage);
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

        int type = m.getMsgType();
        switch (type) {
            case 1:
                processTWOAMessage(m);
                break;
            case 2:
                processTWOBMessage(m);
        }
        event.setFree();
    }
}
