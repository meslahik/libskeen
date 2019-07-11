package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import ch.usi.dslab.lel.ramcast.ServerEventCallback;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class SkeenMessageProcessor2 implements ServerEventCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SkeenMessageProcessor2.class);

    Replica replica;

    SkeenMessageProcessor2(Replica replica) {
        this.replica = replica;
    }

    // only leader processes receive these messages. leaders send skeen Step2 message to leaders
    @Override
    public void call(RamcastServerEvent event) throws IOException {
//        ByteBuffer buffer = event.getReceiveBuffer();
//        SkeenMessage skeenMessage = new SkeenMessage();
//        skeenMessage.update(buffer);
//
//        int clientId = skeenMessage.getClientId();
//        int messageId = skeenMessage.getMsgId();
//        int destinationSize = skeenMessage.getDestinationSize();
//        int[] destinations = skeenMessage.getDestinations();
//        int nodeId = skeenMessage.getNodeId();
//        int lc = skeenMessage.getLC();
//
//        // reply is needed to be sent back to the server (the leader) that sent this skeenMessage
//        SkeenMessage reply = new SkeenMessage(3, clientId, messageId);
//        event.setSendBuffer(reply.getBuffer());
//        try {
//            event.triggerResponse();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        logger.debug("ACK skeenMessage sent back to the server {}", nodeId);
//
//
//
//        logger.debug("replica {} received Skeen STEP2 message {} from {}", replica.pid, skeenMessage, nodeId);
//        Pending p = new Pending(clientId, messageId, nodeId, lc, destinationSize, destinations, skeenMessage);
//        replica.LC.set(Math.max(replica.LC.get(), p.LC));
//        Pair<Integer, Integer> pair = new Pair<>(p.clientId, p.msgId);
//        ArrayList<Pending> arr;
//        if (replica.pendingMsgs.containsKey(pair)) {
//            arr = replica.pendingMsgs.get(pair);
//            arr.add(p);
//        }
//        else {
//            arr = new ArrayList<>();
//            arr.add(p);
//            replica.pendingMsgs.put(pair, arr);
//        }
//        logger.debug("added {} to pending messages", p);
//
//        // process pending messages
//        if (arr.size() < p.destinationSize) {
//            logger.debug("should wait for server votes for pair {}. received {} out of {} votes",
//                    pair, arr.size(), p.destinationSize);
//            return;
//        }
//
//        int maxLC = -1;
//        for (int i = 0; i < arr.size(); i++) {
//            int LC = arr.get(i).LC;
//            maxLC = maxLC < LC ? LC : maxLC;
//        }
////        arr.stream().map(x -> x.LC).max(Integer::compareTo).get();
//        replica.pendingMsgs.remove(pair);
//        logger.debug("have enough votes for pair {}. lc {} is chosen for the message {}:{}",
//                pair, maxLC, p.clientId, p.msgId);
//
//        SkeenMessage newSkeenMessage = new SkeenMessage(2, clientId, messageId, destinationSize, destinations,
//                replica.pid, maxLC);
//        replica.propose(newSkeenMessage);
//    }
    }
}
