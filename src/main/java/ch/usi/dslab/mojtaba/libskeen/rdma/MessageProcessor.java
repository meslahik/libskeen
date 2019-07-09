package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.ServerEventCallback;
import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class MessageProcessor implements ServerEventCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

//    LinkedBlockingQueue<Pair<Message, RamcastServerEvent>> step1PendingEvents = new LinkedBlockingQueue();
//    LinkedBlockingQueue<Pair<Message, RamcastServerEvent>> step2PendingEvents = new LinkedBlockingQueue();

//    ConcurrentLinkedQueue<Pair<Message, RamcastServerEvent>> step1PendingEvents = new ConcurrentLinkedQueue();
//    ConcurrentLinkedQueue<Pair<Message, RamcastServerEvent>> step2PendingEvents = new ConcurrentLinkedQueue();

//    class ConsensusDeliverer implements Runnable {
//
//        @Override
//        public void run() {
//            while (true) {
//                SkeenMessage msg = replica.decide();
//                deliverConsensus(msg);
//            }
//        }
//    }

//    void deliverConsensus(SkeenMessage m) {
//        int type = m.getMsgType();
//        switch (type) {
//            case 1:
//                logger.debug("replica {} delivered Skeen STEP1 message {}",replica.pid, m);
//                processStep1Message(m);
//                break;
//            case 2:
//                logger.debug("replica {} delivered Skeen MaxLC message {}", replica.pid, m);
//                processMaxLCMessages(m);
//        }
//    }


    Replica replica;

    MessageProcessor(Replica replica) {
        this.replica = replica;
//        Thread consensusDeliverer = new Thread(new ConsensusDeliverer(), "ConsensusDeliverer-" + server.node.pid);
//        consensusDeliverer.start();
    }

    void processStep2Message(SkeenMessage skeenMessage, RamcastServerEvent event) {
        int clientId = skeenMessage.getClientId();
        int messageId = skeenMessage.getMsgId();
        int destinationSize = skeenMessage.getDestinationSize();
        int[] destinations = skeenMessage.getDestinations();
        int nodeId = skeenMessage.getNodeId();
        int lc = skeenMessage.getLC();

        // reply is needed to be sent back to the server (the leader) that sent this skeenMessage
        SkeenMessage reply = new SkeenMessage(3, clientId, messageId);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("ACK skeenMessage sent back to the server {}", nodeId);



        logger.debug("replica {} received Skeen STEP2 message {} from {}", replica.pid, skeenMessage, nodeId);
        Pending p = new Pending(clientId, messageId, nodeId, lc, destinationSize, destinations, skeenMessage);
        replica.LC = Math.max(replica.LC, p.LC);
        Pair<Integer, Integer> pair = new Pair<>(p.clientId, p.msgId);
        ArrayList<Pending> arr;
        if (replica.pendingMsgs.containsKey(pair)) {
            arr = replica.pendingMsgs.get(pair);
            arr.add(p);
        }
        else {
            arr = new ArrayList<>();
            arr.add(p);
            replica.pendingMsgs.put(pair, arr);
        }
        logger.debug("added {} to pending messages", p);

        // process pending messages
        if (arr.size() < p.destinationSize) {
            logger.debug("should wait for server votes for pair {}. received {} out of {} votes",
                    pair, arr.size(), p.destinationSize);
            return;
        }

        int maxLC = -1;
        for (int i = 0; i < arr.size(); i++) {
            int LC = arr.get(i).LC;
            maxLC = maxLC < LC ? LC : maxLC;
        }
        replica.pendingMsgs.remove(pair);
        logger.debug("have enough votes for pair {}. lc {} is chosen for the message {}:{}",
                pair, maxLC, p.clientId, p.msgId);

        SkeenMessage newSkeenMessage = new SkeenMessage(2, clientId, messageId, destinationSize, destinations,
                replica.pid, maxLC);
        replica.propose(newSkeenMessage);
    }

    void processStep1Message(SkeenMessage m, RamcastServerEvent event) {
        logger.debug("replica {} received Skeen STEP1 message {}", replica.pid, m);
        int clientId = m.getClientId();
        int messageId = m.getMsgId();

//                SkeenMessage reply = new SkeenMessage(3, clientId, messageId);
//                event.setSendBuffer(reply.getBuffer());
//                try {
//                    event.triggerResponse();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }

        Pair<Integer, Integer> pair = new Pair<>(clientId, messageId);
        replica.waitingEvents.put(pair, event);
        replica.propose(m);
    }

    // only leader processes receive these messages. clients skeen Step1 messages to leaders; leaders also skeen Step2 message to leaders
    @Override
    public void call(RamcastServerEvent event) {
        ByteBuffer buffer = event.getReceiveBuffer();
        SkeenMessage m = new SkeenMessage();
        m.update(buffer);

        int type = m.getMsgType();
        switch (type) {
            case 1:
                processStep1Message(m, event);
                break;
            case 2:
                processStep2Message(m, event);
                break;
        }
    }

//    // threads for processing different message types
//    @Override
//    public void call(RamcastServerEvent event) {
//        ByteBuffer buffer = event.getReceiveBuffer();
//        Message m = new Message();
//        m.update(buffer);
//
//        int type = m.getMsgType();
//        switch (type) {
//            case 1:
////                int clientId = m.getClientId();
////                int messageId = m.getMsgId();
////                messageConnectionMap.put(new Pair<>(clientId, messageId), connection);
////                processStep1Message(m, event);
//                try {
//                    logger.debug("routing step1 message");
//                    step1PendingEvents.put(new Pair<>(m, event));
////                    step1PendingEvents.add(new Pair<>(m, event));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                break;
//            case 2:
//                try {
//                    logger.debug("routing step2 message");
//                    step2PendingEvents.put(new Pair<>(m, event));
////                    step2PendingEvents.add(new Pair<>(m, event));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            case 3:
//                // ack message for receiving
//        }
//    }

}
