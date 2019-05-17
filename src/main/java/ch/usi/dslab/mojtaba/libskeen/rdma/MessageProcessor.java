package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import ch.usi.dslab.lel.ramcast.ServerEventCallback;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MessageProcessor implements ServerEventCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    class Pending {
        int clientId;
        int msgId;
        int destinationSize;
        int[] destination;
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

    class ConsensusDeliverer implements Runnable {

        @Override
        public void run() {
            while (true) {
                SkeenMessage msg = replica.decide();
                deliverConsensus(msg);
            }
        }
    }

    // TODO needs to be Atomic?
    int LC = 0;

    Server server;
    Replica replica;

    private Map<Pair<Integer, Integer>, ArrayList<Pending>> pendingMsgs = new ConcurrentHashMap<>();
    private Map<Pair<Integer, Integer>, RamcastServerEvent> waitingEvents = new ConcurrentHashMap<>();
    private TreeMap<Integer, Pending> ordered = new TreeMap<>();

    MessageProcessor(Server server, Replica replica) {
        this.server = server;
        this.replica = replica;

        Thread consensusDeliverer = new Thread(new ConsensusDeliverer(), "ConsensusDeliverer-" + server.node.pid);
        consensusDeliverer.start();
    }

    private void processMaxLCMessages(SkeenMessage skeenMessage) {
        int clientId = skeenMessage.getClientId();
        int messageId = skeenMessage.getMsgId();
        int destinationSize = skeenMessage.getDestinationSize();
        int[] destinations = skeenMessage.getDestinations();
        int nodeId = skeenMessage.getNodeId();
        int maxLC = skeenMessage.getLC();

        Pending p = new Pending(clientId, messageId, nodeId, maxLC, destinationSize, destinations, skeenMessage);

        ordered.put(maxLC, p);
        logger.debug("message {}-{}:{} is put in ordered queue to be delivered", maxLC, p.clientId, p.msgId);

        while (ordered.size() > 0) {
            Map.Entry<Integer, Pending> minOrdered = ordered.firstEntry();
            Integer minOrderedLC = minOrdered.getKey();
            Pending pending = minOrdered.getValue();

            boolean flag = true;
            Collection<ArrayList<Pending>> arrs = pendingMsgs.values();
            Iterator<ArrayList<Pending>> it = arrs.iterator();
            while (it.hasNext()) {
                ArrayList<Pending> arr2 = it.next();
                for (int i = 0; i < arr2.size(); i++) {
                    Pending pending2 = arr2.get(i);
                    if (pending2.LC < minOrderedLC) {
                        flag = false;
                        break;
                    }
                }
            }

            if (flag) {
                ordered.pollFirstEntry();
                Pair<Integer, Integer> deliverPair = new Pair<>(pending.clientId, pending.msgId);
                server.atomicDeliver.add(deliverPair);
                logger.debug("atomic decide message {}-{}:{}", minOrderedLC, pending.clientId, pending.msgId);

//                SkeenMessage reply = new SkeenMessage(3, pending.clientId, pending.msgId);
//                RamcastServerEvent event = waitingEvents.get(deliverPair);
//                if (event != null) {
//                    event.setSendBuffer(reply.getBuffer());
//                    try {
//                        event.triggerResponse();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                    logger.debug("reply sent to the client");
//                    waitingEvents.remove(deliverPair);
//                }
            } else {
                break;
            }
        }
    }

    private synchronized void processStep2Message(SkeenMessage skeenMessage, RamcastServerEvent event) {

        int clientId = skeenMessage.getClientId();
        int messageId = skeenMessage.getMsgId();
        int destinationSize = skeenMessage.getDestinationSize();
        int[] destinations = skeenMessage.getDestinations();
        int nodeId = skeenMessage.getNodeId();
        int lc = skeenMessage.getLC();
        logger.debug("received STEP2 skeenMessage {} from {}", skeenMessage, nodeId);

//        // reply is needed to be sent back to the server that sent this skeenMessage
//        SkeenMessage reply = new SkeenMessage(3, clientId, messageId);
//        event.setSendBuffer(reply.getBuffer());
//        try {
//            event.triggerResponse();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        logger.debug("ACK skeenMessage sent back to the server {}", nodeId);

        Pending p = new Pending(clientId, messageId, nodeId, lc, destinationSize, destinations, skeenMessage);
        LC = Math.max(LC, p.LC);
        Pair<Integer, Integer> pair = new Pair<>(p.clientId, p.msgId);
        ArrayList<Pending> arr;
        if (pendingMsgs.containsKey(pair)) {
            arr = pendingMsgs.get(pair);
            arr.add(p);
        }
        else {
            arr = new ArrayList<>();
            arr.add(p);
            pendingMsgs.put(pair, arr);
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
        pendingMsgs.remove(pair);
        logger.debug("have enough votes for pair {}. lc {} is chosen for the message {}:{}",
                pair, maxLC, p.clientId, p.msgId);

        SkeenMessage newSkeenMessage = new SkeenMessage(2, clientId, messageId, destinationSize, destinations,
                server.node.pid, maxLC);
        replica.propose(newSkeenMessage);
    }

    private void processStep1Message(SkeenMessage skeenMessage) {
        int clientId = skeenMessage.getClientId();
        int messageId = skeenMessage.getMsgId();
        int destinationSize = skeenMessage.getDestinationSize();
        int[] destinations = skeenMessage.getDestinations();

        LC++;

        if (server.node.isLeader) {
            SkeenMessage newSkeenMessage = new SkeenMessage(2, clientId, messageId, destinationSize, destinations,
                    server.node.pid, LC);

            List<Group> destinationGroups = new ArrayList<>();
            for (int id : destinations)
                destinationGroups.add(Group.getGroup(id));
            for (Group g : destinationGroups) {
                server.send(newSkeenMessage, false, g.nodeList.get(0).pid);
                logger.debug("sent skeenMessage {} to server {}", newSkeenMessage, g.nodeList.get(0));
            }
        }
    }

    void deliverConsensus(SkeenMessage m) {
        int type = m.getMsgType();
        switch (type) {
            case 1:
                logger.debug("received STEP1 message {}", m);
                processStep1Message(m);
                break;
            case 2:
                logger.debug("received STEP2 message {}", m);
                processMaxLCMessages(m);
        }
    }

    // only leader processes receive these messages. clients send to leaders; leaders also send to leaders
    @Override
    public void call(RamcastServerEvent event) {
        ByteBuffer buffer = event.getReceiveBuffer();
        SkeenMessage m = new SkeenMessage();
        m.update(buffer);

        int type = m.getMsgType();
        switch (type) {
            case 1:
//                logger.debug("received STEP1 message {}", m);
                int clientId = m.getClientId();
                int messageId = m.getMsgId();

                Pair<Integer, Integer> pair = new Pair<>(clientId, messageId);
                waitingEvents.put(pair, event);

                replica.propose(m);
                break;
            case 2:
//                logger.debug("received STEP2 message {}", m);
                processStep2Message(m, event);
        }
        event.setFree();
    }
}
