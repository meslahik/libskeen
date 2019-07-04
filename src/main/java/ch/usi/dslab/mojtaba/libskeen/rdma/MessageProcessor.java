package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.ramcast.ServerEventCallback;
import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class MessageProcessor implements ServerEventCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    ThroughputPassiveMonitor tpMonitor;
    LatencyPassiveMonitor latMonitor;

//    LinkedBlockingQueue<Pair<Message, RamcastServerEvent>> step1PendingEvents = new LinkedBlockingQueue();
//    LinkedBlockingQueue<Pair<Message, RamcastServerEvent>> step2PendingEvents = new LinkedBlockingQueue();

//    ConcurrentLinkedQueue<Pair<Message, RamcastServerEvent>> step1PendingEvents = new ConcurrentLinkedQueue();
//    ConcurrentLinkedQueue<Pair<Message, RamcastServerEvent>> step2PendingEvents = new ConcurrentLinkedQueue();

    class Pending {
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

    class ConsensusDeliverer implements Runnable {

        @Override
        public void run() {
            while (true) {
                SkeenMessage msg = replica.decide();
                deliverConsensus(msg);
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

    // TODO needs to be Atomic?
    int LC = 0;

    Server server;
    Replica replica;
    boolean isGathererEnabled;

    private Map<Pair<Integer, Integer>, ArrayList<Pending>> pendingMsgs = new ConcurrentHashMap<>();
    private TreeMap<Integer, Pending> ordered = new TreeMap<>();

    private Map<Pair<Integer, Integer>, RamcastServerEvent> waitingEvents = new ConcurrentHashMap<>();

    MessageProcessor(Server server, Replica replica,
                     boolean isGathererEnabled, String gathererHost, int gathererPort, String fileDirectory, int experimentDuration, int warmUpTime) {
        this.server = server;
        this.replica = replica;

        Thread consensusDeliverer = new Thread(new ConsensusDeliverer(), "ConsensusDeliverer-" + server.node.pid);
        consensusDeliverer.start();

        this.isGathererEnabled = isGathererEnabled;
        if (isGathererEnabled) {
            DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
            tpMonitor = new ThroughputPassiveMonitor(server.node.pid, "server", true);
            latMonitor = new LatencyPassiveMonitor(server.node.pid, "server", true);
        }
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
        logger.debug("replica {} message {}-{}:{} is put in ordered queue to be delivered", replica.pid, maxLC, p.clientId, p.msgId);

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
                server.addDeliveredMessage(deliverPair);
                logger.debug("replica {} atomic decide message {}-{}:{}", replica.pid, minOrderedLC, pending.clientId, pending.msgId);

                if (isGathererEnabled) {
                    tpMonitor.incrementCount();
                    latMonitor.logLatency(pending.sendTime, System.currentTimeMillis());
                }

                SkeenMessage reply = new SkeenMessage(3, pending.clientId, pending.msgId);
                RamcastServerEvent event = waitingEvents.get(deliverPair);
                if (event != null) {
                    event.setSendBuffer(reply.getBuffer());
                    try {
                        event.triggerResponse();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    logger.debug("replica {} reply sent to the client", replica.pid);
                    waitingEvents.remove(deliverPair);
                }
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

        // reply is needed to be sent back to the server (the leader) that sent this skeenMessage
        SkeenMessage reply = new SkeenMessage(3, clientId, messageId);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("ACK skeenMessage sent back to the server {}", nodeId);

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
                Node gLeader = g.nodeList.get(0);
                server.sendNonBlocking(newSkeenMessage, true, gLeader.pid);
                logger.debug("sent skeenMessage {} to server {}", newSkeenMessage, gLeader);
            }
            for (Group g : destinationGroups) {
                Node gLeader = g.nodeList.get(0);
                server.deliverReply(gLeader.pid);
                logger.debug("sent skeenMessage {} to server {}", newSkeenMessage, gLeader);
            }
        }
    }

    // only leader processes receive these messages. clients sendConsensusStep2Message to leaders; leaders also sendConsensusStep2Message to leaders
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
                processStep2Message(m, event);
                break;
            case 3:
                // ack message
                logger.debug("MessageProcessor: ACK received");

        }
//        event.setFree();
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
