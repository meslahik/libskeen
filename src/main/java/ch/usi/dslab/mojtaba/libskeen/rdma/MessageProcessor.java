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


        public Pending(int clientId, int msgId, int nodeId, int LC, int destinationSize, int[] destination, long sendTime) {
            this.clientId = clientId;
            this.msgId = msgId;
            this.destinationSize = destinationSize;
            this.destination = destination;
            this.sendTime = sendTime;
            this.nodeId = nodeId;
            this.LC = LC;

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

    Server server;
    boolean isGathererEnabled;

    private Map<Pair<Integer, Integer>, ArrayList<Pending>> pendingMsgs = new ConcurrentHashMap<>();
    private TreeMap<Integer, Pending> ordered = new TreeMap<>();

    private Map<Pair<Integer, Integer>, RamcastServerEvent> waitingEvents = new ConcurrentHashMap<>();

//    ExecutorService step1Executor = Executors.newFixedThreadPool(1);
//    ExecutorService step2Executor = Executors.newFixedThreadPool(1);
//
//    Runnable step1Processor = () -> {
//        while(true) {
//            try {
////                    Pair pair = step1PendingEvents.take();
//                Pair pair = null;
//                while(pair == null)
//                    pair = step1PendingEvents.poll();
//                Message msg = (Message) pair.getKey();
//                RamcastServerEvent event = (RamcastServerEvent) pair.getValue();
//                processStep1Message(msg, event);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    };
//
//    Runnable step2Processor = () -> {
//        while(true) {
//            try {
////                    Pair pair = step2PendingEvents.take();
//                Pair pair = null;
//                while(pair == null)
//                    pair = step2PendingEvents.poll();
//                Message msg = (Message) pair.getKey();
//                RamcastServerEvent event = (RamcastServerEvent) pair.getValue();
//                processStep2Message(msg, event);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    };

    MessageProcessor(Server server,
                     boolean isGathererEnabled, String gathererHost, int gathererPort, String fileDirectory, int experimentDuration, int warmUpTime) {
        this.server = server;
        init(isGathererEnabled, gathererHost, gathererPort, fileDirectory, experimentDuration, warmUpTime);
    }

    public void init(boolean isGathererEnabled, String gathererHost, int gathererPort, String fileDirectory, int experimentDuration, int warmUpTime) {
        this.isGathererEnabled = isGathererEnabled;
        if (isGathererEnabled) {
            DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
            tpMonitor = new ThroughputPassiveMonitor(server.node.pid, "server", true);
            latMonitor = new LatencyPassiveMonitor(server.node.pid, "server", true);
        }
    }

    private void processPendingMessages(Pair<Integer, Integer> pair) {
        ArrayList<Pending> arr = pendingMsgs.get(pair);
        Pending p = arr.get(0);
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
                logger.debug("atomic deliver message {}-{}:{}", minOrderedLC, pending.clientId, pending.msgId);

                if (isGathererEnabled) {
                    tpMonitor.incrementCount();
                    latMonitor.logLatency(pending.sendTime, System.currentTimeMillis());
                }

                Message reply = new Message(3, pending.clientId, pending.msgId);
                RamcastServerEvent event = waitingEvents.get(deliverPair);
                event.setSendBuffer(reply.getBuffer());
                try {
                    event.triggerResponse();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                logger.debug("reply sent to the client");
                waitingEvents.remove(deliverPair);
            } else {
                break;
            }
        }
    }

    private synchronized void processStep2Message(Message message, RamcastServerEvent event) {

        int clientId = message.getClientId();
        int messageId = message.getMsgId();
        int destinationSize = message.getDestinationSize();
        int[] destinations = message.getDestinations();
        long sendTime = message.getSendTime();
        int nodeId = message.getNodeId();
        int lc = message.getLC();
        logger.debug("received STEP2 message {} from {}", message, nodeId);

        // reply is needed to be sent back to the server that sent this message
        Message reply = new Message(3, clientId, messageId);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("ACK message sent back to the server {}", nodeId);

        Pending p = new Pending(clientId, messageId, nodeId, lc, destinationSize, destinations, sendTime);
        server.LC = Math.max(server.LC, p.LC);
        Pair<Integer, Integer> pair = new Pair<>(p.clientId, p.msgId);
        if (pendingMsgs.containsKey(pair))
            pendingMsgs.get(pair).add(p);
        else {
            ArrayList<Pending> arr = new ArrayList<>();
            arr.add(p);
            pendingMsgs.put(pair, arr);
        }
        logger.debug("added {} to pending messages", p);
        processPendingMessages(pair);
    }

    private void processStep1Message(Message message, RamcastServerEvent event) {
        int clientId = message.getClientId();
        int messageId = message.getMsgId();
        int destinationSize = message.getDestinationSize();
        int[] destinations = message.getDestinations();
        long sendTime = message.getSendTime();
        logger.debug("received STEP1 message {} from {}", message, clientId);

        Pair<Integer, Integer> pair = new Pair<>(clientId, messageId);
        waitingEvents.put(pair, event);

////        // reply is needed to be sent back to the server that sent this message
//        Message reply = new Message(3, clientId, messageId);
//        event.setSendBuffer(reply.getBuffer());
//        try {
//            event.triggerResponse();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        logger.debug("ACK message sent back to the client {}", clientId);

        Message newMessage = new Message(2, clientId, messageId, destinationSize, destinations, sendTime, server.node.pid, ++server.LC);

        List<Group> destinationGroups = new ArrayList<>();
        for (int id: destinations)
            destinationGroups.add(Group.getGroup(id));
        logger.debug("destination size for msgType 2: {}", destinationGroups.size());
        for (Group g: destinationGroups) {
//            server.send(newMessage, true, g.nodeList.get(0).pid);
            server.sendNonBlocking(newMessage, true, g.nodeList.get(0).pid);
            logger.debug("sent message {} to server {}", newMessage, g.nodeList.get(0));
        }
        logger.debug("Step1 message processed");
    }

    @Override
    public void call(RamcastServerEvent event) {
        ByteBuffer buffer = event.getReceiveBuffer();
        Message m = new Message();
        m.update(buffer);

        int type = m.getMsgType();
        switch (type) {
            case 1:
//                int clientId = m.getClientId();
//                int messageId = m.getMsgId();
//                messageConnectionMap.put(new Pair<>(clientId, messageId), connection);
                processStep1Message(m, event);
                break;
            case 2:
                processStep2Message(m, event);
            case 3:
                // ack message for receiving
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
