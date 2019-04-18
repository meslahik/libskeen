package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.OnReceiveCallback;
import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MessageProcessor implements OnReceiveCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    class Pending {
        int clientId;
        int msgId;
        int destinationSize;
        int[] destination;
        int nodeId;
        int LC;


        public Pending(int clientId, int msgId, int nodeId, int LC, int destinationSize, int[] destination) {
            this.clientId = clientId;
            this.msgId = msgId;
            this.destinationSize = destinationSize;
            this.destination = destination;
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

    private Map<Pair<Integer, Integer>, ArrayList<Pending>> pendingMsgs = new ConcurrentHashMap<>();
    private Map<Pair<Integer, Integer>, RamcastServerEvent> waitingEvents = new ConcurrentHashMap<>();
    private TreeMap<Integer, Pending> ordered = new TreeMap<>();
//    static Map<Pair<Integer, Integer>, RamcastSender> messageConnectionMap = new HashMap<>();

    MessageProcessor(Server server) {
        this.server = server;
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
                logger.debug("atomic decide message {}-{}:{}", minOrderedLC, pending.clientId, pending.msgId);

                Message reply = new Message(3, pending.clientId, pending.msgId);
                RamcastServerEvent event = waitingEvents.get(deliverPair);
                event.setResponse(reply);
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
        int nodeId = message.getNodeId();
        int lc = message.getLC();
        logger.debug("received STEP2 message {} from {}", message, nodeId);

        // reply is needed to be sent back to the server that sent this message
        Message reply = new Message(3, clientId, messageId);
        event.setResponse(reply);
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("ACK message sent back to the server {}", nodeId);

        Pending p = new Pending(clientId, messageId, nodeId, lc, destinationSize, destinations);
        server.LC = Math.max(server.LC, p.LC);
        Pair<Integer, Integer> pair = new Pair<Integer, Integer>(p.clientId, p.msgId);
        if (pendingMsgs.containsKey(pair))
            pendingMsgs.get(pair).add(p);
        else {
            ArrayList<Pending> arr = new ArrayList<Pending>();
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

        Pair<Integer, Integer> pair = new Pair<>(clientId, messageId);
        waitingEvents.put(pair, event);

        Message newMessage = new Message(2, clientId, messageId, destinationSize, destinations, server.node.pid, ++server.LC);

        List<Group> destinationGroups = new ArrayList<>();
        for (int id: destinations)
            destinationGroups.add(Group.getGroup(id));
        for (Group g: destinationGroups) {
            server.send(newMessage, false, g.nodeList.get(0).pid);
            logger.debug("sent message {} to server {}", newMessage, g.nodeList.get(0));
        }
    }

    @Override
    public void callback(RamcastServerEvent event) throws IOException {
        Message m = (Message) event.getReceiveMessage();

        int type = m.getMsgType();
        switch (type) {
            case 1:
                logger.debug("received STEP1 message {}", m);
//                int clientId = m.getClientId();
//                int messageId = m.getMsgId();
//                messageConnectionMap.put(new Pair<>(clientId, messageId), connection);
                processStep1Message(m, event);
                break;
            case 2:
//                logger.debug("received STEP2 message {}", m);
                processStep2Message(m, event);
        }
    }
}
