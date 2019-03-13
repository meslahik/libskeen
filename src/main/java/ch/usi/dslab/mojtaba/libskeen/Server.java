package ch.usi.dslab.mojtaba.libskeen;

import ch.usi.dslab.bezerra.netwrapper.Message;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Server extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Server.class);

    class Pending {
        int clientId;
        int msgId;
        int nodeId;
        int LC;
        Message msg;
        List<Integer> destination;

        public Pending(int clientId, int msgId, int nodeId, int LC, Message msg, List<Integer> destination) {
            this.clientId = clientId;
            this.msgId = msgId;
            this.nodeId = nodeId;
            this.LC = LC;
            this.msg = msg;
            this.destination = destination;
        }

        @Override
        public String toString() {
            return "[pending: clientId=" + clientId + ", MsgId=" + msgId + ", nodeId=" + nodeId + ", LC=" + LC +
                    ", msg=" + msg + ", destinations=" + destination;
        }
    }

    // TODO needs to be Atomic?
    int LC = 0;

    private Map<Pair<Integer, Integer>, ArrayList<Pending>> pendingMsgs = new ConcurrentHashMap<>();
    private TreeMap<Integer, Message> ordered = new TreeMap<Integer, Message>();
    public BlockingQueue<Message> atomicDeliver = new LinkedBlockingQueue<Message>();

    public Server(int id, String configFile) {
        super(id, true, configFile);
    }

    Message atomicDeliver() {
        try {
            return atomicDeliver.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    void processPendingMessages(Pair<Integer, Integer> pair) {
        ArrayList<Pending> arr = pendingMsgs.get(pair);
        Pending p = arr.get(0);
        if (arr.size() < p.destination.size()) {
            logger.debug("should wait for server votes for pair {}. received {} out of {} replies",
                    pair, arr.size(), p.destination.size());
            return;
        }

        int maxLC = -1;
        for (int i = 0; i < arr.size(); i++) {
            int LC = arr.get(i).LC;
            maxLC = maxLC < LC ? LC : maxLC;
        }
        pendingMsgs.remove(pair);
        ordered.put(maxLC, p.msg);
        logger.debug("have enough votes for pair {}. lc {} is chosen for the message [{}:{}, {}]",
                pair, maxLC, p.clientId, p.msgId, p.msg);

        while(ordered.size() > 0) {
            Integer minOrderedLC = ordered.firstKey();

            boolean flag = true;
            Collection<ArrayList<Pending>> arrs = pendingMsgs.values();
            Iterator<ArrayList<Pending>> it = arrs.iterator();
            while(it.hasNext()) {
                ArrayList<Pending> arr2 = it.next();
                for(int i=0; i<arr2.size(); i++) {
                    Pending pending = arr2.get(i);
                    if (pending.LC < minOrderedLC) {
                        flag = false;
                        break;
                    }
                }
            }

            if(flag) {
                Map.Entry<Integer, Message> minOrdered = ordered.pollFirstEntry();
                Message msg = minOrdered.getValue();
                atomicDeliver.add(msg);
                logger.debug("atomic deliver message {}", msg);
            } else {
                logger.debug("message is in ordered queue to be delivered later");
                break;
            }
        }
    }

    void processStep2Message(Message wrapperMessage) {

        int clientId = (int ) wrapperMessage.getItem(1);
        int messageId = (int) wrapperMessage.getItem(2);
        Message message = (Message) wrapperMessage.getItem(3);
        List<Integer> destinations = (List<Integer>) wrapperMessage.getItem(4);
        int nodeId = (int) wrapperMessage.getItem(5);
        int lc = (int) wrapperMessage.getItem(6);

        Pending p = new Pending(clientId, messageId, nodeId, lc, message, destinations);
        LC = Math.max(LC, p.LC);
        Pair<Integer, Integer> pair = new Pair<Integer, Integer>(p.clientId, p.msgId);
        if (pendingMsgs.containsKey(pair))
            pendingMsgs.get(pair).add(p);
        else {
            ArrayList<Pending> arr = new ArrayList<Pending>();
            arr.add(p);
            pendingMsgs.put(pair, arr);
        }
        logger.debug("added pending {} to pending messages", pair, p);
        processPendingMessages(pair);
    }

    void processStep1Message(Message wrapperMessage) {
        int clientId = (int ) wrapperMessage.getItem(1);
        int messageId = (int) wrapperMessage.getItem(2);
        Message message = (Message) wrapperMessage.getItem(3);
        List<Integer> destinations = (List<Integer>) wrapperMessage.getItem(4);

        Message newWrapperMessage = new Message(MessageType.STEP2, clientId, messageId, message, destinations, node.pid, ++LC);

        List<Group> destinationGroups = new ArrayList<>();
        for (int id: destinations)
            destinationGroups.add(Group.getGroup(id));
        for (Group g: destinationGroups) {
            send(newWrapperMessage, g.nodeList.get(0));
            logger.debug("sent message {} to server {}", newWrapperMessage, g.nodeList.get(0));
        }
    }

    void processClientMessage(Message wrapperMessage) {
        int clientId = (int ) wrapperMessage.getItem(1);
        int messageId = (int) wrapperMessage.getItem(2);
        Message message = (Message) wrapperMessage.getItem(3);
        List<Integer> destinations = (List<Integer>) wrapperMessage.getItem(4);

        Message newWrapperMessage = new Message(MessageType.STEP1, clientId, messageId, message, destinations);

        List<Group> destinationGroups = new ArrayList<>();
        for (int id: destinations)
            destinationGroups.add(Group.getGroup(id));
        for (Group g: destinationGroups) {
            send(newWrapperMessage, g.nodeList.get(0));
            logger.debug("sent message {} to server {}", newWrapperMessage, g.nodeList.get(0));
        }
    }

    @Override
    void uponDelivery(Message m) {
        MessageType type = (MessageType) m.getItem(0);
        switch (type) {
            case CLIENT:
                logger.debug("received CLIENT message {}", m);
                processClientMessage(m);
                break;
            case STEP1:
                logger.debug("received STEP1 message {}", m);
                processStep1Message(m);
                break;
            case STEP2:
                logger.debug("received STEP2 message {}", m);
                processStep2Message(m);
        }
    }

    public static void main(String[] args) {
        int serverId = Integer.parseInt(args[0]);
        String configFile = args[1];

        Server server = new Server(serverId, configFile);
    }
}
