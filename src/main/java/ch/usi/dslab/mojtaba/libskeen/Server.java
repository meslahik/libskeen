package ch.usi.dslab.mojtaba.libskeen;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPConnection;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPMessage;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    class ConsensusDeliverer implements Runnable {

        @Override
        public void run() {
            while (true) {
                Message msg = replica.decide();
                deliverConsensus(msg);
            }
        }
    }

    // TODO: needs to be Atomic?
    int LC = 0;

    private Map<Pair<Integer, Integer>, ArrayList<Pending>> pendingMsgs = new ConcurrentHashMap<>();
    private TreeMap<Integer, Pending> ordered = new TreeMap<>();

    static Map<Pair<Integer, Integer>, TCPConnection> messageConnectionMap = new HashMap<>();

    // Replica
    Replica replica = Replica.replicaMap.get(node.pid);
//    public BlockingQueue<Message> consensusMsgs = new LinkedBlockingQueue<>();

    public Server(int id, String configFile) {
        super(id, true, configFile);
        replica.startRunning();
        Thread consensusDeliverer = new Thread(new ConsensusDeliverer(), "ConsensusDeliverer-" + node.pid);
        consensusDeliverer.start();
        System.out.println("server " + id + " started");
    }

//    Message atomicDeliver() {
//        try {
//            return atomicDeliver.take();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    void processPendingMessages(Pair<Integer, Integer> pair) {
        ArrayList<Pending> arr = pendingMsgs.get(pair);
        Pending p = arr.get(0);
        if (arr.size() < p.destination.size()) {
            logger.debug("should wait for server votes for pair {}. received {} out of {} votes",
                    pair, arr.size(), p.destination.size());
            return;
        }

        int maxLC = -1;
        for (int i = 0; i < arr.size(); i++) {
            int LC = arr.get(i).LC;
            maxLC = maxLC < LC ? LC : maxLC;
        }
        pendingMsgs.remove(pair);
        logger.debug("have enough votes for pair {}. lc {} is chosen for the message [{}:{}, {}]",
                pair, maxLC, p.clientId, p.msgId, p.msg);
        ordered.put(maxLC, p);
        logger.debug("message {}:{} is put in ordered queue to be delivered", maxLC, p.msg);

        while(ordered.size() > 0) {
            Map.Entry<Integer, Pending> minOrdered =  ordered.firstEntry();
            Integer minOrderedLC = minOrdered.getKey();
            Pending pending = minOrdered.getValue();

            boolean flag = true;
            Collection<ArrayList<Pending>> arrs = pendingMsgs.values();
            Iterator<ArrayList<Pending>> it = arrs.iterator();
            while(it.hasNext()) {
                ArrayList<Pending> arr2 = it.next();
                for(int i=0; i<arr2.size(); i++) {
                    Pending pending2 = arr2.get(i);
                    if (pending2.LC < minOrderedLC) {
                        flag = false;
                        break;
                    }
                }
            }

            if(flag) {
                ordered.pollFirstEntry();
//                atomicDeliver.add(pending.msg);
                logger.debug("atomic decide message {}:{}", minOrderedLC, pending.msg);

                Pair<Integer, Integer> pairOrdered = new Pair<>(pending.clientId, pending.msgId);
                TCPConnection connection = messageConnectionMap.get(pairOrdered);
                if (node.pid == 0 && connection == null) { //TODO: node.pid == ??
                    logger.debug("no connection found for pair {}", pairOrdered);
                } else if (node.pid == 0) {
                    Message reply = new Message("Ack for " + pending.msg);
                    send(reply, connection);
                    logger.debug("sent reply: {} to {}", reply, connection);
                    messageConnectionMap.remove(pairOrdered);
                    logger.debug("removed pair {} form message connection map", pairOrdered);
                }

            } else {
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
        logger.debug("added {} to pending messages", p);
        processPendingMessages(pair);
    }

    void processStep1Message(Message wrapperMessage) {
        int clientId = (int ) wrapperMessage.getItem(1);
        int messageId = (int) wrapperMessage.getItem(2);
        Message message = (Message) wrapperMessage.getItem(3);
        List<Integer> destinations = (List<Integer>) wrapperMessage.getItem(4);

        LC++;

        if (node.isLeader) {
            Message newWrapperMessage = new Message(MessageType.STEP2, clientId, messageId, message, destinations, node.pid, LC);

            List<Group> destinationGroups = new ArrayList<>();
            for (int id : destinations)
                destinationGroups.add(Group.getGroup(id));
            for (Group g : destinationGroups) {
                send(newWrapperMessage, g.nodeList.get(0));
                logger.debug("sent message {} to server {}", newWrapperMessage, g.nodeList.get(0));
            }
        }
    }

    void deliverConsensus(Message m) {
        MessageType type = (MessageType) m.getItem(0);
        switch (type) {
            case STEP1:
                logger.debug("received STEP1 message {}", m);
                processStep1Message(m);
                break;
            case STEP2:
                logger.debug("received STEP2 message {}", m);
                processStep2Message(m);
        }
    }

    // only leader processes decide messages; client sends to leaders; leaders also sends to leaders
    @Override
    void uponDelivery(TCPMessage tcpMessage) {
        TCPConnection connection = tcpMessage.getConnection();
        Message m = tcpMessage.getContents();

        MessageType type = (MessageType) m.getItem(0);
        switch (type) {
            case STEP1:
                int clientId = (int) m.getItem(1);
                int messageId = (int) m.getItem(2);
                Pair<Integer, Integer> pair = new Pair<>(clientId, messageId);
                messageConnectionMap.put(pair, connection);
                logger.debug("messageConnectionMap put {}, {}", pair, connection);
        }

        replica.propose(m);
    }

    public static void main(String[] args) {
        int serverId = Integer.parseInt(args[0]);
        String configFile = args[1];

        Server server = new Server(serverId, configFile);
    }
}
