package ch.usi.dslab.mojtaba.libskeen;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPConnection;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPMessage;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Server extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Server.class);

    class Pending {
        int processId;
        int msgId;
        int LC;
        Message msg;
        List<Group> destination;

        public Pending(int processId, int msgId, int LC, Message msg, List<Group> destination) {
            this.processId = processId;
            this.msgId = msgId;
            this.LC = LC;
            this.msg = msg;
            this.destination = destination;
        }
    }

    int LC = 0;
    int msgId = 0;

    private Map<Pair<Integer, Integer>, ArrayList<Pending>> pendingMsgs = new ConcurrentHashMap<Pair<Integer, Integer>, ArrayList<Pending>>();
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

//    void launch() {
//        while(true) {
//            TCPMessage tcpMessage = uponDelivery();
//            TCPConnection connection = tcpMessage.getConnection();
//            Message msg = tcpMessage.getContents();
//            SkeenMessage.OP op = (SkeenMessage.OP) msg.getItem(0);
//
//            switch (op) {
//                case Step1:
//                    Message msg2 = (Message) msg.getItem(1);
//                    int msgId = (Integer) msg.getItem(2);
//                    List<Group> dests = (List<Group>) msg.getItem(3);
//                    Message newMsg = new Message(SkeenMessage.OP.Step2, new Pending(node.pid, msgId, ++LC, msg2, dests));
//                    reliableMulticast(newMsg, dests);
//                    break;
//                case Step2:
//                    Pending p = (Pending) msg.getItem(1);
//                    LC = Math.max(LC, p.LC);
//                    Pair<Integer, Integer> pair = new Pair<Integer, Integer>(p.processId, p.msgId);
//                    if (pendingMsgs.containsKey(pair))
//                        pendingMsgs.get(pair).add(p);
//                    else {
//                        ArrayList<Pending> arr = new ArrayList<Pending>();
//                        arr.add(p);
//                        pendingMsgs.put(pair, arr);
//                    }
//                    processPending(pair, p);
//            }
//        }
//    }
//
//    void processPending(Pair<Integer, Integer> pair, Pending p) {
//        ArrayList<Pending> arr = pendingMsgs.get(pair);
//        if (arr.size() < p.destination.size())
//            return;
//
//        int maxLC = -1;
//        for (int i = 0; i < arr.size(); i++) {
//            int LC = arr.get(i).LC;
//            maxLC = maxLC < LC ? LC : maxLC;
//        }
//        pendingMsgs.remove(pair);
//        ordered.put(maxLC, p.msg);
//
//        while(ordered.size() > 0) {
//            Integer minOrderedLC = ordered.firstKey();
//
//            boolean flag = true;
//            Collection<ArrayList<Pending>> arrs = pendingMsgs.values();
//            Iterator<ArrayList<Pending>> it = arrs.iterator();
//            while(it.hasNext()) {
//                ArrayList<Pending> arr2 = it.next();
//                for(int i=0; i<arr2.size(); i++) {
//                    Pending pending = arr2.get(i);
//                    if (pending.LC < minOrderedLC) {
//                        flag = false;
//                        break;
//                    }
//                }
//            }
//
//            if(flag) {
//                Map.Entry<Integer, Message> minOrdered = ordered.pollFirstEntry();
//                Message msg = minOrdered.getValue();
//                atomicDeliver.add(msg);
//            } else
//                break;
//        }
//    }

//    void send() {
//        Message newMsg = new Message(SkeenMessage.OP.Step1, msg, ++msgId, destinations);
//        reliableMulticast(newMsg, destinations);
//    }

    @Override
    void uponDelivery(Message m) {
        logger.debug("received message " + m);
        Message reply = new Message("Ack for message " + m.getItem(0));
        TCPConnection connection = messageConnectionMap.get(m);
        send(reply, connection);
        logger.debug("sent reply: " + reply);
    }

    public static void main(String[] args) {
        int serverId = Integer.parseInt(args[0]);
        String configFile = args[1];

        Server server = new Server(serverId, configFile);
    }
}
