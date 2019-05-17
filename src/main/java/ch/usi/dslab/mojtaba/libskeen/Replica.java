package ch.usi.dslab.mojtaba.libskeen;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.tcp.*;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Replica implements Runnable, TCPDestination{
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Replica.class);

    enum ConsensusMessageType {
        TWOA,
        TWOB
    }

    static HashMap<Integer, Replica> replicaMap;

    static {
        replicaMap = new HashMap<>();
    }

    int pid;
    int gid;

    String host;
    int port;

    TCPReceiver receiver;
    private TCPSender sender;

    BlockingQueue<Message> decidedQueue = new LinkedBlockingQueue<>();
    Map<Integer, TCPConnection> connectedPRocesses = new HashMap<>();

    // Acceptor
//    Map<Integer, Message> messages = new HashMap<>();

    // Learner
    Map<Integer, ArrayList<Integer>> pendingMessages = new HashMap<>();
    TreeMap<Integer, Message> pendingDeliverMessages = new TreeMap<>();

    int instanceNum = 0;
    int lastDeliveredInstance = 0;

    Replica(String host, int port, int pid, int gid) {
        this.host = host;
        this.port = port;
        this.pid = pid;
        this.gid = gid;
        replicaMap.put(pid, this);
    }

    void startRunning() {
        receiver = new TCPReceiver(port);
        sender = new TCPSender();
        Thread processThread = new Thread(this, "Replica-" + pid);
        processThread.start();
        createConnections();
    }

    // Learner
    public Message decide() {
        try {
            return decidedQueue.take();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Proposer
    public void propose(Message message) {
        Message wrapperMessage = new Message(ConsensusMessageType.TWOA, ++instanceNum, message);
        for (Replica replica: Group.getGroup(gid).replicaList) {
            logger.debug("propose sent to replica {}, {}", replica.pid, wrapperMessage);
            send(wrapperMessage, replica);
        }
    }

    public void createConnections() {
        for (Replica replica : Group.getGroup(gid).replicaList) {
            connect(replica);
        }
    }

    public TCPConnection connect(Replica replica) {
        boolean connected = false;
        while (connected == false) {
            try {
                TCPConnection newConnection = sender.connect(replica);
                receiver.addConnection(newConnection);
                connectedPRocesses.put(replica.pid, newConnection);
                connected = true;
                logger.debug("Connected to Destination {}:{}", replica.getAddress(), replica.getPort());
                return newConnection;
            }
            catch (IOException e) {
                try {
                    logger.debug("Destination {}:{} refused connection, retrying...", replica.getAddress(), replica.getPort());
                    connected = false;
                    Thread.sleep(1000);
                }
                catch (InterruptedException ee) {
                    ee.printStackTrace();
                    System.exit(1);
                }
            }
        }
        return null;
    }

    void send(Message msg, TCPDestination destination) {
        sender.send(msg, destination);
    }

    void send(Message msg, TCPConnection connection) {
        sender.send(msg, connection);
    }

    // Acceptor
    void processTWOAMessage(Message wrapperMessage) {
        int msgInstanceNum = (int) wrapperMessage.getItem(1);
        Message message = (Message) wrapperMessage.getItem(2);

//        messages.put(instanceNum, message);
        Message newWrapperMessage = new Message(ConsensusMessageType.TWOB, msgInstanceNum, message, pid);
        for (Replica replica: Group.getGroup(gid).replicaList)
            send(newWrapperMessage, replica);
    }

    // Learner
    void processTWOBMessage(Message wrapperMessage) {
        logger.debug("process message {}", wrapperMessage);
        int msgInstanceNum = (int) wrapperMessage.getItem(1);
        if (lastDeliveredInstance >= msgInstanceNum) {
            logger.debug("ignore message {}", wrapperMessage);
            return;
        }


        Message message = (Message) wrapperMessage.getItem(2);
        int replicaId = (int) wrapperMessage.getItem(3);

        if (pendingMessages.containsKey(msgInstanceNum))
            pendingMessages.get(msgInstanceNum).add(replicaId);
        else {
            ArrayList<Integer> array = new ArrayList<>();
            array.add(replicaId);
            pendingMessages.put(msgInstanceNum, array);
        }

        ArrayList<Integer> arrayList = pendingMessages.get(msgInstanceNum);
        if (arrayList.size() > Group.getGroup(gid).replicaList.size() / 2) {
            logger.debug("put into pending message for decision delivery. received {} votes for decision instance {}.", arrayList.size(), msgInstanceNum);
            pendingDeliverMessages.put(msgInstanceNum, message);
        }

        while(pendingDeliverMessages.size() != 0) {
            if (pendingDeliverMessages.firstEntry().getKey() == lastDeliveredInstance + 1) {
                Message deliverMessage = pendingDeliverMessages.pollFirstEntry().getValue();
                lastDeliveredInstance++;
                try {
                    logger.debug("replica {} decide message {}; lastDeliverdInstance {}", pid, deliverMessage, lastDeliveredInstance);
                    decidedQueue.put(deliverMessage);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else break;
        }
    }

    // Acceptor & Learner
    @Override
    public void run() {
        try {
            while (true) {
                TCPMessage newTcpMsg = receiver.receive(1000);
                if (newTcpMsg == null)
                    continue;
                else {
//                    TCPConnection connection = newTcpMsg.getConnection();
                    Message wrapperMessage = newTcpMsg.getContents();
                    ConsensusMessageType type = (ConsensusMessageType) wrapperMessage.getItem(0);

                    switch (type) {
                        case TWOA:
//                            logger.debug("received TWOA message {}", wrapperMessage);
                            processTWOAMessage(wrapperMessage);
                            break;
                        case TWOB:
//                            logger.debug("received TWOB message {}", wrapperMessage);
                            processTWOBMessage(wrapperMessage);
                    }

                }
            }
//            logger.debug("Exiting process received messages loop...");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public String getAddress() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }
}
