package ch.usi.dslab.mojtaba.libskeen;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.tcp.*;
import com.google.common.collect.MapMaker;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public abstract class Process implements Runnable {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Process.class);

    Node node;

    TCPReceiver receiver;
    private TCPSender   sender;
    boolean listenForConnections;
    boolean running;
    Thread processThread;

    static Map<Message, TCPConnection> messageConnectionMap = new MapMaker().weakKeys().weakValues().makeMap();

//    static Map<Integer, Process> pidsIndex = new ConcurrentHashMap<>();

    Map<Integer, TCPConnection> connectedPRocesses = new HashMap<Integer, TCPConnection>();


    public Process(int id, boolean isServer, String configFile) {
        Configuration.loadConfig(configFile);
        if (isServer) {
            node = Node.getNode(id);
//            pidsIndex.put(node.pid, this);
            listenForConnections = true;
        } else {
            node = new Node(id, false);
//            pidsIndex.put(node.pid, this);
            listenForConnections = false;
        }
        startRunning();
    }

    public void startRunning() {
        receiver = listenForConnections ? new TCPReceiver(node.port) : new TCPReceiver();
        sender = new TCPSender();
        running = true;
        processThread = new Thread(this, "Process-" + node.pid);
        processThread.start();
        logger.debug("Process {} started running", node.pid);
        createConnections();
    }

    public void createConnections() {
        // once done loading the processes, start a thread here that will keep trying to connect to each
        // learner/coordinator. exceptions are likely to be thrown, as processes start at different times, but keep
        // trying, until the client is connected to all coordinators (TODO: to all learners, in case of fast opt).

        // this is sub-optimal though. ideally, a central coordinator (e.g., ZooKeeper, ZooFence, Volery...)
        // would be used. but that would be an over-optimization, done only if this library is ever published.

        for (Node node : Node.nodeMap.values()) {
            connect(node);
        }
    }

    public TCPConnection connect(Node node) {
        boolean connected = false;
        while (connected == false) {
            try {
                TCPConnection newConnection = sender.connect(node);
                receiver.addConnection(newConnection);
                connectedPRocesses.put(node.pid, newConnection);
                connected = true;
                logger.debug("Connected to Destination {}:{}", node.getAddress(), node.getPort());
                return newConnection;
            }
            catch (IOException e) {
                try {
                    logger.debug("Destination {}:{} refused connection, retrying...", node.getAddress(), node.getPort());
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

    void reliableMulticast(Message msg, List<Group> destinations) {
        List<TCPConnection> conn = new ArrayList<TCPConnection>();
        for (int i=0; i< destinations.size(); i++) {
            TCPConnection connection = connectedPRocesses.get(i);
            conn.add(connection);
        }
        sender.multiConnectionSend(msg, conn);
    }

    TCPMessage receive() {
        return receiver.receive();
    }

    @Override
    public void run() {
        try {
            while (running) {
                TCPMessage newTcpMsg = receiver.receive(1000);
                if (newTcpMsg == null)
                    continue;
                else {
                    TCPConnection connection = newTcpMsg.getConnection();
                    Message contents = newTcpMsg.getContents();
                    contents.rewind();
                    messageConnectionMap.put(contents, connection);
                    uponDelivery(contents);
                }
            }
            logger.debug("Exiting process received messages loop...");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    abstract void uponDelivery(Message m);

}
