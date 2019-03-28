package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.bezerra.netwrapper.tcp.*;
import ch.usi.dslab.lel.ramcast.RamcastSender;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Process.class);

    Node node;

//    TCPReceiver receiver;
//    private TCPSender   sender;
    Map<Integer, RamcastSender> senders = new HashMap<>();
    boolean listenForConnections;
    boolean running;
//    Thread processThread;

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
    }

    public void startRunning(int sendQueue, int recvQueue, int maxinline, int clienttimeout) {
//        receiver = listenForConnections ? new TCPReceiver(node.port) : new TCPReceiver();
//        sender = new TCPSender();

//        processThread = new Thread(this, "Process-" + node.pid);
//        processThread.start();
        running = true;
        logger.debug("Process {} started running", node.pid);
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        createConnections(sendQueue, recvQueue, maxinline, clienttimeout);
    }

    public void createConnections(int sendQueue, int recvQueue, int maxinline, int clienttimeout) {
        // once done loading the processes, start a thread here that will keep trying to connect to each
        // learner/coordinator. exceptions are likely to be thrown, as processes start at different times, but keep
        // trying, until the client is connected to all coordinators (TODO: to all learners, in case of fast opt).

        // this is sub-optimal though. ideally, a central coordinator (e.g., ZooKeeper, ZooFence, Volery...)
        // would be used. but that would be an over-optimization, done only if this library is ever published.

        for (Node node : Node.nodeMap.values()) {
            connect(node, sendQueue, recvQueue, maxinline, clienttimeout);
        }
        System.out.println("All senders created!");
    }

    public boolean connect(Node node, int sendQueue, int recvQueue, int maxinline, int clienttimeout) {
//        logger.debug("creating sender for host {}", node.host);
        RamcastSender sender =
                new RamcastSender<>(node.host, node.port, sendQueue,recvQueue, maxinline, clienttimeout, Message::new);
        logger.debug("sender created for {}", node.host);

        senders.put(node.pid, sender);
        return true;


//        boolean connected = false;
//        while (connected == false) {
//            try {
//                TCPConnection newConnection = sender.connect(node);
//                receiver.addConnection(newConnection);
//                connectedPRocesses.put(node.pid, newConnection);
//                connected = true;
//                logger.debug("Connected to Destination {}:{}", node.getAddress(), node.getPort());
//                return newConnection;
//            }
//            catch (IOException e) {
//                try {
//                    logger.debug("Destination {}:{} refused connection, retrying...", node.getAddress(), node.getPort());
//                    connected = false;
//                    Thread.sleep(1000);
//                }
//                catch (InterruptedException ee) {
//                    ee.printStackTrace();
//                    System.exit(1);
//                }
//            }
//        }
//        return null;

    }

    void send(Message msg, boolean expectReply, int nodeId) {
        senders.get(nodeId).sendMessage(msg, expectReply);
    }

    Message deliverReply(int nodeId) {
        return (Message) senders.get(nodeId).deliverReply();
    }

//    void send(Message msg, TCPDestination destination) {
//        sender.send(msg, destination);
//    }
//
//    void send(Message msg, TCPConnection connection) {
//        sender.send(msg, connection);
//    }

//    void reliableMulticast(Message msg, List<Group> destinations) {
//        List<TCPConnection> conn = new ArrayList<TCPConnection>();
//        for (int i=0; i< destinations.size(); i++) {
//            TCPConnection connection = connectedPRocesses.get(i);
//            conn.add(connection);
//        }
//        sender.multiConnectionSend(msg, conn);
//    }

//    TCPMessage receive() {
//        return receiver.receive();
//    }

//    @Override
//    public void run() {
//        try {
//            while (running) {
//                TCPMessage newTcpMsg = receiver.receive(1000);
//                if (newTcpMsg == null)
//                    continue;
//                else {
////                    TCPConnection connection = newTcpMsg.getConnection();
////                    Message contents = newTcpMsg.getContents();
////                    contents.rewind();
////                    messageConnectionMap.put(contents, connection);
//                    uponDelivery(newTcpMsg);
//                }
//            }
//            logger.debug("Exiting process received messages loop...");
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//    }

//    abstract void uponDelivery(TCPMessage m);

}
