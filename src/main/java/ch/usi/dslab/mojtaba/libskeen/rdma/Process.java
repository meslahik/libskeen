package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.bezerra.netwrapper.tcp.*;
import ch.usi.dslab.bezerra.sense.datapoints.TimelineDataPoint;
import ch.usi.dslab.lel.ramcast.RamcastSender;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class Process implements Runnable {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Process.class);

    Node node;

    Map<Integer, RamcastSender> senders = new HashMap<>();
    boolean listenForConnections;
    boolean running;

    LinkedBlockingQueue<Pair<Integer, ByteBuffer>> responses = new LinkedBlockingQueue<>();

    public Process(int id, boolean isServer, String configFile) {
        Configuration.loadConfig(configFile);
        if (isServer) {
            node = Node.getNode(id);
            listenForConnections = true;
        } else {
            node = new Node(id, false);
            listenForConnections = false;
        }
    }

    public void startRunning(int sendQueue, int recvQueue, int maxinline, int clienttimeout) {
        running = true;
        logger.debug("Process {} started running", node.pid);
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        createConnections(sendQueue, recvQueue, maxinline, clienttimeout);

//        Thread deliverer = new Thread(this, "Deliverer-" + node.pid);
//        deliverer.start();
//        logger.debug("deliverer started");
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
        System.out.println("Process " + node.pid + ": All senders created!");
    }

    public boolean connect(Node node, int sendQueue, int recvQueue, int maxinline, int clienttimeout) {
//        logger.debug("creating sender for host {}", node.host);
        RamcastSender sender =
                new RamcastSender(node.host, node.port, sendQueue,recvQueue, maxinline, clienttimeout);
        logger.debug("sender created for {}", node.host);

        senders.put(node.pid, sender);
        return true;
    }

    Buffer send(Message msg, boolean expectReply, int nodeId) {
        return senders.get(nodeId).send(msg.getBuffer(), expectReply);
    }


//    Buffer deliverReply(int nodeId) {
//        RamcastSender sender = senders.get(nodeId);
//        return sender.deliverReply();
//    }

    @Override
    public void run() {
//        int size = senders.size();
//        for (int i = 0; true; i++) {
//            int senderId = i % size;
//            ByteBuffer reply = senders.get(senderId).deliverReply();
//            try {
//                responses.put(new Pair<>(i, reply));
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }
}
