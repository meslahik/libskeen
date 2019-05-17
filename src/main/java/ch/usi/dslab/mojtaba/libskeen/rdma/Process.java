package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastSender;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.util.HashMap;
import java.util.Map;

public abstract class Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Process.class);

    Node node;

    Map<Integer, RamcastSender> senders = new HashMap<>();
    boolean listenForConnections;
    boolean running;

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
        running = true;
        logger.debug("Process {} started running", node.pid);
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!listenForConnections || node.isLeader)
            createConnections(sendQueue, recvQueue, maxinline, clienttimeout);
    }

    public void createConnections(int sendQueue, int recvQueue, int maxinline, int clienttimeout) {
        // once done loading the processes, start a thread here that will keep trying to connect to each
        // learner/coordinator. exceptions are likely to be thrown, as processes start at different times, but keep
        // trying, until the client is connected to all coordinators (TODO: to all learners, in case of fast opt).

        // this is sub-optimal though. ideally, a central coordinator (e.g., ZooKeeper, ZooFence, Volery...)
        // would be used. but that would be an over-optimization, done only if this library is ever published.

        for (Group group: Group.groupList.values()) {
            connect(group.nodeList.get(0), sendQueue, recvQueue, maxinline, clienttimeout);
        }
        System.out.println("Process " + node.pid + ": All senders created!");
    }

    public boolean connect(Node node, int sendQueue, int recvQueue, int maxinline, int clienttimeout) {
//        logger.debug("connecting to node {} port {}", node.host, node.port);
        RamcastSender sender =
                new RamcastSender(node.host, node.port, sendQueue,recvQueue, maxinline, clienttimeout);
        logger.debug("sender created for {}, port {}", node.host, node.port);

        senders.put(node.pid, sender);
        return true;
    }

    Buffer send(SkeenMessage msg, boolean expectReply, int nodeId) {
        return senders.get(nodeId).send(msg.getBuffer(), expectReply);
    }

//    SkeenMessage deliverReply(int nodeId) {
//        return (SkeenMessage) senders.get(nodeId).deliverReply();
//    }

}
