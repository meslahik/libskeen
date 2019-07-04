package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastFuture;
import ch.usi.dslab.lel.ramcast.RamcastReceiver;
import ch.usi.dslab.lel.ramcast.RamcastSender;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Replica implements Runnable {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Replica.class);

    static HashMap<Integer, Replica> replicaMap;

    static {
        replicaMap = new HashMap<>();
    }

    int pid;
    int gid;

    String host;
    int port;

    RamcastReceiver agent;
    Map<Integer, RamcastSender> senders = new HashMap<>();

    ConsensusMsgProcessor consensusMsgProcessor;

    int instanceNum = 0;

    Replica(String host, int port, int pid, int gid) {
        this.host = host;
        this.port = port;
        this.pid = pid;
        this.gid = gid;
        replicaMap.put(pid, this);
        consensusMsgProcessor = new ConsensusMsgProcessor(this);
    }

    void startRunning(int poolsize, int recvQueue, int sendQueue, int wqSize, int servicetimeout,
                      boolean polling, int maxinline, int signalInterval) {
        agent = new RamcastReceiver(host, port, ByteBuffer.allocateDirect(10), consensusMsgProcessor, (x) -> {}, (x)->{});
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        createConnections(sendQueue, recvQueue, maxinline, servicetimeout);

        Thread senderThread = new Thread(this, "SenderThread-" + pid);
        senderThread.start();
    }

    public void createConnections(int sendQueue, int recvQueue, int maxinline, int clienttimeout) {
        for (Replica replica : Group.groupList.get(gid).replicaList) {
            connect(replica, sendQueue, recvQueue, maxinline, clienttimeout);
        }
        System.out.println("Replica " + pid + ": All senders created!");
    }

    public boolean connect(Replica replica, int sendQueue, int recvQueue, int maxinline, int clienttimeout) {
        RamcastSender sender =
                new RamcastSender(replica.host, replica.port, sendQueue,recvQueue, maxinline);
        logger.debug("sender created for {}, {}", replica.host, replica.port);

        senders.put(replica.pid, sender);
        return true;
    }

    public String getAddress() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return Integer.toString(pid);
    }

    // Learner
    public SkeenMessage decide() {
        try {
            return consensusMsgProcessor.decidedQueue.take();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    Buffer send(ConsensusMessage msg, boolean expectReply, int nodeId) {
        return senders.get(nodeId).send(msg.getBuffer(), expectReply);
    }

    RamcastFuture sendNonBlocking(ConsensusMessage msg, boolean expectReply, int nodeId) {
        return senders.get(nodeId).sendNonBlocking(msg.getBuffer(), expectReply);
    }

    Buffer deliverReply(int nodeId) {
        return senders.get(nodeId).deliverReply();
    }

    // Proposer
    public void propose(SkeenMessage skeenMessage) {
        ConsensusMessage wrapperMessage = new ConsensusMessage(1, ++instanceNum, skeenMessage);
        sendingMessages.add(wrapperMessage);
    }

    // Acceptor
    public void accept(ConsensusMessage consensusMessage) {
        sendingMessages.add(consensusMessage);
    }

    LinkedBlockingQueue<ConsensusMessage> sendingMessages = new LinkedBlockingQueue<>();

    @Override
    public void run() {
        while (true) {
            try {
                ConsensusMessage consensusMessage = sendingMessages.take();
                ArrayList<Replica> replicaList = Group.getGroup(gid).replicaList;
                logger.debug("replica {} is sending to replicas {} message {}", pid, replicaList, consensusMessage);
                for (Replica replica: replicaList)
                    sendNonBlocking(consensusMessage, true, replica.pid);
                logger.debug("replica {} is waiting for ACKs from replicas {}", pid, replicaList);
                for (Replica replica: replicaList)
                    deliverReply(replica.pid);
                logger.debug("replica {} sending finished", pid);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
