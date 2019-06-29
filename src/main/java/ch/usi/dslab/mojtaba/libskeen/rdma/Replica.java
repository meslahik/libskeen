package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastReceiver;
import ch.usi.dslab.lel.ramcast.RamcastSender;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Replica {
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

    // Learner
    public SkeenMessage decide() {
        try {
            return consensusMsgProcessor.decidedQueue.take();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Proposer
    public void propose(SkeenMessage skeenMessage) {
        ConsensusMessage wrapperMessage = new ConsensusMessage(1, ++instanceNum, skeenMessage);
        for (Replica replica: Group.getGroup(gid).replicaList) {
            logger.debug("propose sent to replica {}, {}", replica.pid, wrapperMessage);
            senders.get(replica.pid).send(wrapperMessage.getBuffer(), false);
        }
    }

}
