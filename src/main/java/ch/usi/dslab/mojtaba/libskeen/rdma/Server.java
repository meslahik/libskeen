package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.RamcastReceiver;

import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Server extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Server.class);

    Replica replica = Replica.replicaMap.get(node.pid);

    RamcastConfig config;

    RamcastReceiver agent;
    MessageProcessor messageProcessor;
    BlockingQueue<Pair<Integer, Integer>> atomicDeliver = new LinkedBlockingQueue<>();

    public Server(int id, String configFile,
                  int poolsize, int recvQueue, int sendQueue, int wqSize, int servicetimeout,
                  boolean polling, int maxinline, int signalInterval,
                  boolean isGathererEnabled, String gathererHost, int gathererPort, String fileDirectory, int experimentDuration, int warmUpTime) {
        super(id, true, configFile);

        config = RamcastConfig.getInstance();
        config.setRecvQueueSize(recvQueue);
        config.setSendQueueSize(sendQueue);
        config.setMaxinline(maxinline);
        config.setServiceTimeout(servicetimeout);
        config.setSignalInterval(signalInterval);
        config.setWrQueueSize(wqSize);
        config.setPolling(polling);
        config.setPayloadSize(ConsensusMessage.size());

        messageProcessor = new MessageProcessor(this, replica,
                isGathererEnabled, gathererHost, gathererPort, fileDirectory, experimentDuration, warmUpTime);
        agent = new RamcastReceiver(node.host, node.port, ByteBuffer.allocateDirect(10), messageProcessor, (x) -> {}, (x)->{});

        System.out.println("running...server " + node.host + ", poolsize " + poolsize + ", maxinline " + maxinline +
                ", polling " + polling + ", recvQueue " + recvQueue + ", sendQueue " + sendQueue + ", wqSize " + wqSize +
                ", rpcservice-timeout " + servicetimeout);
        startRunning(sendQueue, recvQueue, maxinline);
//        logger.debug("running replica ...");
//        replica.startRunning(poolsize, recvQueue, sendQueue, wqSize, servicetimeout, polling, maxinline, signalInterval);
        replica.startRunning(poolsize, recvQueue, sendQueue, wqSize, servicetimeout, polling, maxinline, signalInterval);
    }

    void addDeliveredMessage(Pair<Integer, Integer> deliverPair) {
        atomicDeliver.add(deliverPair);
    }

    Pair<Integer, Integer> atomicDeliver() {
        try {
            return atomicDeliver.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        int serverId = Integer.parseInt(args[0]);
        String configFile = args[1];

        boolean isGathererEnabled = Boolean.parseBoolean(args[2]);
        String gathererHost = args[3];
        int gathererPort = Integer.parseInt(args[4]);
        String fileDirectory = args[5];
        int experimentDuration = Integer.parseInt(args[6]);
        int warmUpTime = Integer.parseInt(args[7]);

        int poolsize = 1;
        int recvQueue = 100;
        int sendQueue = 100;
        int wqSize = 1;
        // all endpoints (the receiver and all senders) use this timeout =>
        //  cmProcessor events timeout (receiver and senders); cqProcessor (receiver) event timeout when polling is false
        int servicetimeout = 1; //millisecond
        boolean polling = true; //receiver
        int maxinline = 0;
        int signalInterval = 1;


        Server server = new Server(serverId, configFile,
                poolsize, recvQueue, sendQueue, wqSize, servicetimeout, polling, maxinline, signalInterval,
                isGathererEnabled, gathererHost, gathererPort, fileDirectory, experimentDuration, warmUpTime);
    }
}
