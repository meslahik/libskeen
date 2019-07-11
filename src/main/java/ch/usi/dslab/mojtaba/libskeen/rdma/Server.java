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

    Replica replica;

    RamcastConfig config;

//    RamcastReceiver agent;
//    RamcastReceiver agent2;
    SkeenMessageProcessor1 skeenMessageProcessor1;
    SkeenMessageProcessor2 skeenMessageProcessor2;
    BlockingQueue<Pair<Integer, Integer>> deliveredMessages = new LinkedBlockingQueue<>();

//    class Deliver implements DeliverCallback {
//
//        @Override
//        public void call(Pair<Integer, Integer> deliverPair) {
//            // delivered ordered messages ...
//        }
//    }
//
//    Deliver onDeliver = new Deliver();

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

        replica = Replica.replicaMap.get(node.pid);
//        replica.setOnDeliver();
        skeenMessageProcessor1 = new SkeenMessageProcessor1(replica);
//        skeenMessageProcessor2 = new SkeenMessageProcessor2(replica);
        new RamcastReceiver(node.host, node.port, ByteBuffer.allocateDirect(10), skeenMessageProcessor1, (x) -> {}, (x)->{});
//        new RamcastReceiver(node.host, node.port+100, ByteBuffer.allocateDirect(10), skeenMessageProcessor2, (x) -> {}, (x)->{});

        System.out.println("running...server " + node.host + ", poolsize " + poolsize + ", maxinline " + maxinline +
                ", polling " + polling + ", recvQueue " + recvQueue + ", sendQueue " + sendQueue + ", wqSize " + wqSize +
                ", rpcservice-timeout " + servicetimeout);
        startRunning(sendQueue, recvQueue, maxinline);

//        logger.debug("running replica ...");
        replica.setGatherer(isGathererEnabled, gathererHost, gathererPort, fileDirectory, experimentDuration, warmUpTime);
        replica.setServer(this);
        replica.startRunning(poolsize, recvQueue, sendQueue, wqSize, servicetimeout, polling, maxinline, signalInterval);
    }

    void addDeliveredMessage(Pair<Integer, Integer> deliverPair) {
        try {
            deliveredMessages.put(deliverPair);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Pair<Integer, Integer> deliver() {
        try {
            return deliveredMessages.take();
        } catch (Exception e) {
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

        int poolsize = 1; //never used
        int recvQueue = 100;
        int sendQueue = 100;
        int wqSize = 1;
        // all endpoints (the receiver and all senders) use this timeout =>
        //  cmProcessor events timeout (receiver and senders); cqProcessor (receiver) event timeout when polling is false
        int servicetimeout = 1; //millisecond
        boolean polling = true; //receiver
        int maxinline = 0; //byte
        int signalInterval = 1; //RDMA write signaled interval

        Server server = new Server(serverId, configFile,
                poolsize, recvQueue, sendQueue, wqSize, servicetimeout, polling, maxinline, signalInterval,
                isGathererEnabled, gathererHost, gathererPort, fileDirectory, experimentDuration, warmUpTime);
    }
}
