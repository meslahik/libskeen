package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.RamcastReceiver;

import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class Server extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Server.class);

    Replica replica;

    RamcastConfig config;

    RamcastReceiver agent;
    MessageProcessor messageProcessor;

    class Deliver implements DeliverCallback {

        @Override
        public void call(Pair<Integer, Integer> deliverPair) {
            // delivered ordered messages ...
        }
    }

    Deliver onDeliver = new Deliver();

    public Server(int id, String configFile,
                  int poolsize, int recvQueue, int sendQueue, int wqSize, int servicetimeout,
                  boolean polling, int maxinline, int signalInterval,
                  boolean isGathererEnabled, String gathererHost, int gathererPort, String fileDirectory, int experimentDuration, int warmUpTime) {
        super(id, true, configFile);

        Replica.replicaMap.get(node.pid).setOnDeliver(onDeliver);

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

        messageProcessor = new MessageProcessor(replica);
        agent = new RamcastReceiver(node.host, node.port, ByteBuffer.allocateDirect(10), messageProcessor, (x) -> {}, (x)->{});

        System.out.println("running...server " + node.host + ", poolsize " + poolsize + ", maxinline " + maxinline +
                ", polling " + polling + ", recvQueue " + recvQueue + ", sendQueue " + sendQueue + ", wqSize " + wqSize +
                ", rpcservice-timeout " + servicetimeout);
        startRunning(sendQueue, recvQueue, maxinline);

//        logger.debug("running replica ...");
        replica.setGatherer(isGathererEnabled, gathererHost, gathererPort, fileDirectory, experimentDuration, warmUpTime);
        replica.setServer(this);
        replica.startRunning(poolsize, recvQueue, sendQueue, wqSize, servicetimeout, polling, maxinline, signalInterval);
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
