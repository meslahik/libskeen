package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Semaphore;

public class Client extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Client.class);
    ThroughputPassiveMonitor tpMonitor;
    LatencyPassiveMonitor latMonitor;

    Semaphore sendPermits;

    int msgId = 0;
    RamcastConfig config;
    boolean isGathererEnabled = false;

    public Client(int id, String configFile,
                  int recvQueue, int sendQueue, int maxinline, int servicetimeout,
                  int signalInterval, int wqSize, boolean polling) {
        super(id, false, configFile);

        config = RamcastConfig.getInstance();
        config.setRecvQueueSize(recvQueue);
        config.setSendQueueSize(sendQueue);
        config.setMaxinline(maxinline);
        config.setServiceTimeout(servicetimeout);
        config.setSignalInterval(signalInterval);
        config.setWrQueueSize(wqSize);
        config.setPolling(polling);
        config.setPayloadSize(ConsensusMessage.size());
    }

    public void init(String[] args) {
        isGathererEnabled = Boolean.parseBoolean(args[2]);
        String gathererHost = args[3];
        int gathererPort = Integer.parseInt(args[4]);
        String fileDirectory = args[5];
        int experimentDuration = Integer.parseInt(args[6]);
        int warmUpTime = Integer.parseInt(args[7]);

        if (isGathererEnabled) {
            DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
            tpMonitor = new ThroughputPassiveMonitor(node.pid, "client_overall", true);
            latMonitor = new LatencyPassiveMonitor(node.pid, "client_overall", true);
        }
        setDestinations();
    }

    int[] destinations;
    int[] destinationGroups;

    void setDestinations() {
        int groupSize = Group.groupSize();
        this.destinations = new int[groupSize];
        this.destinationGroups = new int[groupSize];
        int i = 0;
        for (Group group: Group.groupList.values()) {
            destinationGroups[i] = group.id;
            destinations[i++] = group.nodeList.get(0).pid;
        }
        System.out.println("group size: " + groupSize);
        System.out.println("group Leader IDs: " + Arrays.toString(destinations));
    }

    public void multicast() {
        SkeenMessage skeenMessage = new SkeenMessage(1, node.pid, ++msgId, destinations.length, destinationGroups);

        for (int i=0; i < destinations.length; i++) {
            logger.debug("sending message {}", skeenMessage);
            sendNonBlocking(skeenMessage, true, destinations[i]);
//            ByteBuffer reply = (ByteBuffer) sendConsensusStep2Message(message, false, g.nodeList.get(0).pid);
//            int op = reply.getInt();
//            int clientId = reply.getInt();
//            int messageId = reply.getInt();
//            logger.debug("reply: {}, {}, {}", op, clientId, messageId);
//            logger.debug("reply received");
        }
        logger.debug("multicast {} to its destinations {}", skeenMessage, destinations);

        for (int k = 0; k < destinations.length; k++)
            deliverReply(destinations[k]);

    }

    void runBatch() {
        for (int i = 0; i < 100000000; i++) {
            long sendTime = System.currentTimeMillis();
            multicast();
            long recvTime = System.currentTimeMillis();
            if (isGathererEnabled) {
                tpMonitor.incrementCount();
                latMonitor.logLatency(sendTime, recvTime);
            }
//            try {
//                Thread.sleep(10);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
        }
    }

    void getPermit() {
        try {
            sendPermits.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void releasePermit() {
        sendPermits.release();
    }

    public static void main(String[] args) {
        int clientId = Integer.parseInt(args[0]);
        String configFile = args[1];

        int poolsize = 1;
        int recvQueue = 100;
        int sendQueue = 100;
        int wqSize = 1;
        int servicetimeout = 1; //millisecond
        boolean polling = true; // not used for clients
        int maxinline = 0;
        int signalInterval = 1;


        Client client = new Client(clientId, configFile, recvQueue, sendQueue, maxinline, servicetimeout,
                signalInterval, wqSize, polling);
        client.init(args);
        client.startRunning(sendQueue, recvQueue, maxinline);
        System.out.println("client " + client.node.pid + " started");


//        logger.debug("sending message ...");
//        client.multicast(dests);
//        logger.debug("sending message ...");
//        client.multicast(dests);
        client.runBatch();
    }
}
