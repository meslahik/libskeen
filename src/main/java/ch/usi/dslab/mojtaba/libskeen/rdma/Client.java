package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;

public class Client extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Client.class);
    ThroughputPassiveMonitor tpMonitor;
    LatencyPassiveMonitor latMonitor;

    int msgId = 0;

    Semaphore sendPermits;

    RamcastConfig config;

    boolean isGathererEnabled = false;

//    public Client(int id, String configFile) {
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
        config.setPayloadSize(Message.size());


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
    }

    public void multicast(int[] destinations) {
        long sendTime = System.currentTimeMillis();
        Message message = new Message(1, node.pid, ++msgId, destinations.length, destinations, sendTime);

        List<Group> destinationGroups = new ArrayList<>();
        for (int id: destinations)
            destinationGroups.add(Group.getGroup(id));
        for (Group g: destinationGroups) {
            ByteBuffer reply = (ByteBuffer) send(message, true, g.nodeList.get(0).pid);
//            int op = reply.getInt();
//            int clientId = reply.getInt();
//            int messageId = reply.getInt();
//            logger.debug("reply: {}, {}, {}", op, clientId, messageId);
            logger.debug("reply received");
        }
        logger.debug("sent message {} to its destinations {}", message, destinations);
    }

    public void multicast(int groupID) {
        int[] destinations = new int[1];
        destinations[0] = groupID;
        multicast(destinations);
    }

//    public ByteBuffer deliverReply() {
//        try {
//            Pair<Integer, ByteBuffer> response = responses.take();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

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

    void runBatch() {
        int groupSize = Group.groupSize();
        Set<Integer> groupIDs = Group.groupIDs();
        int[] dests = new int[groupSize];
        Iterator<Integer> it = groupIDs.iterator();
        int j=0;
        while (it.hasNext())
            dests[j++] = it.next();
        System.out.println("groupsize: " + groupSize);
        System.out.println("groupIDs: " + groupIDs);

        for (int i=0; i < 100000000; i++) {
//            System.out.println("i=" + i);
            long sendTime = System.currentTimeMillis();
            multicast(dests);
//            Message reply = client.deliverReply(dests[0]);
//            logger.debug("reply: {}", reply);
            long recvTime = System.currentTimeMillis();
            if (isGathererEnabled) {
                tpMonitor.incrementCount();
                latMonitor.logLatency(sendTime, recvTime);
            }
        }
    }


    public static void main(String[] args) {
        int clientId = Integer.parseInt(args[0]);
        String configFile = args[1];

        int poolsize = 1;
        int recvQueue = 1000;
        int sendQueue = 1000;
        int wqSize = 1;
        int servicetimeout = 0;
        boolean polling = true;
        int maxinline = 0;
        int signalInterval = 1;


//        Client client = new Client(clientId, configFile);
        Client client = new Client(clientId, configFile, recvQueue, sendQueue, maxinline, servicetimeout,
                signalInterval, wqSize, polling);

        client.init(args);
        client.startRunning(sendQueue, recvQueue, maxinline, clientId);
        System.out.println("client " + client.node.pid + " started");


//        logger.debug("sending message ...");
//        client.multicast(dests);
//        logger.debug("sending message ...");
//        client.multicast(dests);

        client.runBatch();
    }
}
