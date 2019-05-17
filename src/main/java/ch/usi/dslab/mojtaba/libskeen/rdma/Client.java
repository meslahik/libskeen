package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class Client extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Client.class);
//    ThroughputPassiveMonitor tpMonitor;
//    LatencyPassiveMonitor latMonitor;

    Semaphore sendPermits;

    RamcastConfig config;

    int msgId = 0;
    BlockingQueue<SkeenMessage> receivedReply = new LinkedBlockingQueue<>();

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
//        String gathererHost = args[2];
//        int gathererPort = Integer.parseInt(args[3]);
//        String fileDirectory = args[4];
//        int experimentDuration = Integer.parseInt(args[5]);
//        int warmUpTime = Integer.parseInt(args[6]);
//
//        DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
//
//        tpMonitor = new ThroughputPassiveMonitor(node.pid, "client_overall", true);
//        latMonitor = new LatencyPassiveMonitor(node.pid, "client_overall", true);
    }

    public void multicast(int[] destinations) {
        SkeenMessage skeenMessage = new SkeenMessage(1, node.pid, ++msgId, destinations.length, destinations);

        List<Group> destinationGroups = new ArrayList<>();
        for (int id: destinations)
            destinationGroups.add(Group.getGroup(id));
        for (Group g: destinationGroups) {
            ByteBuffer buffer = (ByteBuffer) send(skeenMessage, false, g.nodeList.get(0).pid);
        }
        logger.debug("sent skeenMessage {} to its destinations {}", skeenMessage, destinations);
    }

    public void multicast(int groupID) {
        int[] destinations = new int[1];
        destinations[0] = groupID;
        multicast(destinations);
    }

//    public SkeenMessage deliverReply() {
//        try {
//            return receivedReply.take();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

//    @Override
//    void uponDelivery(TCPMessage tcpMessage) {
//        try {
//            SkeenMessage m = tcpMessage.getContents();
//            logger.debug("received reply: " + m);
//            m.rewind();
//            receivedReply.put(m);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
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

    public static void main(String[] args) {
        int clientId = Integer.parseInt(args[0]);
        String configFile = args[1];

        int poolsize = 1;
        int recvQueue = 100;
        int sendQueue = 100;
        int wqSize = recvQueue;
        int servicetimeout = 0;
        boolean polling = false;
        int maxinline = 0;
        int signalInterval = 1;



        Client client = new Client(clientId, configFile, recvQueue, sendQueue, maxinline, servicetimeout,
                signalInterval, wqSize, polling);
        client.init(args);
        client.startRunning(sendQueue, recvQueue, maxinline, clientId);
        System.out.println("client " + client.node.pid + " started");

        int groupSize = Group.groupSize();
        Set<Integer> groupIDs = Group.groupIDs();
        int[] dests = new int[groupSize];

        Iterator<Integer> it = groupIDs.iterator();
        int j=0;
        while (it.hasNext())
            dests[j++] = it.next();

//        System.out.println("groupsize: " + groupSize);
//        System.out.println("groupIDs: " + groupIDs);


//        logger.debug("sending message ...");
//        client.multicast(dests);
//        logger.debug("sending message ...");
//        client.multicast(dests);

        for (int i=0; i < 2; i++) {
//            long sendTime = System.currentTimeMillis();
            client.multicast(dests);
//            SkeenMessage reply = client.deliverReply(dests[0]);
//            logger.debug("reply: {}", reply);
//            long recvTime = System.currentTimeMillis();
//            client.tpMonitor.incrementCount();;
//            client.latMonitor.logLatency(sendTime, recvTime);
        }
    }
}
