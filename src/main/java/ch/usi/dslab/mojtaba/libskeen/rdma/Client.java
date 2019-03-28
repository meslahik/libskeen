package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Client extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Client.class);
    ThroughputPassiveMonitor tpMonitor;
    LatencyPassiveMonitor latMonitor;

    int msgId = 0;
    BlockingQueue<Message> receivedReply = new LinkedBlockingQueue<>();

    public Client(int id, String configFile) {
        super(id, false, configFile);
    }

    public void init(String[] args) {
        String gathererHost = args[2];
        int gathererPort = Integer.parseInt(args[3]);
        String fileDirectory = args[4];
        int experimentDuration = Integer.parseInt(args[5]);
        int warmUpTime = Integer.parseInt(args[6]);

        DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);

        tpMonitor = new ThroughputPassiveMonitor(node.pid, "client_overall", true);
        latMonitor = new LatencyPassiveMonitor(node.pid, "client_overall", true);
    }

    public void multicast(int[] destinations) {
        Message message = new Message(1, node.pid, ++msgId, destinations.length, destinations);

        List<Group> destinationGroups = new ArrayList<>();
        for (int id: destinations)
            destinationGroups.add(Group.getGroup(id));
        for (Group g: destinationGroups) {
            send(message, true, g.nodeList.get(0).pid);
        }
        logger.debug("sent message {} to its destinations {}", message, destinations);
    }

    public void multicast(int groupID) {
        int[] destinations = new int[1];
        destinations[0] = groupID;
        multicast(destinations);
    }

//    public Message deliverReply() {
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
//            Message m = tcpMessage.getContents();
//            logger.debug("received reply: " + m);
//            m.rewind();
//            receivedReply.put(m);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    public static void main(String[] args) {
        int clientId = Integer.parseInt(args[0]);
        String configFile = args[1];

        int threadCount = 1;
        int connections = 1;
        int recvQueue = 100;
        int sendQueue = 100;
        int loop = 100000;
        int maxinline = 0;
        int batchSize = 1;
        int clienttimeout = 3;
        int size = 24;
        String host = "192.168.4.1";
        int port = 9999;

        Client client = new Client(clientId, configFile);
        client.init(args);
        client.startRunning(sendQueue, recvQueue, maxinline, clientId);
        System.out.println("client " + client.node.pid + " started");

        int[] dests = new int[2];
        dests[0] = 0;
        dests[1] = 1;


//        logger.debug("sending message ...");
//        client.multicast(dests);
//        logger.debug("sending message ...");
//        client.multicast(dests);

        for (int i=0; i < 10; i++) {
            long sendTime = System.currentTimeMillis();
            client.multicast(dests);
            Message reply = client.deliverReply(dests[0]);
            logger.debug("reply: {}", reply);
            long recvTime = System.currentTimeMillis();
            client.tpMonitor.incrementCount();;
            client.latMonitor.logLatency(sendTime, recvTime);
        }
    }
}
