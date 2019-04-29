package ch.usi.dslab.mojtaba.libskeen.bench;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.mojtaba.libskeen.Client;
import ch.usi.dslab.mojtaba.libskeen.Group;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class BenchClient {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(BenchClient.class);

    private ThroughputPassiveMonitor tpMonitor;
    private LatencyPassiveMonitor latMonitor;

    Client client;

    public BenchClient(int clientId, String configFile) {
        client = new Client(clientId, configFile);
    }

    void launch(String[] args) {
        String gathererHost = args[2];
        int gathererPort = Integer.parseInt(args[3]);
        String fileDirectory = args[4];
        int experimentDuration = Integer.parseInt(args[5]);
        int warmUpTime = Integer.parseInt(args[6]);

        DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);

        tpMonitor = new ThroughputPassiveMonitor(client.node.getPid(), "client_overall", true);
        latMonitor = new LatencyPassiveMonitor(client.node.getPid(), "client_overall", true);


        System.out.println("group size: " + Group.groupSize());
        System.out.println("group size: " + Group.groupIDs());
        ArrayList<Integer> dests = new ArrayList<>(Group.groupSize());
        dests.addAll(Group.groupIDs());

        System.out.println("client " + client.node.getPid() + " started");
        Message message = new Message("client message");
        for (int i=0; i < 10000; i++) {
            long sendTime = System.currentTimeMillis();

            client.multicast(message, dests);
            Message reply = client.deliverReply();
            logger.debug("reply: {}", reply);
            long recvTime = System.currentTimeMillis();
            tpMonitor.incrementCount();
            latMonitor.logLatency(sendTime, recvTime);
        }
        System.out.println("finished");
    }

    public static void main(String[] args) {
        int clientId = Integer.parseInt(args[0]);
        String configFile = args[1];
        BenchClient c = new BenchClient(clientId, configFile);
        c.launch(args);
    }
}
