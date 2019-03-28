package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.RamcastReceiver;

import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Server extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Server.class);

    // TODO needs to be Atomic?
    int LC = 0;

    RamcastReceiver agent;
    MessageProcessor messageProcessor;
    public BlockingQueue<Pair<Integer, Integer>> atomicDeliver = new LinkedBlockingQueue<>();

    public Server(int id, String configFile,
                  int poolsize, int recvQueue, int sendQueue, int wqSize, int servicetimeout,
                  boolean polling, int maxinline) {
        super(id, true, configFile);

        System.out.println("running...server " + node.host + ", poolsize " + poolsize + ", maxinline " + maxinline +
                ", polling " + polling + ", recvQueue " + recvQueue + ", sendQueue " + sendQueue + ", wqSize " + wqSize +
                ", rpcservice-timeout " + servicetimeout);
        messageProcessor = new MessageProcessor(this);
        agent = new RamcastReceiver<Message>(node.host, node.port, sendQueue, recvQueue, maxinline, servicetimeout, polling, wqSize,
                Message::new, messageProcessor);
    }

    Pair<Integer, Integer> atomicDeliver() {
        try {
            return atomicDeliver.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

//    @Override
//    void uponDelivery(TCPMessage tcpMessage) {
//
//    }

    public static void main(String[] args) {
        int serverId = Integer.parseInt(args[0]);
        String configFile = args[1];

        int poolsize = 1;
        int recvQueue = 100;
        int sendQueue = 100;
        int wqSize = recvQueue;
        int servicetimeout = 1;
        boolean polling = false;
        int maxinline = 0;


        Server server = new Server(serverId, configFile,
                poolsize, recvQueue, sendQueue, wqSize, servicetimeout, polling, maxinline);
        server.startRunning(sendQueue, recvQueue, maxinline, servicetimeout);
    }
}
