package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.ramcast.RamcastFuture;
import ch.usi.dslab.lel.ramcast.RamcastReceiver;
import ch.usi.dslab.lel.ramcast.RamcastSender;
import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class Replica {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Replica.class);

    static HashMap<Integer, Replica> replicaMap;

    static {
        replicaMap = new HashMap<>();
    }

    boolean isGathererEnabled;
    ThroughputPassiveMonitor tpMonitor;
    LatencyPassiveMonitor latMonitor;

    int pid;
    int gid;
    String host;
    int port;

    int instanceNum = 0;
    int LC = 0; // TODO: needs to be Atomic?
    int lastDeliveredInstance = 0;

    Server server;
    RamcastReceiver agent;
    Map<Integer, RamcastSender> senders = new HashMap<>();

    Decide decideCallback = new Decide();
    DeliverCallback onDeliver;
    ConsensusMsgProcessor consensusMsgProcessor;

    Map<Pair<Integer, Integer>, ArrayList<Pending>> pendingMsgs = new ConcurrentHashMap<>();
    Map<Integer, ArrayList<Integer>> pendingMessages = new HashMap<>();
    TreeMap<Integer, SkeenMessage> pendingDeliverMessages = new TreeMap<>();
    Map<Pair<Integer, Integer>, RamcastServerEvent> waitingEvents = new ConcurrentHashMap<>();

//    Executor executor = Executors.newSingleThreadExecutor();
//    ConsensusMessage sendConsensusStep3Message = new ConsensusMessage();

    Replica(String host, int port, int pid, int gid) {
        this.host = host;
        this.port = port;
        this.pid = pid;
        this.gid = gid;
        replicaMap.put(pid, this);
        consensusMsgProcessor = new ConsensusMsgProcessor(this);

    }

    void setGatherer(boolean isGathererEnabled, String gathererHost, int gathererPort, String fileDirectory, int experimentDuration, int warmUpTime) {
        this.isGathererEnabled = isGathererEnabled;
        if (isGathererEnabled) {
            DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
            tpMonitor = new ThroughputPassiveMonitor(pid, "server", true);
            latMonitor = new LatencyPassiveMonitor(pid, "server", true);
        }
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

//        Thread senderThread = new Thread(this, "SenderThread-" + pid);
//        senderThread.start();
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

    Buffer send(ConsensusMessage msg, boolean expectReply, int nodeId) {
        return senders.get(nodeId).send(msg.getBuffer(), expectReply);
    }

    RamcastFuture sendNonBlocking(ConsensusMessage msg, boolean expectReply, int nodeId) {
        return senders.get(nodeId).sendNonBlocking(msg.getBuffer(), expectReply);
    }

    ByteBuffer deliverReply(RamcastFuture future) {
        try {
            return future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    void deliverReply(Set<RamcastFuture> futureSet) {
        try {
            for (RamcastFuture future : futureSet)
                future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getAddress() {
        return host;
    }

    public int getPort() {
        return port;
    }

    void setOnDeliver(DeliverCallback onDeliver) {
        this.onDeliver = onDeliver;
    }

    void setServer(Server server) {
        this.server = server;
    }

    public String toString() {
        return Integer.toString(pid);
    }


    public void sendConsensusStep1(SkeenMessage skeenMessage) {
        ConsensusMessage consensusMessage = new ConsensusMessage(1, ++instanceNum, skeenMessage);
        ArrayList<Replica> replicaList = Group.getGroup(gid).replicaList;

        // a fake message for the vote from this replica
        ConsensusMessage fakeMessage = new ConsensusMessage(2, instanceNum, skeenMessage, pid);
        processConsensusStep2Message(fakeMessage);

        logger.debug("replica {} is sending to replicas {} message {}", pid, replicaList, consensusMessage);
        Set<RamcastFuture> deliverFutures = new HashSet<>();
        for (Replica replica : replicaList) {
            if (replica.pid == pid)
                continue;
            RamcastFuture future = sendNonBlocking(consensusMessage, true, replica.pid);
            deliverFutures.add(future);
        }
        logger.debug("replica {} is waiting for ACKs from replicas {}", pid, replicaList);
        for (RamcastFuture future : deliverFutures) {
            ByteBuffer buffer = deliverReply(future);
            ConsensusMessage message = new ConsensusMessage();
            message.update(buffer);
            processConsensusStep2Message(message);
        }
    }

    void sendConsensusStep3(ConsensusMessage consensusMessage) {
        ArrayList<Replica> replicaList = Group.getGroup(gid).replicaList;
        logger.debug("replica {} is sending to replicas {} MESSAGE {}", pid, replicaList, consensusMessage);
        Set<RamcastFuture> deliverFutures = new HashSet<>();
        for (Replica replica : replicaList) {
            RamcastFuture future = sendNonBlocking(consensusMessage, true, replica.pid);
            deliverFutures.add(future);
        }
        logger.debug("replica {} is waiting for ACKs from replicas {}", pid, replicaList);
        deliverReply(deliverFutures);
        logger.debug("replica {} sending finished", pid);
    }

    void processConsensusStep2Message(ConsensusMessage wrapperMessage) {
        logger.debug("process message {}", wrapperMessage);

        int msgInstanceNum = wrapperMessage.getInstanceId();
        if (lastDeliveredInstance >= msgInstanceNum) {
            logger.debug("ignore message {}", wrapperMessage);
            return;
        }

        SkeenMessage message = wrapperMessage.getSkeenMessage();
        int replicaId = wrapperMessage.getReplicaId();

        if (pendingMessages.containsKey(msgInstanceNum))
            pendingMessages.get(msgInstanceNum).add(replicaId);
        else {
            ArrayList<Integer> array = new ArrayList<>();
            array.add(replicaId);
            pendingMessages.put(msgInstanceNum, array);
        }

        ArrayList<Integer> arrayList = pendingMessages.get(msgInstanceNum);
        logger.debug("put into pending messages for decision delivery. received {} votes for decision instance {}.",
                arrayList.size(), msgInstanceNum);

        if (arrayList.size() > Group.getGroup(gid).replicaList.size() / 2) {
            pendingDeliverMessages.put(msgInstanceNum, message);
        }

        while(pendingDeliverMessages.size() != 0) {
            if (pendingDeliverMessages.firstEntry().getKey() == lastDeliveredInstance + 1) {
                Map.Entry<Integer, SkeenMessage> entry = pendingDeliverMessages.pollFirstEntry();
                SkeenMessage deliverMessage = entry.getValue();
                pendingMessages.remove(entry.getKey());
                lastDeliveredInstance++;

                // send message to other replicas
                ConsensusMessage learnMessage = new ConsensusMessage(3, lastDeliveredInstance, deliverMessage);
                sendConsensusStep3(learnMessage);
//                sendConsensusStep3Message = new ConsensusMessage(3, lastDeliveredInstance, deliverMessage);
//                executor.execute(this);
            } else break;
        }
    }

    class Decide implements DecideCallback {
        private TreeMap<Integer, Pending> ordered = new TreeMap<>();

        void processStep1Message(SkeenMessage skeenMessage) {
            int clientId = skeenMessage.getClientId();
            int messageId = skeenMessage.getMsgId();
            int destinationSize = skeenMessage.getDestinationSize();
            int[] destinations = skeenMessage.getDestinations();

            LC++;

            if (server.node.isLeader) {
                SkeenMessage newSkeenMessage = new SkeenMessage(2, clientId, messageId, destinationSize, destinations,
                        server.node.pid, LC);

                List<Group> destinationGroups = new ArrayList<>();
                for (int id : destinations)
                    destinationGroups.add(Group.getGroup(id));
                Set<RamcastFuture> deliverFutures = new HashSet<>();
                for (Group g : destinationGroups) {
                    Node gLeader = g.nodeList.get(0);
                    RamcastFuture future = server.sendNonBlocking(newSkeenMessage, true, gLeader.pid);
                    deliverFutures.add(future);
                    logger.debug("sent skeenMessage {} to server {}", newSkeenMessage, gLeader);
                }
                server.deliverReply(deliverFutures);
            }
        }

        private void processMaxLCMessages(SkeenMessage skeenMessage) {
            int clientId = skeenMessage.getClientId();
            int messageId = skeenMessage.getMsgId();
            int destinationSize = skeenMessage.getDestinationSize();
            int[] destinations = skeenMessage.getDestinations();
            int nodeId = skeenMessage.getNodeId();
            int maxLC = skeenMessage.getLC();

            Pending p = new Pending(clientId, messageId, nodeId, maxLC, destinationSize, destinations, skeenMessage);

            ordered.put(maxLC, p);
            logger.debug("replica {} message {}-{}:{} is put in ordered queue to be delivered", pid, maxLC, p.clientId, p.msgId);

            while (ordered.size() > 0) {
                Map.Entry<Integer, Pending> minOrdered = ordered.firstEntry();
                Integer minOrderedLC = minOrdered.getKey();
                Pending pending = minOrdered.getValue();

                boolean flag = true;
                Collection<ArrayList<Pending>> arrs = pendingMsgs.values();
                Iterator<ArrayList<Pending>> it = arrs.iterator();
                while (it.hasNext()) {
                    ArrayList<Pending> arr2 = it.next();
                    for (int i = 0; i < arr2.size(); i++) {
                        Pending pending2 = arr2.get(i);
                        if (pending2.LC < minOrderedLC) {
                            flag = false;
                            break;
                        }
                    }
                }

                if (flag) {
                    ordered.pollFirstEntry();
                    Pair<Integer, Integer> deliverPair = new Pair<>(pending.clientId, pending.msgId);
//                server.addDeliveredMessage(deliverPair);
                    onDeliver.call(deliverPair);
                    logger.debug("replica {} atomic deliver message {}-{}:{}", pid, minOrderedLC, pending.clientId, pending.msgId);

                    if (isGathererEnabled) {
                        tpMonitor.incrementCount();
                        latMonitor.logLatency(pending.sendTime, System.currentTimeMillis());
                    }

                    SkeenMessage reply = new SkeenMessage(3, pending.clientId, pending.msgId);
                    RamcastServerEvent event = waitingEvents.get(deliverPair);
                    if (event != null) {
                        event.setSendBuffer(reply.getBuffer());
                        try {
                            event.triggerResponse();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        logger.debug("replica {} reply sent to the client", pid);
                        waitingEvents.remove(deliverPair);
                    }
                } else {
                    break;
                }
            }
        }

        @Override
        public void call(SkeenMessage skeenMessage) {
            int type = skeenMessage.getMsgType();
            switch (type) {
                case 1:
                    logger.debug("replica {} delivered Skeen STEP1 message {}",pid, skeenMessage);
                    processStep1Message(skeenMessage);
                    break;
                case 2:
                    logger.debug("replica {} delivered Skeen MaxLC message {}", pid, skeenMessage);
                    processMaxLCMessages(skeenMessage);
            }
        }
    }

//    @Override
//    public void run() {
//        ConsensusMessage msg = sendConsensusStep3Message;
//        ArrayList<Replica> replicaList = Group.getGroup(gid).replicaList;
//        logger.debug("replica {} is sending to replicas {} MESSAGE {}", pid, replicaList, msg);
//        Set<RamcastFuture> deliverFutures = new HashSet<>();
//        for (Replica replica : replicaList) {
//            RamcastFuture future = sendNonBlocking(msg, true, replica.pid);
//            deliverFutures.add(future);
//        }
//        logger.debug("replica {} is waiting for ACKs from replicas {}", pid, replicaList);
//        deliverReply(deliverFutures);
//        logger.debug("replica {} sending finished", pid);
//
////        while (true) {
////            try {
////                ConsensusMessage consensusMessage = sendingMessages.take();
////                ArrayList<Replica> replicaList = Group.getGroup(gid).replicaList;
////                logger.debug("replica {} is sending to replicas {} MESSAGE {}", pid, replicaList, consensusMessage);
////                Set<RamcastFuture> deliverFutures = new HashSet<>();
////                for (Replica replica : replicaList) {
////                    RamcastFuture future = sendNonBlocking(consensusMessage, true, replica.pid);
////                    deliverFutures.add(future);
////                }
////                logger.debug("replica {} is waiting for ACKs from replicas {}", pid, replicaList);
////                deliverReply(deliverFutures);
////                logger.debug("replica {} sending finished", pid);
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
////        }
//    }
}
