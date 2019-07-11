package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.lel.ramcast.ServerEventCallback;
import ch.usi.dslab.lel.ramcast.RamcastServerEvent;
import javafx.util.Pair;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class SkeenMessageProcessor1 implements ServerEventCallback {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SkeenMessageProcessor1.class);

    Replica replica;

    SkeenMessageProcessor1(Replica replica) {
        this.replica = replica;
    }

    void processSkeenStep1Message(RamcastServerEvent event, SkeenMessage skeenMessage) {
        logger.debug("replica {} received Skeen STEP1 message {}", replica.pid, skeenMessage);
        int clientId = skeenMessage.getClientId();
        int messageId = skeenMessage.getMsgId();

//                SkeenMessage reply = new SkeenMessage(3, clientId, messageId);
//                event.setSendBuffer(reply.getBuffer());
//                try {
//                    event.triggerResponse();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }

        Pair<Integer, Integer> pair = new Pair<>(clientId, messageId);
        replica.waitingEvents.put(pair, event);
        replica.propose(skeenMessage);
    }

    void processSkeenStep2Message(RamcastServerEvent event, SkeenMessage skeenMessage) {
        int clientId = skeenMessage.getClientId();
        int messageId = skeenMessage.getMsgId();
        int destinationSize = skeenMessage.getDestinationSize();
        int[] destinations = skeenMessage.getDestinations();
        int nodeId = skeenMessage.getNodeId();
        int lc = skeenMessage.getLC();

        // reply is needed to be sent back to the server (the leader) that sent this skeenMessage
        SkeenMessage reply = new SkeenMessage(3, clientId, messageId);
        event.setSendBuffer(reply.getBuffer());
        try {
            event.triggerResponse();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("ACK skeenMessage sent back to the server {}", nodeId);



        logger.debug("replica {} received Skeen STEP2 message {} from {}", replica.pid, skeenMessage, nodeId);
        Pending p = new Pending(clientId, messageId, nodeId, lc, destinationSize, destinations, skeenMessage);
        replica.LC.set(Math.max(replica.LC.get(), p.LC));
        Pair<Integer, Integer> pair = new Pair<>(p.clientId, p.msgId);
        ArrayList<Pending> arr;
        if (replica.pendingMsgs.containsKey(pair)) {
            arr = replica.pendingMsgs.get(pair);
            arr.add(p);
        }
        else {
            arr = new ArrayList<>();
            arr.add(p);
            replica.pendingMsgs.put(pair, arr);
        }
        logger.debug("added {} to pending messages", p);

        // process pending messages
        if (arr.size() < p.destinationSize) {
            logger.debug("should wait for server votes for pair {}. received {} out of {} votes",
                    pair, arr.size(), p.destinationSize);
            return;
        }

        int maxLC = -1;
        for (int i = 0; i < arr.size(); i++) {
            int LC = arr.get(i).LC;
            maxLC = maxLC < LC ? LC : maxLC;
        }
//        arr.stream().map(x -> x.LC).max(Integer::compareTo).get();
        replica.pendingMsgs.remove(pair);
        logger.debug("have enough votes for pair {}. lc {} is chosen for the message {}:{}",
                pair, maxLC, p.clientId, p.msgId);

        SkeenMessage newSkeenMessage = new SkeenMessage(2, clientId, messageId, destinationSize, destinations,
                replica.pid, maxLC);
        replica.propose(newSkeenMessage);
    }

    // only leader processes receive these messages. clients send skeen Step1 messages to leaders
    @Override
    public void call(RamcastServerEvent event) {
        ByteBuffer buffer = event.getReceiveBuffer();
        SkeenMessage m = new SkeenMessage();
        m.update(buffer);

        int type = m.getMsgType();
        switch (type) {
            case 1:
                processSkeenStep1Message(event, m);
                break;
            case 2:
                processSkeenStep2Message(event, m);
                break;
        }
    }
}
