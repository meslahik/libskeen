package ch.usi.dslab.mojtaba.libskeen;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPConnection;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Client extends Process {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Server.class);

    int msgId = 0;
    BlockingQueue<Message> receivedReply = new LinkedBlockingQueue<>();

    public Client(int id, String configFile) {
        super(id, false, configFile);
    }

    TCPConnection connect(int serverId) {
        Node node = Node.getNode(serverId);
        return connect(node);
    }

    void atomicMulticast(Message msg, List<Group> destinations) {
        Message newMsg = new Message(SkeenMessage.OP.Step1, msg, ++msgId, destinations);
        reliableMulticast(newMsg, destinations);
    }

    void multicast(Message message, List<Integer> groupIDs) {
        List<Group> destinations = new ArrayList<>();
        for (int id: groupIDs)
            destinations.add(Group.getGroup(id));
        for (Group g : destinations) {
            Node node = g.nodeList.get(0);
            send(message, node);
        }
    }

    void multicast(Message message, int groupID) {
        List<Integer> destinations = new ArrayList<>(1);
        destinations.add(groupID);
        multicast(message, destinations);
    }

    public Message deliverReply() {
        try {
            return receivedReply.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    void uponDelivery(Message m) {
        logger.debug("received reply: " + m);
        try {
            receivedReply.put(m);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int clientId = Integer.parseInt(args[0]);
        String configFile = args[1];
        Client client = new Client(clientId, configFile);

        Message msg = new Message("client message" + ++client.msgId, clientId);
        // sending TCPDestination, TCPSender creates a connection if there is no connection available
        // TCPSender keeps track of those connections in a Map<TCPDestination, TCPConnection>
        client.multicast(msg, 0);
        logger.debug("sent the message " + msg);

        Message msg2 = new Message("client message" + ++client.msgId, clientId);
        // sending TCPDestination, TCPSender creates a connection if there is no connection available
        // TCPSender keeps track of those connections in a Map<TCPDestination, TCPConnection>
        client.multicast(msg2, 1);
        logger.debug("sent the message " + msg);
    }
}
