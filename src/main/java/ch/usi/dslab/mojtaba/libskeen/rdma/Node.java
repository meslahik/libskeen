package ch.usi.dslab.mojtaba.libskeen.rdma;

import ch.usi.dslab.bezerra.netwrapper.tcp.TCPDestination;

import java.util.HashMap;

public class Node implements TCPDestination {

    static HashMap<Integer, Node> nodeMap;

    static {
        nodeMap = new HashMap<>();

    }

    public static Node getNode(int id) {
        return nodeMap.get(id);
    }

    int pid;
    int gid;

    String host;
    int port;

    boolean isLeader;

    public Node (int pid, boolean isServer) {
        this.pid = pid;
        if (isServer)
            nodeMap.put(pid, this);
    }

    void setGroupId (int gid) {
        this.gid = gid;
    }

    void setAddress(String host) {
        this.host = host;
    }

    void setPort(int port) {
        this.port = port;
    }

    void setLeader (boolean isLeader) {
        this.isLeader = isLeader;
    }

    @Override
    public String getAddress() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "[node: " + pid + "]";
    }
}
