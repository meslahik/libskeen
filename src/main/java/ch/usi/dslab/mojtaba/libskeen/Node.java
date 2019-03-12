package ch.usi.dslab.mojtaba.libskeen;

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

    @Override
    public String getAddress() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }
}
