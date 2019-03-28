package ch.usi.dslab.mojtaba.libskeen.rdma;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Group {
    static Map<Integer, Group> groupList;

    static {
        groupList = new HashMap<>();
    }

    static Group getGroup(int id) {
        return groupList.get(id);
    }

    int id;
    ArrayList<Node> nodeList;

    public Group(int id) {
        this.id = id;
        nodeList = new ArrayList<>();
        groupList.put(id, this);
    }

    void addNode(Node node) {
        nodeList.add(node);
    }
}
