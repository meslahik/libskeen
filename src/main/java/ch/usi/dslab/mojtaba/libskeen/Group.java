package ch.usi.dslab.mojtaba.libskeen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Group {
    static Map<Integer, Group> groupList;

    static {
        groupList = new HashMap<>();
    }

    static Group getGroup(int id) {
        return groupList.get(id);
    }

    public static int groupSize() {return groupList.size(); }

    public static Set<Integer> groupIDs() {return groupList.keySet(); }

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
