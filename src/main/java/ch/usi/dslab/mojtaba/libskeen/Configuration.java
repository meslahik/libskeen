package ch.usi.dslab.mojtaba.libskeen;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class Configuration {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Configuration.class);

    public static void loadConfig(String configFile){
        try {

            JSONParser parser = new JSONParser();
            Object nodeObj = parser.parse(new FileReader(configFile));
            JSONObject config = (JSONObject) nodeObj;

//            if (config.containsKey("batching_enabled")) {
//                boolean batchingEnabled = (Boolean) config.get("batching_enabled");
//                RequestBatcher.setEnabled(batchingEnabled);
//            }
//            if (config.containsKey("client_batch_size_threshold_bytes")) {
//                int clientBatchSizeThreshold = getJSInt(config, "client_batch_size_threshold_bytes");
//                RequestBatcher.setClientBatchSize_Bytes(clientBatchSizeThreshold);
//            }
//
//            if (config.containsKey("client_batch_time_threshold_ms")) {
//                int clientBatchTimeThreshold = getJSInt(config, "client_batch_time_threshold_ms");
//                RequestBatcher.setClientBatchTimeout_ms(clientBatchTimeThreshold);
//            }


            // ===========================================
            // Creating Nodes
            JSONArray groupMembersArray = (JSONArray) config.get("group_members");
            Iterator<Object> it_groupMember = groupMembersArray.iterator();
            while (it_groupMember.hasNext()) {
                JSONObject gmnode = (JSONObject) it_groupMember.next();

                int pid = getJSInt(gmnode, "pid");
                int gid = getJSInt(gmnode, "group");
                String host = (String) gmnode.get("host");
                int port = getJSInt(gmnode, "port");
                int replicaPort = getJSInt(gmnode, "replica_port");

                Group rgroup;
                boolean isleader = false;
                if (Group.getGroup(gid) == null) {
                    rgroup = new Group(gid);
                    isleader = true;
                }
                else
                    rgroup = Group.getGroup(gid);
                Node node = new Node(pid, true);
                node.setGroupId(gid);
                node.setAddress(host);
                node.setPort(port);
                node.setLeader(isleader);
                rgroup.addNode(node);
                Replica replica = new Replica(host, replicaPort, pid, gid);
                rgroup.addReplica(replica);
            }
            logger.debug("config file load successfully");

        } catch (IOException | ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    static int getJSInt(JSONObject jsobj, String fieldName) {
        return ((Long) jsobj.get(fieldName)).intValue();
    }
}
