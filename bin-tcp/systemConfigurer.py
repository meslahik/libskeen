#!/usr/bin/python

# try: import simplejson as json
# except ImportError: import json

import json
import math
import sys

import common
from common import get_item


def generateJMcastConfiguration(nodes_available, numPartitions, numOracles, replicasPerPartition,
                                configFilePath,
                                saveToFile, serverPerNode=1):
    config = dict()
    config["agent_class"] = "JMcastAgent"
    config["group_number"] = numPartitions
    config["replicas_per_group"] = replicasPerPartition
    config["rmcast_agent_class"] = "SimpleReliableMulticastAgent"
    
    node_index = 0
    count = 0
    pid = 0
    config["group_members"] = []
    for p in range(numPartitions):
        for i_p in range(replicasPerPartition):
            config["group_members"].append({
                "pid": pid,
                "gpid": i_p,
                "group": p,
                "host": nodes_available[node_index],
                "port": 50000 + pid,
                "rmcast_address": nodes_available[node_index],
                "rmcast_port": 56000 + pid
            })

            count += 1
            pid += 1
            if count >= serverPerNode:
                node_index += 1
                count = 0
    if saveToFile:
        systemConfigurationFile = open(configFilePath, 'w')
        json.dump(config, systemConfigurationFile, sort_keys=False, indent=4, ensure_ascii=False)
        systemConfigurationFile.flush()
        systemConfigurationFile.close()

        # save mcast config
        records = []
        for node in config["group_members"]:
            records.append(
                "node " + str(node["group"]) + " " + str(node["gpid"]) + " " + node["host"] + " " + str(node["port"]))

    # print config
    remainingNodes = nodes_available[node_index + 1:]
    minClientId = pid + 1
    return {
        "config_file": configFilePath,
        # "coordinator_list": helperList["coordinator"],
        # "acceptor_list": helperList["acceptor"],
        # "server_list": serverList,
        "client_initial_pid": 10001,
        "remaining_nodes": remainingNodes
    }


def generateSystemConfigurationForJMcast(numPartitions, numOracles, saveToFile=True):
    replicasPerPartition = common.replicasPerPartition
    sysConfigFilePath = common.SYSTEM_CONFIG_DIR + "/" + str(numPartitions) + "g_system_config.json"
    # partitionsFilePath = common.PARTITION_CONFIG_FILE
    numOracles = numOracles
    availableNodes = common.NODES

    systemConfiguration = generateJMcastConfiguration(availableNodes, numPartitions, numOracles,
                                                      replicasPerPartition,
                                                      sysConfigFilePath, saveToFile,
                                                      common.serverPerNode)

    # systemConfiguration["screen_node"] = screenNode
    # systemConfiguration["gatherer_node"] = gathererNode
    # systemConfiguration["partitioning_file"] = partitionsFilePath
    return systemConfiguration


if __name__ == '__main__':
    # usage
    def usage():
        print "usage: <partitions_num>"
        sys.exit(1)


    numPartitions = common.iarg(1)
    numOracle = 1
    generateSystemConfigurationForJMcast(numPartitions, numOracle, True)
