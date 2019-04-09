#!/usr/bin/python
import json
import logging
import sys

import common

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

if len(sys.argv) != 1:
    print "usage: " + common.sarg(0)
    sys.exit(1)

config_file = common.SYSTEM_CONFIG_FILE

config_stream = open(config_file)
config = json.load(config_stream)


cmdList = []
for member in config["group_members"]:
    pid = member["pid"]
    group = member["group"]
    host = member["host"]
    port = member["port"]

    launchNodeCmdString = [common.JAVA_BIN, common.JAVA_CLASSPATH, '-DHOSTNAME=' + str(pid) + "-" + str(group)]
    launchNodeCmdString += [common.LIBSKEEN_CLASS_SERVER, pid, config_file]
    # launchNodeCmdString += ["true", common.NODES[1], common.SENSE_PORT, common.SENSE_DIRECTORY, common.SENSE_DURATION, common.SENSE_WARMUP]
    launchNodeCmdString = " ".join([str(val) for val in launchNodeCmdString])
    cmdList.append({"node": host, "port": port, "cmdstring": launchNodeCmdString})
    # print launchNodeCmdString

config_stream.close()
print(cmdList)

thread = common.LauncherThread(cmdList)
thread.start()
thread.join()
