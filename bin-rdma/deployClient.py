#!/usr/bin/python

import sys
import time
import common
import systemConfigurer
from common import iarg
from common import sarg


def usage():
    print "usage: " + sys.argv[0] + " <group_count>"
    sys.exit()


if (len(sys.argv) not in [2]):
    usage()


client_id = 1000
client_host = '192.168.3.8'

group_count = common.iarg(1)
config_file = common.SYSTEM_CONFIG_DIR + "/" + str(group_count) + "g_system_config.json"

cmdArgs = [common.JAVA_BIN, common.JAVA_CLASSPATH]
cmdArgs += [common.LIBSKEEN_RDMA_CLASS_CLIENT, str(client_id), config_file]
cmdArgs += [common.SENSE_HOST, common.SENSE_PORT, common.SENSE_DIRECTORY, common.SENSE_DURATION, common.SENSE_WARMUP]

cmdString = ' '.join([str(val) for val in cmdArgs])
print cmdString

common.sshcmd(client_host, cmdString)


