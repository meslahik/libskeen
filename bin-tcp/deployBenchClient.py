#!/usr/bin/python

import sys
import time
import common
import systemConfigurer
from common import iarg
from common import sarg


def usage():
    print "usage: " + sys.argv[0]
    sys.exit()


if (len(sys.argv) not in [1]):
    usage()


client_id = 1000
client_host = '192.168.3.32'

cmdArgs = [common.JAVA_BIN, common.JAVA_CLASSPATH]
cmdArgs += [common.LIBSKEEN_CLASS_BENCHCLIENT, str(client_id), common.SYSTEM_CONFIG_FILE]
cmdArgs += [common.SENSE_HOST, common.SENSE_PORT, common.SENSE_DIRECTORY, common.SENSE_DURATION, common.SENSE_WARMUP]

cmdString = ' '.join([str(val) for val in cmdArgs])
print cmdString

common.sshcmd(client_host, cmdString)


