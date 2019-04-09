#!/usr/bin/python

import sys
import time
import common
import systemConfigurer
from common import iarg
from common import sarg


def usage():
    print "usage: " + sys.argv[0] + " group_count numClients"
    sys.exit()


if (len(sys.argv) not in [3]):
    usage()

# parameters
gathererHost = common.SENSE_HOST
config_mode = sarg(1)
numClients = iarg(2)

logsargs = []

logsargs.append("throughput")
logsargs.append("client_overall")
logsargs.append(numClients)

logsargs.append("latency")
logsargs.append("client_overall")
logsargs.append(numClients)


directory = common.SENSE_DIRECTORY + "/skeentcp_" + config_mode + "g_" + str(numClients)


cmdArgs = [common.JAVA_BIN, common.JAVA_CLASSPATH, common.javaGathererClass,
           common.SENSE_PORT, directory] + logsargs
cmdString = ' '.join([str(val) for val in cmdArgs])
print cmdString

timetowait = common.SENSE_DURATION * 2

exitcode = common.sshcmd(gathererHost, cmdString, timetowait)
# if exitcode != 0:
#     common.localcmd("touch %s/FAILED.txt" % (directory))


