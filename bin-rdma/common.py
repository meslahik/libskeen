import inspect
import json
import logging
import os
import re
import shlex
import socket
import subprocess
import sys
import threading

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
# logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        def target():
            logging.debug('Thread started')
            run_args = shlex.split(self.cmd)
            self.process = subprocess.Popen(run_args)
            self.process.communicate()
            logging.debug('Thread finished')

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            logging.debug('Terminating process')
            self.process.terminate()
            thread.join()
        return self.process.returncode


class LauncherThread(threading.Thread):
    def __init__(self, clist):
        threading.Thread.__init__(self)
        self.cmdList = clist

    def run(self):
        for cmd in self.cmdList:
            # logging.debug("Executing: %s", cmd["cmdstring"])
            sshcmdbg(cmd["node"], cmd["cmdstring"])


def script_dir():
    return os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda _: None)))


def sshcmd(node, cmdstring, timeout=None):
    finalstring = "ssh -o StrictHostKeyChecking=no " + node + " \"" + cmdstring + "\""
    logging.debug(finalstring)
    cmd = Command(finalstring)
    return cmd.run(timeout)


def localcmd(cmdstring, timeout=None):
    logging.debug("localcmd:%s", cmdstring)
    cmd = Command(cmdstring)
    return cmd.run(timeout)


def sshcmdbg(node, cmdstring):
    cmd = "ssh -o StrictHostKeyChecking=no " + node + " \"" + cmdstring + "\" &"
    logging.debug("sshcmdbg: %s", cmd)
    os.system(cmd)


def localcmdbg(cmdstring):
    logging.debug("localcmdbg: %s", cmdstring)
    os.system(cmdstring + " &")


def get_item(lst, key, value):
    index = get_index(lst, key, value)
    if index == -1:
        return None
    else:
        pass
    return lst[index]


def get_index(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
    return -1


def get_system_config_file(config_type):
    if config_type is None:
        return {'partitioning': SYSTEM_CONFIG_DIR + '/partitioning.json',
                'system_config': SYSTEM_CONFIG_DIR + '/system_config.json'}
    partitioning_file = SYSTEM_CONFIG_DIR + '/' + config_type + '_partitioning.json'
    system_config_file = SYSTEM_CONFIG_DIR + '/' + config_type + '_system_config.json'
    if not os.path.isfile(partitioning_file):
        logging.error('ERROR: parititoning file not found: %s', partitioning_file)
        sys.exit(1)
    if not os.path.isfile(system_config_file):
        logging.error('ERROR: system config file not found: %s', system_config_file)
        sys.exit(1)
    return {'partitioning': partitioning_file,
            'system_config': system_config_file}


def read_json_file(file_name):
    file_stream = open(file_name)
    content = json.load(file_stream)
    file_stream.close()
    return content


def sarg(i):
    return sys.argv[i]


def iarg(i):
    return int(sarg(i))


def farg(i):
    return float(sarg(i))


def getScreenNode():
    return NODES[0]


def getNonScreenNodes():
    return NODES[1:]


NODE = 0
CLIENTS = 1


def mapClientsToNodes(numClients, nodesList):
    # clientMap is a list of dicts
    # clientMap = [{NODE: x, CLIENTS: y}, {NODE: z, CLIENTS: w}]
    clientMap = []
    clientsPerNode = int(numClients / len(nodesList))
    for node in nodesList:
        clientMap.append({NODE: node, CLIENTS: clientsPerNode})
    for extra in range(numClients % len(nodesList)):
        clientMap[extra][CLIENTS] += 1
    return clientMap


def numUsedClientNodes(arg1, arg2=None):
    if arg2 is None:
        return numUsedClientNodes_1(arg1)
    elif arg2 is not None:
        return numUsedClientNodes_2(arg1, arg2)


def numUsedClientNodes_2(numClients, clientNodes):
    return min(numClients, len(clientNodes))


def numUsedClientNodes_1(clientNodesMap):
    numUsed = 0
    for mapping in clientNodesMap:
        if mapping[CLIENTS] > 0:
            numUsed += 1
    return numUsed


DEBUGGING = False

LOCALHOST = False
LOCALHOST_NODES = []
for i in range(1, 50): LOCALHOST_NODES.append("127.0.0.1")


# available machines
DEAD_NODES = [40]

def noderange(first, last):
    return ["192.168.3." + str(val) for val in [node for node in range(first, last + 1) if node not in DEAD_NODES]]

def noderange2(first, last):
    return ["192.168.4." + str(val) for val in [node for node in range(first, last + 1) if node not in DEAD_NODES]]

NODES = LOCALHOST_NODES if LOCALHOST else noderange(1, 10)
NODES_RDMA = LOCALHOST_NODES if LOCALHOST else noderange2(1, 8)
# NODES_CLIENTS = noderange(20, 40)

# PARTITION CONFIG
replicasPerPartition = 1
serverPerNode = 1


# parameters
HOME = '/'.join(os.path.dirname(os.path.abspath(__file__)).split('/')[:-1])

GLOBAL_HOME = os.path.normpath(script_dir() + '/../../')

BIN_HOME = os.path.normpath(GLOBAL_HOME + '/libskeen/bin-tcp')

DYNASTAR_HOME = os.path.normpath(GLOBAL_HOME + '/dynastarv2')
DYNASTAR_CP = os.path.normpath(DYNASTAR_HOME + '/target/classes')

LIBSKEEN_RDMA_CLASS_SERVER = 'ch.usi.dslab.mojtaba.libskeen.rdma.Server'
LIBSKEEN_RDMA_CLASS_CLIENT = 'ch.usi.dslab.mojtaba.libskeen.rdma.Client'

LIBMCAD_HOME = os.path.normpath(GLOBAL_HOME + '/libmcad')
LIBMCAD_CP = os.path.normpath(LIBMCAD_HOME + '/target/classes')
LIBMCAD_CLASS_RIDGE = 'ch.usi.dslab.bezerra.mcad.ridge.RidgeEnsembleNode'

NETWRAPPER_HOME = os.path.normpath(GLOBAL_HOME + '/netwrapper')
NETWRAPPER_CP = os.path.normpath(NETWRAPPER_HOME + '/target/classes')

RIDGE_HOME = os.path.normpath(GLOBAL_HOME + '/ridge')
RIDGE_CP = os.path.normpath(RIDGE_HOME + '/target/classes')

LIBSKEEN_HOME = os.path.normpath(GLOBAL_HOME + '/libskeen')
LIBSKEEN_CP = os.path.normpath(LIBSKEEN_HOME + '/target/classes')
LIBSKEEN_DEPENDENCIES = os.path.normpath(LIBSKEEN_HOME + '/target/dependency/*')

DISNI_HOME = os.path.normpath(GLOBAL_HOME + '/disni')
DISNI_CP = os.path.normpath(LIBSKEEN_HOME + '/target/classes')
DISNI_DEPENDENCIES = os.path.normpath(LIBSKEEN_HOME + '/target/dependency/*')

LIBRAMCAST_HOME = os.path.normpath(GLOBAL_HOME + '/libramcast')
LIBRAMCAST_CP = os.path.normpath(LIBRAMCAST_HOME + '/target/classes')
LIBRAMCAST_DEPENDENCIES = os.path.normpath(LIBRAMCAST_HOME + '/target/dependency/*')

SENSE_HOME = os.path.normpath(GLOBAL_HOME + '/sense')
SENSE_CP = os.path.normpath(SENSE_HOME + '/target/classes')
SENSE_DEPENDENCIES = os.path.normpath(SENSE_HOME + '/target/dependency/*')

SENSE_ENABLED = 'true'
SENSE_HOST = '192.168.3.9'
SENSE_PORT = 60000
SENSE_DIRECTORY = '/home/eslahm/logs/libskeen-rdma-messaging/data'
SENSE_DURATION = 60
SENSE_WARMUP = 10

SYSTEM_CONFIG_DIR = os.path.normpath(script_dir() + '/systemConfigs')
SYSTEM_CONFIG_FILE = SYSTEM_CONFIG_DIR + "/8g_system_config.json"

_class_path = [DISNI_CP, DISNI_DEPENDENCIES, LIBSKEEN_CP, LIBSKEEN_DEPENDENCIES, SENSE_CP, SENSE_DEPENDENCIES, LIBRAMCAST_CP, LIBRAMCAST_DEPENDENCIES]

JAVA_BIN = 'java -Dlog4j.configuration=file:' + script_dir() + '/log4jDebug.xml'
JAVA_CLASSPATH = '-cp \'' + ':'.join([str(val) for val in _class_path]) + "\'"


# SCRIPTS
clockSynchronizer = HOME + "/bin/clockSynchronizer.py"
continousClockSynchronizer = HOME + "/bin/continuousClockSynchronizer.py"
chirperServerDeployer = HOME + "/bin/deployServer.py"
chirperOracleDeployer = HOME + "/bin/deployOracle.py"
chirperClientDeployer = HOME + "/bin/deployTestRunners.py"
chirperClientDynamicDeployer = HOME + "/bin/deployTestRunnerActors.py"
chirperAllInOne = HOME + "/bin/runAllOnce.py"
cleaner = HOME + "/bin/cleanUp.py"
benchCommonPath = HOME + "/bin/common.py"
runBatchPath = HOME + "/bin/runBatch.py"
multicastDeployer = HOME + "/bin/deployMcast.py"

# MONITORING
gathererDeployer = HOME + "/bin/deployGatherer.py"
javaGathererClass = "ch.usi.dslab.bezerra.sense.DataGatherer"
javaBWMonitorClass = "ch.usi.dslab.bezerra.sense.monitors.BWMonitor"
javaCPUMonitorClass = "ch.usi.dslab.bezerra.sense.monitors.CPUEmbededMonitorJavaMXBean"
javaCPUMonitorClass = "ch.usi.dslab.bezerra.sense.monitors.CPUMonitorMPStat"
javaMemoryMonitorClass = "ch.usi.dslab.bezerra.sense.monitors.MemoryMonitor"


