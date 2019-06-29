#!/usr/bin/python

import sys
import threading

import common
from common import sshcmd

lock = threading.Lock()


class clockSyncThread(threading.Thread):
    def __init__(self, target, *args):
        self._target = target
        self._args = args
        threading.Thread.__init__(self)

    def run(self):
        self._target(*self._args)


#####################################################

numQueries = 3
ntpServer = "node249"


# ntpServer  = "swisstime.ethz.ch"
# ntpServer  = "0.ch.pool.ntp.org"

def printLocked(st):
    lock.acquire()
    print st
    lock.release()


def synchNode(node):
    printLocked("Synchronizing clock of " + node + "...")
    sshcmd(node, "systemctl stop ntpd.service")
    for _ in range(numQueries):
        sshcmd(node, "sudo ntpdate -b " + ntpServer)
    printLocked("Done with " + node)


#     sshcmd(node, "sudo service ntp start")

nodesToSync = common.NODES

threads = []

if len(sys.argv) > 1:
    numQueries = int(sys.argv[1])

for node in nodesToSync:
    t = clockSyncThread(synchNode, node)
    threads.append(t)
    t.start()

for t in threads:
    t.join()
