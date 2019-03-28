#!/usr/bin/python

import getpass
import inspect
import os
import sys


def script_dir():
    return os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda _: None)))


if len(sys.argv) not in [1, 2]:
    print "Incorrect usage: Use " + sys.argv[0] + " mode <diff, deploy>"

if len(sys.argv) == 1:
    mode = "deploy"
else:
    mode = sys.argv[1]

IGNORE_FILE = script_dir() + '/.clusterIgnore'

GLOBAL_HOME = '/Users/meslahik/PhD/Projects/RDMAPaxos/code/AtomicMulticast'

TARGET_NODE = 'eslahm@dslab.inf.usi.ch'  # head node
TARGET_HOME = '/home/eslahm/AtomicMulticast'

CMD_CREATE_DIR = ["ssh -p 9022", TARGET_NODE, "'mkdir -p", TARGET_HOME, "'"]
CMD_CREATE_DIR = ' '.join([str(val) for val in CMD_CREATE_DIR])

if mode == "deploy":
    CMD_COPY_BULD = ["rsync", "-rav", "--delete", "--exclude-from='" + IGNORE_FILE + "'",
                     "-e 'ssh -p 9022'", GLOBAL_HOME + "/*", TARGET_NODE + ":" + TARGET_HOME]
else:
    CMD_COPY_BULD = ["rsync", "-azh", "--dry-run", "--delete-after",
                     "--exclude-from='" + IGNORE_FILE + "'",
                     '--out-format="[%t]:%o:%f:Last Modified %M"',
                     "-e 'ssh -p 9022'", GLOBAL_HOME + "/*", TARGET_NODE + ":" + TARGET_HOME]
CMD_COPY_BULD = ' '.join([str(val) for val in CMD_COPY_BULD])

print 'Running mode: ' + mode
print CMD_CREATE_DIR
print CMD_COPY_BULD
os.system(CMD_CREATE_DIR)
os.system(CMD_COPY_BULD)
