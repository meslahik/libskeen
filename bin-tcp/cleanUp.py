#!/usr/bin/python

import sys
import common
import getpass

# if len(sys.argv) == 2:
#     allNodes = common.NODES[1:]
# else:
#     allNodes = common.NODES_CLIENTS

allNodes = common.NODES

user = getpass.getuser()
if user == 'meslahik':
    user = 'eslahm'

if common.LOCALHOST:
    common.localcmd("pkill -9 java")
else:
    for node in allNodes :
        common.sshcmdbg(node, "pkill -9 java")

print "finished"