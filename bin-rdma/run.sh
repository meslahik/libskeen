#!/bin/bash

name="libskeen"

PREFIX=""
SUFFIX=""

for var in "$@"; do
  if [ "$var" = "-d" ]; then
    PREFIX="valgrind "
  elif [ "$var" = "-v" ]; then
    SUFFIX=" -v > /tmp/log\${RANDOM}.txt"
  else
    name="$var"
  fi
done

echo "Running $name..."
sleep 1

tmux new-session -d -s libskeen
tmux new-window -t libskeen
tmux selectp -t 0
tmux split -h
tmux selectp -t 0
tmux split
# tmux selectp -t 0
tmux split
#tmux selectp -t 2
#tmux split -p 70
tmux selectp -t 3
tmux split
tmux selectp -t 3
tmux split
tmux selectp -t 5
tmux split
tmux selectp -t 0
# tmux select-layout even-vertical
# tmux split

JAVA="java -Dlog4j.configuration=file:/home/eslahm/AtomicMulticast/libskeen/bin-rdma/log4jDebug.xml"
CP="-cp /home/eslahm/AtomicMulticast/libskeen/target/classes/:/home/eslahm/AtomicMulticast/disni/target/classes/:/home/eslahm/AtomicMulticast/libskeen/target/dependency/*:/home/eslahm/AtomicMulticast/libramcast/target/classes/:/home/eslahm/AtomicMulticast/libramcast/target/dependency/*"
SERVER="ch.usi.dslab.mojtaba.libskeen.rdma.Server"
CLIENT="ch.usi.dslab.mojtaba.libskeen.rdma.Client"
CONFIG_FILE="/home/eslahm/AtomicMulticast/libskeen/bin-rdma/skeen_replication_system_config.json"

tmux send-keys -t 1 "ssh -o StrictHostKeyChecking=no node3 \"$JAVA $CP $SERVER 0 $CONFIG_FILE\"" C-m
tmux send-keys -t 2 "ssh -o StrictHostKeyChecking=no node4 \"$JAVA $CP $SERVER 1 $CONFIG_FILE\"" C-m
tmux send-keys -t 3 "ssh -o StrictHostKeyChecking=no node5 \"$JAVA $CP $SERVER 2 $CONFIG_FILE\"" C-m
tmux send-keys -t 4 "ssh -o StrictHostKeyChecking=no node6 \"$JAVA $CP $SERVER 3 $CONFIG_FILE\"" C-m
tmux send-keys -t 5 "ssh -o StrictHostKeyChecking=no node7 \"$JAVA $CP $SERVER 4 $CONFIG_FILE\"" C-m
tmux send-keys -t 6 "ssh -o StrictHostKeyChecking=no node8 \"$JAVA $CP $SERVER 5 $CONFIG_FILE\"" C-m

tmux set -g mouse on
tmux attach-session -t libskeen

# sleep 3
# tmux send-keys -t 0 "$JAVA $CP $CLIENT 1000 $CONFIG_FILE" C-m
