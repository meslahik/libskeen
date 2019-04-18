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

JAVA="java -Dlog4j.configuration=file:/Users/meslahik/PhD/Projects/RDMAPaxos/code/AtomicMulticast/libskeen/bin-tcp/log4jDebug.xml"
CP="-cp /Users/meslahik/PhD/Projects/RDMAPaxos/code/AtomicMulticast/libskeen/target/classes:/Users/meslahik/PhD/Projects/RDMAPaxos/code/AtomicMulticast/libskeen/target/dependency/*"
SERVER="ch.usi.dslab.mojtaba.libskeen.Server"
CLIENT="ch.usi.dslab.mojtaba.libskeen.Client"
CONFIG_FILE="/Users/meslahik/PhD/Projects/RDMAPaxos/code/AtomicMulticast/libskeen/bin-tcp/skeen_system_config_localhost.json"

tmux send-keys -t 1 "$JAVA $CP $SERVER 0 $CONFIG_FILE" C-m
tmux send-keys -t 2 "$JAVA $CP $SERVER 1 $CONFIG_FILE" C-m
tmux send-keys -t 3 "$JAVA $CP $SERVER 2 $CONFIG_FILE" C-m
tmux send-keys -t 4 "$JAVA $CP $SERVER 3 $CONFIG_FILE" C-m
tmux send-keys -t 5 "$JAVA $CP $SERVER 4 $CONFIG_FILE" C-m
tmux send-keys -t 6 "$JAVA $CP $SERVER 5 $CONFIG_FILE" C-m

tmux set -g mouse on
tmux attach-session -t libskeen

# sleep 3
# tmux send-keys -t 0 "$JAVA $CP $CLIENT 1000 $CONFIG_FILE" C-m
