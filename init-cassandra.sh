#!/bin/bash
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <node_name>"
    exit 1
fi

NODE_NAME=$1
CASS_DIR="/temp/teamd-cass/apache-cassandra-4.1.3/bin"

# Allocate the specified node
salloc -p long -w $NODE_NAME bash <<EOL
    srun --pty $CASS_DIR/cassandra
EOL
# Check if an argument was provided
#if [ "$#" -ne 1 ]; then
#    echo "Usage: $0 <node_name>"
#    exit 1
#fi
#node=$1
#salloc --partition=long --nodelist="$node"
#srun --pty bash
#CASS_DIR=/temp/teamd-cass/apache-cassandra-4.1.3/bin
#$CASS_DIR/cassandra