#!/bin/bash
#

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

source .bashrc
NODE=$(hostname)
echo $(logtime) "$(java -version)"
echo $(logtime) "$NODE: $JAVA_HOME"
echo $(logtime) "$NODE: $CASSANDRA_HOME"
$cassandra -f > ${HOME}/cass-startup-${NODE}.log 2>&1 &
sleep 1800
echo $(logtime) "$(ps -ef | grep cassandra)"
echo $(logtime) "$(nodetool)"

