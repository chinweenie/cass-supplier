#!/bin/bash
#

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

source .bashrc
NODE=$(hostname)
echo $(logtime) "$(java -version)"
echo $(logtime) "$NODE: $JAVA_HOME"
rm -f $CASS_DIR/xact_files/stderr
rm -f $CASS_DIR/xact_files/stdout
$cassandra -f -Dcassandra.ignore_dc=true > ${HOME}/cass_log/cass-startup-${NODE}.log 2>&1 &
sleep 10800

