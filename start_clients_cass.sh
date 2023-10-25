#!/bin/bash


source ~/.bashrc
echo "start_clients_cass.sh started" > ${HOME}/client_script_started.log
echo $1
IFS=' ' read -ra ARGS <<< "$1"
echo "Executing start_clients_cass.sh in $(hostname)" > ${HOME}/client_script_started.log
python $CASS_DIR/xact_files/app.py "${ARGS[@]}"
