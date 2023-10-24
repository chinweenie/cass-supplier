#!/bin/bash


source ~/.bashrc
echo $1
IFS=' ' read -ra ARGS <<< "$1"
python $CASS_DIR/xact_files/app.py "${ARGS[@]}"
