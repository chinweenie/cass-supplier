#!/bin/bash


echo $1
IFS=' ' read -ra ARGS <<< "$1"
python /temp/teamd-cass/app.py "${ARGS[@]}"
