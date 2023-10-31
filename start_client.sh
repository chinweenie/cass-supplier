#!/bin/bash

source .bashrc
logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}
IP_ADDRESS=$1
file1=$2
file2=$3
file3=$4
file4=$5
source .bashrc
NODE=$(hostname)
echo "Executing files in ${hostname} ${file1} ${file2} ${file2} ${file3} ${file4}"
echo "File 2 ${file2}"
echo "File 3 ${file3}"
echo "File 4 ${file4}"

python $CASS_DIR/xact_files/app.py $IP_ADDRESS $file1 > ${HOME}/batch_log/client-${NODE}-$file1.log 2>&1 &
python $CASS_DIR/xact_files/app.py $IP_ADDRESS $file2 > ${HOME}/batch_log/client-${NODE}-$file2.log 2>&1 &
python $CASS_DIR/xact_files/app.py $IP_ADDRESS $file3 > ${HOME}/batch_log/client-${NODE}-$file3.log 2>&1 &
python $CASS_DIR/xact_files/app.py $IP_ADDRESS $file4 > ${HOME}/batch_log/client-${NODE}-$file4.log 2>&1 &