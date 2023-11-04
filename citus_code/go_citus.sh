#!/bin/bash
#SBATCH --job-name=start_citus
#SBATCH --partition=long
#SBATCH --output=/home/stuproj/cs4224d/batch_log/start_citus-%j.out
#SBATCH --error=/home/stuproj/cs4224d/batch_log/start_citus-%j.err
#SBATCH --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=2G
#SBATCH --cpus-per-task=24

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

source ${HOME}/.bashrc

echo "Creating tables and import data at xcnd30"
psql <<EOF
\i ./citus_code/preparation/delete_table.sql
\i ./citus_code/preparation/data_modeling.sql
\i ./citus_code/preparation/data_import.sql 
EOF
echo "clean up previous tables"
echo "finish create 7 tables and data import"

echo ${HOME}
echo "Before Execution of srun."
array=("0.txt" "1.txt" "2.txt" "3.txt" "4.txt" "5.txt" "6.txt" "7.txt" "8.txt" "9.txt" "10.txt" "11.txt" "12.txt" "13.txt" "14.txt" "15.txt" "16.txt" "17.txt" "18.txt" "19.txt")
ip_address=("192.168.48.249" "192.168.48.250" "192.168.48.251" "192.168.48.252" "192.168.48.253")
shuffled_array=($(for i in "${array[@]}"; do echo "$i"; done | shuf))
driver_dir = "$CITUS_DIR/citus_code/main_driver.py"
nodes=("xcnd30" "xcnd31" "xcnd32" "xcnd33" "xcnd34")

srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd31 bash -c "python $driver_dir ./xact_files/${shuffled_array[0]}" > ${HOME}/citus_log/client-${nodes[1]}-${shuffled_array[0]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd31 bash -c "python $driver_dir ./xact_files/${shuffled_array[1]}" > ${HOME}/citus_log/client-${nodes[1]}-${shuffled_array[1]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd31 bash -c "python $driver_dir ./xact_files/${shuffled_array[2]}" > ${HOME}/citus_log/client-${nodes[1]}-${shuffled_array[2]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd31 bash -c "python $driver_dir ./xact_files/${shuffled_array[3]}" > ${HOME}/citus_log/client-${nodes[1]}-${shuffled_array[3]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd31 bash -c "python $driver_dir ./xact_files/${shuffled_array[4]}" > ${HOME}/citus_log/client-${nodes[1]}-${shuffled_array[4]}.log 2>&1 &

srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd32 bash -c "python $driver_dir ./xact_files/${shuffled_array[5]}" > ${HOME}/citus_log/client-${nodes[2]}-${shuffled_array[5]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd32 bash -c "python $driver_dir ./xact_files/${shuffled_array[6]}" > ${HOME}/citus_log/client-${nodes[2]}-${shuffled_array[6]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd32 bash -c "python $driver_dir ./xact_files/${shuffled_array[7]}" > ${HOME}/citus_log/client-${nodes[2]}-${shuffled_array[7]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd32 bash -c "python $driver_dir ./xact_files/${shuffled_array[8]}" > ${HOME}/citus_log/client-${nodes[2]}-${shuffled_array[8]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd32 bash -c "python $driver_dir ./xact_files/${shuffled_array[9]}" > ${HOME}/citus_log/client-${nodes[2]}-${shuffled_array[9]}.log 2>&1 &

srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd33 bash -c "python $driver_dir ./xact_files/${shuffled_array[10]}" > ${HOME}/citus_log/client-${nodes[3]}-${shuffled_array[10]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd33 bash -c "python $driver_dir ./xact_files/${shuffled_array[11]}" > ${HOME}/citus_log/client-${nodes[2]}-${shuffled_array[11]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd33 bash -c "python $driver_dir ./xact_files/${shuffled_array[12]}" > ${HOME}/citus_log/client-${nodes[3]}-${shuffled_array[12]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd33 bash -c "python $driver_dir ./xact_files/${shuffled_array[13]}" > ${HOME}/citus_log/client-${nodes[3]}-${shuffled_array[13]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd33 bash -c "python $driver_dir ./xact_files/${shuffled_array[14]}" > ${HOME}/citus_log/client-${nodes[3]}-${shuffled_array[14]}.log 2>&1 &

srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $driver_dir ./xact_files/${shuffled_array[15]}" > ${HOME}/citus_log/client-${nodes[4]}-${shuffled_array[15]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $driver_dir ./xact_files/${shuffled_array[16]}" > ${HOME}/citus_log/client-${nodes[4]}-${shuffled_array[16]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $driver_dir ./xact_files/${shuffled_array[17]}" > ${HOME}/citus_log/client-${nodes[4]}-${shuffled_array[17]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $driver_dir ./xact_files/${shuffled_array[18]}" > ${HOME}/citus_log/client-${nodes[4]}-${shuffled_array[18]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $driver_dir ./xact_files/${shuffled_array[19]}" > ${HOME}/citus_log/client-${nodes[4]}-${shuffled_array[19]}.log 2>&1 &
