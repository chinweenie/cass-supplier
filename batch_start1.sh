#!/bin/bash
#SBATCH --job-name=start_cass
#SBATCH --partition=long
#SBATCH --output=/home/stuproj/cs4224d/cass_log/start_cass-%j.out
#SBATCH --error=/home/stuproj/cs4224d/cass_log/start_cass-%j.err
#SBATCH --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=2G
#SBATCH --cpus-per-task=24
#SBATCH --time=3:0:0


logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

source ${HOME}/.bashrc
echo "Starting Cassandra on every node"
srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=xcnd30 ${HOME}/start_cass.sh &
sleep 40
srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=xcnd31 ${HOME}/start_cass.sh &
sleep 40
srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=xcnd32 ${HOME}/start_cass.sh &
sleep 40
srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=xcnd33 ${HOME}/start_cass.sh &
sleep 40
srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=xcnd34 ${HOME}/start_cass.sh &
sleep 80

#echo "Creating tables and keyspace in xcnd30"
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd30 bash -c "python $CASS_DIR/cqlsh.py 192.168.48.249 < $CASS_DIR/data_files/startup.cql" > ${HOME}/cass_log/create-schema.log 2>&1 &
#echo "Completed create tables and keyspace"
#sleep 120
#
#echo "Loading data in xcnd32"
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd32 bash -c "python $CASS_DIR/cqlsh.py 192.168.48.251 < $CASS_DIR/data_files/load_data.cql" > ${HOME}/cass_log/load-data.log 2>&1 &
#sleep 1200

echo ${HOME}
echo "Before Execution of srun."
array=("0.txt" "1.txt" "2.txt" "3.txt" "4.txt" "5.txt" "6.txt" "7.txt" "8.txt" "9.txt" "10.txt" "11.txt" "12.txt" "13.txt" "14.txt" "15.txt" "16.txt" "17.txt" "18.txt" "19.txt")
ip_address=("192.168.48.249" "192.168.48.250" "192.168.48.251" "192.168.48.252" "192.168.48.253")
shuffled_array=($(for i in "${array[@]}"; do echo "$i"; done | shuf))
nodes=("xcnd30" "xcnd31" "xcnd32" "xcnd33" "xcnd34")
driver_dir="$CASS_DIR/xact_files/app.py"

srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd30 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[0]}" > ${HOME}/cass_log/client-${nodes[0]}-${shuffled_array[0]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd30 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[1]}" > ${HOME}/cass_log/client-${nodes[0]}-${shuffled_array[1]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd30 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[2]}" > ${HOME}/cass_log/client-${nodes[0]}-${shuffled_array[2]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd30 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[3]}" > ${HOME}/cass_log/client-${nodes[0]}-${shuffled_array[3]}.log 2>&1 &

srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd31 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[4]}" > ${HOME}/cass_log/client-${nodes[1]}-${shuffled_array[4]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd31 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[5]}" > ${HOME}/cass_log/client-${nodes[1]}-${shuffled_array[5]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd31 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[6]}" > ${HOME}/cass_log/client-${nodes[1]}-${shuffled_array[6]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd31 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[7]}" > ${HOME}/cass_log/client-${nodes[1]}-${shuffled_array[7]}.log 2>&1 &

srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd32 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[8]}" > ${HOME}/cass_log/client-${nodes[2]}-${shuffled_array[8]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd32 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[9]}" > ${HOME}/cass_log/client-${nodes[2]}-${shuffled_array[9]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd32 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[10]}" > ${HOME}/cass_log/client-${nodes[2]}-${shuffled_array[10}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd32 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[11]}" > ${HOME}/cass_log/client-${nodes[2]}-${shuffled_array[11]}.log 2>&1 &

srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd33 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[12]}" > ${HOME}/cass_log/client-${nodes[3]}-${shuffled_array[12]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd33 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[13]}" > ${HOME}/cass_log/client-${nodes[3]}-${shuffled_array[13]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd33 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[14]}" > ${HOME}/cass_log/client-${nodes[3]}-${shuffled_array[14]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd33 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[15]}" > ${HOME}/cass_log/client-${nodes[3]}-${shuffled_array[15]}.log 2>&1 &

srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[16]}" > ${HOME}/cass_log/client-${nodes[4]}-${shuffled_array[16]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[17]}" > ${HOME}/cass_log/client-${nodes[4]}-${shuffled_array[17]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[18]}" > ${HOME}/cass_log/client-${nodes[4]}-${shuffled_array[18]}.log 2>&1 &
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[19]}" > ${HOME}/cass_log/client-${nodes[4]}-${shuffled_array[19]}.log 2>&1 &

#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd30 bash -c "$HOME/start_client.sh 192.168.48.249 ${shuffled_array[0]} ${shuffled_array[1]} ${shuffled_array[2]} ${shuffled_array[3]}" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd31 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.250 ${shuffled_array[4]} ${shuffled_array[5]} ${shuffled_array[6]} ${shuffled_array[7]}" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd32 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.251 ${shuffled_array[8]} ${shuffled_array[9]} ${shuffled_array[10]} ${shuffled_array[11]}" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd33 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.252 ${shuffled_array[12]} ${shuffled_array[13]} ${shuffled_array[14]} ${shuffled_array[15]}" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd34 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.253 ${shuffled_array[16]} ${shuffled_array[17]} ${shuffled_array[18]} ${shuffled_array[19]}"
echo "Executed srun on all nodes."

sleep 1800
echo "Computing dbstate, clients and throughput"
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd34 bash -c "python $CASS_DIR/xact_files/calculate_result.py 192.168.48.253" > ${HOME}/cass_log/calculate-result.log 2>&1 &

#sleep 3600
# Killing Cassandra on all nodes
#srun --nodes=5 --ntasks=5 --cpus-per-task=10 --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34 bash -c "kill $(ps aux | grep 'CassandraDaemon' | grep -v 'grep' | awk '{print $2}')"
wait

