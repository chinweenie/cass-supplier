#!/bin/bash
#SBATCH --job-name=start_cass
#SBATCH --partition=long
#SBATCH --output=/home/stuproj/cs4224d/cass_log/start_cass-%j.out
#SBATCH --error=/home/stuproj/cs4224d/cass_log/start_cass-%j.err
#SBATCH --nodelist=xgpf3,xgpf4,xgpf5,xgpf6,xgpf7
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=2G
#SBATCH --cpus-per-task=24
#SBATCH --time=3:0:0


logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

source ${HOME}/.bashrc
echo $@
ip_address=("$@")
seed_ip=${ip_address[0]}
echo "Receiving ip_address ${ip_address[0]}, ${ip_address[1]}, ${ip_address[2]}, ${ip_address[3]}, ${ip_address[4]}, and seed_ip is $seed_ip"

node_prefix=$(echo $SLURM_JOB_NODELIST | cut -d'[' -f1)
node_range=$(echo $SLURM_JOB_NODELIST | cut -d'[' -f2 | cut -d']' -f1)
IFS='-' read -ra range <<< "$node_range"
node_start=$(echo ${range[0]})
node_end=$(echo ${range[1]})

nodes_list=()
for ((i=node_start;i<=node_end;i++)); do
  node_name="${node_prefix}${i}"
  nodes_list+=("$node_name")
done

node1="${nodes_list[0]}"
node2="${nodes_list[1]}"
node3="${nodes_list[2]}"
node4="${nodes_list[3]}"
node5="${nodes_list[4]}"

chmod +x  ${HOME}/install_cass.sh
chmod +x  ${HOME}/start_cass.sh

#echo "Installing Cassandra and setting up configuration yaml in each node"
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 ${HOME}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node1}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 ${HOME}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node2}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 ${HOME}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node3}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 ${HOME}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node4}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 ${HOME}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node5}.log 2>&1 &
#sleep 60

echo "Starting Cassandra on every node"
srun --nodes=3 --ntasks=3 --cpus-per-task=12 --nodelist=$node1,$node2,$node3 ${HOME}/start_cass.sh &
sleep 120
srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=$node4 ${HOME}/start_cass.sh &
sleep 60
srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=$node5 ${HOME}/start_cass.sh &
sleep 100

#echo "Creating tables and keyspace in $node1 using $seed_ip"
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $CASS_DIR/cqlsh.py $seed_ip < $CASS_DIR/data_files/startup.cql" > ${HOME}/cass_log/create-schema.log 2>&1 &
#echo "Completed create tables and keyspace"
#sleep 300
#
#echo "Loading data in $node2 using ${ip_address[1]}"
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=$node2 bash -c "python $CASS_DIR/cqlsh.py ${ip_address[1]} < $CASS_DIR/data_files/load_data.cql" > ${HOME}/cass_log/load-data.log 2>&1 &
#sleep 1200

#echo ${HOME}
#echo "Before Execution of srun."
#array=("0.txt" "1.txt" "2.txt" "3.txt" "4.txt" "5.txt" "6.txt" "7.txt" "8.txt" "9.txt" "10.txt" "11.txt" "12.txt" "13.txt" "14.txt" "15.txt" "16.txt" "17.txt" "18.txt" "19.txt")
#shuffled_array=($(for i in "${array[@]}"; do echo "$i"; done | shuf))
#driver_dir="$CASS_DIR/xact_files/app.py"
#
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[0]}" > ${HOME}/cass_log/client-${node1}-${shuffled_array[0]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[1]}" > ${HOME}/cass_log/client-${node1}-${shuffled_array[1]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[2]}" > ${HOME}/cass_log/client-${node1}-${shuffled_array[2]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[3]}" > ${HOME}/cass_log/client-${node1}-${shuffled_array[3]}.log 2>&1 &
#
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[4]}" > ${HOME}/cass_log/client-${node2}-${shuffled_array[4]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[5]}" > ${HOME}/cass_log/client-${node2}-${shuffled_array[5]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[6]}" > ${HOME}/cass_log/client-${node2}-${shuffled_array[6]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[7]}" > ${HOME}/cass_log/client-${node2}-${shuffled_array[7]}.log 2>&1 &
#
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[8]}" > ${HOME}/cass_log/client-${node3}-${shuffled_array[8]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[9]}" > ${HOME}/cass_log/client-${node3}-${shuffled_array[9]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[10]}" > ${HOME}/cass_log/client-${node3}-${shuffled_array[10]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[11]}" > ${HOME}/cass_log/client-${node3}-${shuffled_array[11]}.log 2>&1 &
#
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[12]}" > ${HOME}/cass_log/client-${node4}-${shuffled_array[12]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[13]}" > ${HOME}/cass_log/client-${node4}-${shuffled_array[13]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[14]}" > ${HOME}/cass_log/client-${node4}-${shuffled_array[14]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[15]}" > ${HOME}/cass_log/client-${node4}-${shuffled_array[15]}.log 2>&1 &
#
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[16]}" > ${HOME}/cass_log/client-${node5}-${shuffled_array[16]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[17]}" > ${HOME}/cass_log/client-${node5}-${shuffled_array[17]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[18]}" > ${HOME}/cass_log/client-${node5}-${shuffled_array[18]}.log 2>&1 &
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[19]}" > ${HOME}/cass_log/client-${node5}-${shuffled_array[19]}.log 2>&1 &
#echo "Executed srun on all nodes."
#sleep 3600
#
echo "Computing dbstate, clients and throughput"
srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=$node3 bash -c "python $CASS_DIR/xact_files/calculate_result.py ${ip_address[2]}" > ${HOME}/cass_log/calculate-result.log 2>&1 &

sleep 3600
# Killing Cassandra on all nodes
#srun --nodes=5 --ntasks=5 --cpus-per-task=10 --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34 bash -c "kill $(ps aux | grep 'CassandraDaemon' | grep -v 'grep' | awk '{print $2}')"
wait

