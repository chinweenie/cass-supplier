#!/bin/bash
#SBATCH --job-name=start_cass
#SBATCH --partition=long
#SBATCH --output=/home/stuproj/cs4224d/cass_log/start_cass-%j.out
#SBATCH --error=/home/stuproj/cs4224d/cass_log/start_cass-%j.err
#SBATCH --nodelist=xcnc[37-41]
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=2G
#SBATCH --cpus-per-task=24
#SBATCH --time=3:0:0


logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

source ${HOME}/.bashrc
CASSD=${HOME}/cass_data_files/
echo $@
action="$1"
shift
ip_address=("$@")
echo "Receiving ip_address ${ip_address[0]}, ${ip_address[1]}, ${ip_address[2]}, ${ip_address[3]}, ${ip_address[4]}"

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

chmod +x  ${CASSD}/install_cass.sh
chmod +x  ${CASSD}/start_cass.sh

if [ "$action" = "init" ]; then
  echo "Installing Cassandra and setting up configuration yaml in each node"
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 ${CASSD}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node1}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 ${CASSD}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node2}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 ${CASSD}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node3}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 ${CASSD}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node4}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 ${CASSD}/install_cass.sh ${ip_address[0]} ${ip_address[1]} ${ip_address[2]} > ${HOME}/cass_log/install_cass_${node5}.log 2>&1 &
  sleep 100
fi

if [ "$action" = "init" ] || [ "$action" = "run" ] || [ "$action" = "calculate" ]; then
  echo "Starting Cassandra on every node"
  srun --nodes=3 --ntasks=3 --cpus-per-task=12 --nodelist=$node1,$node2,$node3 ${CASSD}/start_cass.sh &
  sleep 120
  srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=$node4 ${CASSD}/start_cass.sh &
  sleep 120
  srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=$node5 ${CASSD}/start_cass.sh &
  sleep 120
fi

if [ "$action" = "run" ]; then
  echo "Creating tables and keyspace in $node1 using ${ip_address[0]}"
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $CASS_DIR/cqlsh.py ${ip_address[0]} < $CASS_DIR/data_files/startup.cql" > ${HOME}/cass_log/create-schema.log 2>&1 &
  echo "Completed create tables and keyspace"
  sleep 300

  echo "Loading data in $node2 using ${ip_address[1]}"
  srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=$node2 bash -c "python $CASS_DIR/cqlsh.py ${ip_address[1]} < $CASS_DIR/data_files/load_data.cql" > ${HOME}/cass_log/load-data.log 2>&1 &
  sleep 1200

  echo ${HOME}
  echo "Before Execution of srun."
  array=("0.txt" "1.txt" "2.txt" "3.txt" "4.txt" "5.txt" "6.txt" "7.txt" "8.txt" "9.txt" "10.txt" "11.txt" "12.txt" "13.txt" "14.txt" "15.txt" "16.txt" "17.txt" "18.txt" "19.txt")
  shuffled_array=($(for i in "${array[@]}"; do echo "$i"; done | shuf))
  driver_dir="$CASS_DIR/xact_files/app.py"

  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[0]}" > ${HOME}/cass_log/client-${node1}-${shuffled_array[0]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[1]}" > ${HOME}/cass_log/client-${node1}-${shuffled_array[1]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[2]}" > ${HOME}/cass_log/client-${node1}-${shuffled_array[2]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node1 bash -c "python $driver_dir ${ip_address[0]} ${shuffled_array[3]}" > ${HOME}/cass_log/client-${node1}-${shuffled_array[3]}.log 2>&1 &

  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[4]}" > ${HOME}/cass_log/client-${node2}-${shuffled_array[4]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[5]}" > ${HOME}/cass_log/client-${node2}-${shuffled_array[5]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[6]}" > ${HOME}/cass_log/client-${node2}-${shuffled_array[6]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node2 bash -c "python $driver_dir ${ip_address[1]} ${shuffled_array[7]}" > ${HOME}/cass_log/client-${node2}-${shuffled_array[7]}.log 2>&1 &

  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[8]}" > ${HOME}/cass_log/client-${node3}-${shuffled_array[8]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[9]}" > ${HOME}/cass_log/client-${node3}-${shuffled_array[9]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[10]}" > ${HOME}/cass_log/client-${node3}-${shuffled_array[10]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node3 bash -c "python $driver_dir ${ip_address[2]} ${shuffled_array[11]}" > ${HOME}/cass_log/client-${node3}-${shuffled_array[11]}.log 2>&1 &

  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[12]}" > ${HOME}/cass_log/client-${node4}-${shuffled_array[12]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[13]}" > ${HOME}/cass_log/client-${node4}-${shuffled_array[13]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[14]}" > ${HOME}/cass_log/client-${node4}-${shuffled_array[14]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node4 bash -c "python $driver_dir ${ip_address[3]} ${shuffled_array[15]}" > ${HOME}/cass_log/client-${node4}-${shuffled_array[15]}.log 2>&1 &

  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[16]}" > ${HOME}/cass_log/client-${node5}-${shuffled_array[16]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[17]}" > ${HOME}/cass_log/client-${node5}-${shuffled_array[17]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[18]}" > ${HOME}/cass_log/client-${node5}-${shuffled_array[18]}.log 2>&1 &
  srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=$node5 bash -c "python $driver_dir ${ip_address[4]} ${shuffled_array[19]}" > ${HOME}/cass_log/client-${node5}-${shuffled_array[19]}.log 2>&1 &
  echo "Executed srun on all nodes."
  sleep 3600
fi

if [ "$action" = "calculate" ]; then
  echo "Computing dbstate, clients and throughput"
  srun --nodes=1 --ntasks=1 --cpus-per-task=12 --nodelist=$node3 bash -c "python $CASS_DIR/xact_files/calculate_result.py ${ip_address[2]}" > ${HOME}/cass_log/calculate-result.log 2>&1 &
  sleep 1800
fi

if [ "$action" = "run" ] || [ "$action" = "calculate" ]; then
  echo "Killing Cassandra on all nodes"
  srun --nodes=5 --ntasks=5 --cpus-per-task=2 --nodelist=$node1,$node2,$node3,$node4,$node5 bash -c "kill $(ps aux | grep 'CassandraDaemon' | grep -v 'grep' | awk '{print $2}')"
  #wait
fi

if [ "$action" = "delete" ]; then
  srun --nodes=5 --ntasks=5 --cpus-per-task=2 --nodelist=$node1,$node2,$node3,$node4,$node5 bash -c "rm -r /temp/teamd-cass"
fi








