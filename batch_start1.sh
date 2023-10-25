#!/bin/bash
#SBATCH --job-name=start_cass
#SBATCH --partition=medium
#SBATCH --output=start_cass-%j.out
#SBATCH --error=start_cass-%j.err
#SBATCH --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=2G
#SBATCH --cpus-per-task=20

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

source ${HOME}/.bashrc
echo "Starting Cassandra on every node"
srun --nodes=5 --ntasks=5 --cpus-per-task=16 --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34 ${HOME}/start_cass.sh &
sleep 300

echo "Compiling csv files in xcnd30"
srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd30 bash -c "python /temp/teamd-cass/data_files/initialize_csv_files.py" &
sleep 120

echo "Creating tables and keyspace in xcnd30"
srun --nodes=1 --ntasks=1 --cpus-per-task=16 -nodelist=xcnd30 bash -c "python $CASS_DIR/cqlsh.py 192.168.48.249 < /temp/teamd-cass/data_files/startup.cql" &
sleep 300

echo "Loading data in xcnd30"
srun --nodes=1 --ntasks=1 --cpus-per-task=16 -nodelist=xcnd30 bash -c "python $CASS_DIR/cqlsh.py 192.168.48.249 < /temp/teamd-cass/data_files/load_data.cql" &
sleep 300
#nodes_up=0
#while [ "$nodes_up" -ne "5" ]; do
#    nodes_up=$($nodetool status | grep "^UN" | wc -l)
#    if [ "$nodes_up" -eq "5" ]; then
#        echo "All nodes are Up and Normal. Proceeding with client start."
#        break
#    else
#        echo "$nodes_up nodes are Up. Waiting for all nodes to be Up and Normal."
#        sleep 10  # wait for 10 seconds before checking again
#    fi
#done

echo ${HOME}
echo "Before Execution of srun."
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd30 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.249 0.txt 1.txt 2.txt 3.txt" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd31 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.250 4.txt 5.txt 6.txt 7.txt" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd32 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.251 8.txt 9.txt 10.txt 11.txt" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd33 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.252 12.txt 13.txt 14.txt 15.txt" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd34 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.253 16.txt 17.txt 18.txt 19.txt"
#echo "Executed srun on all nodes."
sleep 3600

