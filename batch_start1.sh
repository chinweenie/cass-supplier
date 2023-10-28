#!/bin/bash
#SBATCH --job-name=start_cass
#SBATCH --partition=medium
#SBATCH --output=/home/stuproj/cs4224d/batch_log/start_cass-%j.out
#SBATCH --error=/home/stuproj/cs4224d/batch_log/start_cass-%j.err
#SBATCH --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34
#SBATCH --ntasks-per-node=1
#SBATCH --mem-per-cpu=2G
#SBATCH --cpus-per-task=24

logtime() {
    date +"%Y-%m-%dT%H:%M:%S:%3N -- "
}

source ${HOME}/.bashrc
echo "Starting Cassandra on every node"
srun --nodes=3 --ntasks=3 --cpus-per-task=12 --nodelist=xcnd30,xcnd31,xcnd32 ${HOME}/start_cass.sh &
sleep 60
srun --nodes=2 --ntasks=2 --cpus-per-task=12 --nodelist=xcnd33,xcnd34 ${HOME}/start_cass.sh &
sleep 60

#echo "Compiling csv files in xcnd30"
#srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd30 bash -c "python $CASS_DIR/data_files/initialize_csv_files.py" > ${HOME}/batch_log/compile-csv.log 2>&1 &
#sleep 60

echo "Creating tables and keyspace in xcnd30"
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd30 bash -c "python $CASS_DIR/cqlsh.py 192.168.48.249 < $CASS_DIR/data_files/startup.cql" > ${HOME}/batch_log/create-schema.log 2>&1 &
echo "Completed create tables and keyspace"
sleep 180

echo "Loading data in xcnd30"
srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd30 bash -c "python $CASS_DIR/cqlsh.py 192.168.48.249 < $CASS_DIR/data_files/load_data.cql" > ${HOME}/batch_log/load-data.log 2>&1 &
sleep 300


echo ${HOME}
echo "Before Execution of srun."
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd30 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.249 0.txt 1.txt 2.txt 3.txt" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd31 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.250 4.txt 5.txt 6.txt 7.txt" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd32 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.251 8.txt 9.txt 10.txt 11.txt" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd33 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.252 12.txt 13.txt 14.txt 15.txt" &
#srun --nodes=1 --ntasks=1 --cpus-per-task=4 --nodelist=xcnd34 bash -c "python $CASS_DIR/xact_files/app.py 192.168.48.253 16.txt 17.txt 18.txt 19.txt"
#echo "Executed srun on all nodes."

# Gently killing Cassandra on all nodes
#srun --nodes=5 --ntasks=5 --cpus-per-task=10 --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34 bash -c "kill $(ps aux | grep 'CassandraDaemon' | grep -v 'grep' | awk '{print $2}')"

sleep 3600

