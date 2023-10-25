#!/bin/bash

#SBATCH --job-name=my_serial_parallel_tasks
#SBATCH --output=job_output_%j.txt
#SBATCH --error=job_output_%j.err
#SBATCH --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34
#SBATCH --ntasks-per-node=1
#SBATCH --time=01:00:00
#SBATCH --partition=long

# Task 1 - Start Cassandra servers serially on each node
source ~/.bashrc
for i in $(seq 1 5); do
    srun --nodes=1 --ntasks=1 ~/start_cass.sh &
done


# Verify Cassandra nodes are up and running
check_cassandra_nodes() {
    for i in $(seq 1 5); do
        status=$(srun --nodes=1 --ntasks=1 nodetool status | grep -c "UN")
        if [ "$status" -ne 5 ]; then
            return 1
        fi
    done
    return 0
}

timeout=300 # Wait for 300 seconds (5 minutes) at most
interval=10 # Check every 10 seconds
elapsed=0

while ! check_cassandra_nodes && [ "$elapsed" -lt "$timeout" ]; do
    sleep "$interval"
    elapsed=$((elapsed+interval))
done

if [ "$elapsed" -ge "$timeout" ]; then
    echo "Cassandra nodes did not start up in time. Exiting."
    exit 1
fi

# Task 2 - Execute in parallel on all 5 nodes
srun --nodes=5 --ntasks-per-node=1 ~/task2_executable.sh
wait