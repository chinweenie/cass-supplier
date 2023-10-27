#!/bin/bash
#SBATCH --job-name=start_cass
#SBATCH --partition=medium
#SBATCH --output=start_cass-%j.out
#SBATCH --error=start_cass-%j.err
#SBATCH --array=0-4
#SBATCH --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34

source .bashrc
# Function to check if Cassandra is running on this node
function cassandra_is_running {
    # This can be adjusted as per your setup, here's a basic example
    if ps aux | grep 'cassandra' > /dev/null; then
        return 0
    else
        return 1
    fi
}
# Start Cassandra on this node
srun ~/start_cass.sh

#$CASS_DIR/cassandra.sh -f > ${HOME}/cass-startup-${NODE}.log 2>&1 &

# Now, each node will wait until all Cassandra instances are running across all nodes
nodes_up=0
# Loop until 5 nodes are Up and Normal
while [ "$nodes_up" -ne "5" ]; do
#  Try print out the result of nodetool status
    nodes_up=$(nodetool status | grep "^UN" | wc -l)

    if [ "$nodes_up" -eq "5" ]; then
        echo "All nodes are Up and Normal. Proceeding with client start."
        # start your clients here
        break
    else
        echo "$nodes_up nodes are Up. Waiting for all nodes to be Up and Normal."
        sleep 10  # wait for 10 seconds before checking again
    fi
done

# At this point, all nodes have confirmed that Cassandra is running on every node.

# Start the distributed client script on each respective node.
# The client script path may differ per node, so adjust as necessary.
arg_for_xcnd30="$(host xcnd30 | awk '/has address/ { print $4 }') '0.txt 1.txt 2.txt 3.txt'"
arg_for_xcnd31="$(host xcnd31 | awk '/has address/ { print $4 }') '4.txt 5.txt 6.txt 7.txt'"
arg_for_xcnd32="$(host xcnd32 | awk '/has address/ { print $4 }') '8.txt 9.txt 10.txt 11.txt'"
arg_for_xcnd33="$(host xcnd33 | awk '/has address/ { print $4 }') '12.txt 13.txt 14.txt 15.txt'"
arg_for_xcnd34="$(host xcnd34 | awk '/has address/ { print $4 }') '16.txt 17.txt 18.txt 19.txt'"
args_for_nodes=(arg_for_xcnd30 $arg_for_xcnd31 $arg_for_xcnd32 $arg_for_xcnd33 $arg_for_xcnd34)
echo "Check slurm task id ${SLURM_ARRAY_TASK_ID}"
#srun ~/start_clients_cass.sh ${!args_for_nodes[$SLURM_ARRAY_TASK_ID]}