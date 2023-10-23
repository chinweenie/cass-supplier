#!/bin/bash
#SBATCH --job-name=assign_clients_cass
#SBATCH --partition=long
#SBATCH --output=assign_clients_cass-%j.out
#SBATCH --error=assign_clients_cass-%j.err
#SBATCH --nodelist=xcnd30,xcnd31,xcnd32,xcnd33,xcnd34


arg_for_xcnd30="$(host xcnd30 | awk '/has address/ { print $4 }') 0.txt 1.txt 2.txt 3.txt"
arg_for_xcnd31="$(host xcnd31 | awk '/has address/ { print $4 }') 4.txt 5.txt 6.txt 7.txt"
arg_for_xcnd32="$(host xcnd32 | awk '/has address/ { print $4 }') 8.txt 9.txt 10.txt 11.txt"
arg_for_xcnd33="$(host xcnd33 | awk '/has address/ { print $4 }') 12.txt 13.txt 14.txt 15.txt"
arg_for_xcnd34="$(host xcnd34 | awk '/has address/ { print $4 }') 16.txt 17.txt 18.txt 19.txt"

args_for_nodes=(arg_for_xcnd30 $arg_for_xcnd31 $arg_for_xcnd32 $arg_for_xcnd33 $arg_for_xcnd34)
echo "Check slurm nodeid ${SLURM_NODEID}"
srun ~/start_clients_cass.sh "${!args_for_nodes[$SLURM_NODEID]}"