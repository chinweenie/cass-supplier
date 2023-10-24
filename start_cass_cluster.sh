#!/bin/sh
#SBATCH --job-name=start_cass_cluster
#SBATCH --partition=medium
#SBATCH --nodelist=xcnd30
#SBATCH --time=1:00:00
#SBATCH --output=cassandra_log.out

srun --nodes=1 --ntasks=1 /temp/teamd-cass/apache-cassandra-4.1.3/bin/./cassandra
echo "end"
sleep 3600
