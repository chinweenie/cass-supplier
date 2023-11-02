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
srun --nodes=1 --ntasks=1 --cpus-per-task=2 --nodelist=xcnd30 bash -c "python $CITUS_DIR/citus_code/preparation/delete_tables.py"
echo "clean up previous tables"


