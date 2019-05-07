#!/bin/bash
# 2 nodes, 4 tasks per node = 8 cores
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0:15:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.6.4-intel-2017.u2

# Execute script file with parameter <num_of_nodes> <num_of_cores> <grid_file> <twitter_file> <file_prefix>
time ./ProcessData.sh 2 4 'melbGrid.json' 'bigTwitter.json' 'temp_'
