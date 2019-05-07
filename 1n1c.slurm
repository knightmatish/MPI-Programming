#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0:10:00
#SBATCH --partition=cloud

# Load required modules
 module load Python/3.6.4-intel-2017.u2

# Execute script file with parameter <num_of_nodes> <num_of_cores> <grid_file> <twitter_file> <file_prefix>
time ./ProcessData.sh 1 1 'melbGrid.json' 'bigTwitter.json' 'temp_'