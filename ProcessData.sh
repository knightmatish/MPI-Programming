
# This script will accept parameters from the slurm file.
num_of_nodes=$1

num_of_cores=$2

grid_file=$3

twitter_file=$4

temp_file_prefix=$5

num_of_cores=$(($1*$2)) 

# Following command will split the Twitter file on the number of cores
split -l $((`awk 'END {print NR}' $twitter_file`/$num_of_cores)) $twitter_file -d $temp_file_prefix

# Following command will count the number of split files
num_of_files="$(ls $temp_file_prefix* | wc -l)"

echo "MPI setup: $num_of_nodes Node(s), $num_of_cores Core(s)"
# This will determine the setup architecture
if [ $num_of_nodes -gt 1 ]
then
   mpirun python3 LocateTweets.py $grid_file $num_of_files $temp_file_prefix 
else
   mpiexec -n $num_of_cores python3 LocateTweets.py $grid_file $num_of_files $temp_file_prefix
fi

# Following command will do the cleanup
rm $temp_file_prefix*

