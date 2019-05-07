'''
@author: nitishm

@author: yshinde

'''
from mpi4py import MPI
import json
import time
import sys
import operator

start = time.time()

MASTER_RANK =0

def format_output(grids_dict):

	# This function will generate the required output
	print ("-"*10,"Generating Output","-"*10)
	total_tweets = 0 	# Count the total tweets located in the given grids
	top_value = 5		# Get the top 5 hashtags from every grid

	for grid in grids_dict:
		if grid['attributes']['num_of_tweets'] != 0:
			top_tweets = grid['attributes']['hashtags'][0]
			total_tweets += grid['attributes']['num_of_tweets']
			print ("-"*5,"Grid -",grid['id'],"- Contains,",grid['attributes']['num_of_tweets'],"posts")
			sorted_tags = sorted(top_tweets.items(),key=operator.itemgetter(1),reverse=True)[:top_value]
			for tag in sorted_tags:
				print ("\t",tag[0],"-",tag[1])

	print ("-"*10,"End","-"*10)
	print ("Total number of tweets located -",total_tweets)

def process_tweet_text(grid,tweet):

	# This function will process the tweet texts to get the hashtags details
	try:
		tweet_text = tweet['doc']['text'].upper().split()
		for tag in tweet_text:
			if tag[0]=='#' and len(tag)>1 :
				if tag in grid['attributes']['hashtags'][0].keys():
					grid['attributes']['hashtags'][0][tag] += 1
				else:
					grid['attributes']['hashtags'][0][tag] = 1

	except Exception as e:
		pass

def locate_tweet(tweet,grids_dict):

	# This function will located every tweets in it's respective grids
	located = False
	x_tweet = tweet['doc']['coordinates']['coordinates'][0]
	y_tweet = tweet['doc']['coordinates']['coordinates'][1]
	for grid in grids_dict['rows']:
		if (x_tweet >= grid['attributes']['geometery'][0] and x_tweet <= grid['attributes']['geometery'][1]) and (y_tweet >= grid['attributes']['geometery'][2] and y_tweet <= grid['attributes']['geometery'][3]):
			located = True
			break;
	if located:

		# Once located it will update the grid_dict data structure
		grid['attributes']['num_of_tweets'] += 1
		process_tweet_text(grid,tweet)
	

def check_tweets(tweet,grids_dict):

	# This function will filter tweets with no geo locations
	try:
		if 'doc' in tweet.keys():
			if len(tweet['doc']['coordinates']['coordinates']) == 2:
				locate_tweet(tweet,grids_dict)
			else:
				print ('doc',tweet['id'],tweet['docs']['geo']['coordinates'])
		elif 'value' in tweet.keys():
			if len(tweet['value']['geometery']['coordinates']) == 2:
				print (tweet['value']['geometery']['coordinates'],tweet['value']['properties']['text'])
	except:
		pass

def read_grid_file(grids_file):

	# This function will create grid data structure for every core to capture all processed tweets
	with open(grids_file) as file:
		grids = json.load(file)

	# grids_dict is the data structure which will hold all the required values
	grids_dict = {}
	grids_dict['rows'] = []
	for grid in grids['features']:
		grid_property = grid['properties']
		coordinates = []
		temp_dict = {}
		for coordinate in grid_property:
			if coordinate == "id":
				temp_dict['id'] = grid_property['id']
				temp_dict['attributes'] = {}
				
			else:
				coordinates.append(grid_property[coordinate])
		
		temp_dict['attributes']['geometery'] = coordinates
		temp_dict['attributes']['num_of_tweets'] = 0
		temp_dict['attributes']['hashtags'] = [{}]
		grids_dict['rows'].append(temp_dict)


	return grids_dict
	

def process_tweets(comm,tweets_file,grids_dict):

	# This function will open every core's respective file and load the tweets
	with open(tweets_file,'rb') as f:
		for i, line in enumerate(f):

			# This will filter any garbage value, also tweets with no geo locations
			try:
				line =line.strip()
				tweet = json.loads(line[:-1])
				check_tweets(tweet,grids_dict)
			except Exception as e:
				try:
					tweet = json.loads(line)
					check_tweets(tweet,grids_dict)
				except Exception as e:
					pass

def master_merge(grids_dict_list,master_grids_dict):
	
	# This function will merge all the data into master's data
	try:
		for slave_grid_dict in grids_dict_list:
			for sgrid in slave_grid_dict['rows']:
				for mgrid in master_grids_dict['rows']:
					if mgrid['id'] == sgrid['id']:
						mgrid['attributes']['num_of_tweets'] += sgrid['attributes']['num_of_tweets']
						for tag_name,tag_count in sgrid['attributes']['hashtags'][0].items():
							if tag_name in mgrid['attributes']['hashtags'][0].keys():
								mgrid['attributes']['hashtags'][0][tag_name] += tag_count
							else:
								mgrid['attributes']['hashtags'][0][tag_name] = tag_count
	except Exception as e:
		pass
	return master_grids_dict	

def master_tweet_processor(comm, input_file,grids_file,num_of_files):

	# Initialise master attributes
	rank = comm.Get_rank()
	size = comm.Get_size()

	# Create a grid data structure to update tweet data
	master_grids_dict = read_grid_file(grids_file)

	# Start processing the tweets and update the grids
	if num_of_files > size:
		if size >= 10:
			for mfile in [input_file+"00",input_file+str(size)]:
				process_tweets(comm,mfile,master_grids_dict)
		else:
			for mfile in [input_file+"00",input_file+"0"+str(size)]:
				process_tweets(comm,mfile,master_grids_dict)
	else:
		process_tweets(comm,input_file+"00",master_grids_dict)

	# If size > 1, master will request data from all the slaves
	if size > 1:
		# This will append processed data sent by every slave
		grids_dict_list = []
		for i in range(size-1):
			# Ask slaves to send back
			comm.send('return_data', dest=(i+1), tag=(i+1))

		for i in range(size-1):
			grids_dict_list.append(comm.recv(source=(i+1), tag=MASTER_RANK))

		for i in range(size-1):
			# Ask slaves to quit their process
			comm.send('exit', dest=(i+1), tag=(i+1))

		# Master will start merging the gathered data with it's own
		master_grids_dict = master_merge(grids_dict_list,master_grids_dict)

		# Lambda function will sort every grid with maximum number of tweets
		master_grids_dict = sorted(master_grids_dict['rows'],key = lambda i:i['attributes']['num_of_tweets'],reverse=True)
		
		# This fuction will generate the reuired output
		format_output(master_grids_dict)

	# If master is alone, it will not perform merge operation
	else:

		# Lambda function will sort every grid with maximum number of tweets
		master_grids_dict = sorted(master_grids_dict['rows'],key = lambda i:i['attributes']['num_of_tweets'],reverse=True)
		
		# This fuction will generate the reuired output
		format_output(master_grids_dict)		
		
def slave_tweet_processor(comm,input_file,grids_file,num_of_files):

	# Initialise slave attributes
	rank = comm.Get_rank()
	size = comm.Get_size()

	# Create a grid data structure to update tweet data
	grids_dict = read_grid_file(grids_file)

	# Start processing the tweets and update the grids
	if rank < 10:
		process_tweets(comm,input_file+"0"+str(rank), grids_dict)
	else:
		process_tweets(comm,input_file+str(rank), grids_dict)

	# After processing the tweets, slave will wait for master to request processed data
	while True:
		in_comm = comm.recv(source=MASTER_RANK, tag=rank)
		if isinstance(in_comm, str):
			if in_comm in ("return_data"):
				# Send data back
				comm.send(grids_dict, dest=MASTER_RANK, tag=MASTER_RANK)
			elif in_comm in ("exit"):
				# End the program for slave
				exit(0)

def main():

	# Accept the arguments from the command line
	try:
		grids_file = sys.argv[1]
		num_of_files = int(sys.argv[2])
		file_prefix = sys.argv[3]
	except:
		# Default values
		num_of_files = 1
		file_prefix = 'temp_'
		print ("Please provide the required parameters:")
		print ("1. Grid file name")
		print ("2. Number of files (Default: 1)")
		print ("3. Prefix of the files (Default: temp_)")
		exit(0)

	# Initialize the MPI call
	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()
	size = comm.Get_size()
		
	# If size > 1 use master/slave architecture
	if size > 1:
		if rank == 0 :
			master_tweet_processor(comm, file_prefix, grids_file,num_of_files)
		else:
			slave_tweet_processor(comm, file_prefix,grids_file,num_of_files)
	# Else only master will do the work
	else:
		master_tweet_processor(comm, file_prefix, grids_file,num_of_files)
	

if __name__ == "__main__":
	main()
	end = time.time()
	print("Program Run Time-",round((end - start),4), "secs")
