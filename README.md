# Supplier Distributed Database Design

## Cassandra
Before starting the Cassandra cluster, we need to compile the csv files needed for data loading
1. ssh to node that you will be loading the data into Cassandra server. In our case, it is xcnd32.
2. All of the data files provided should reside in /temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files
3. Place initialize_csv_files.py inside the above folder.
4. Run ```python initialize_csv_files.py```
5. You should see several df.csv files generated

We are ready to run the program now, following these steps:
1. From any node that you place batch_start1.sh in, run ```mkdir cass_log && sbatch batch_start1.sh```
2. This should submit a slurm batch job to
    - a. start Cassandra servers in the nodes specified in the script,
    - b. create schema 
    - c. load data from /temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files
    - d. run client servers - app.py on all the nodes
    - e. generate result reports into /temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/results/ 
    - f. sleep for 3600s and then shut down
3. run ```sacct -j <job_id> --format=JobID,Start,End,Elapsed,REQCPUS,ALLOCTRES%30,Node``` to check task status in the batch job.
4. From local machine, run the following shell script to copy out the generated reports. This would copy the directory into your Desktop.
```
scp -r cs4224d@192.168.48.249:/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/results ~/Desktop/results_xcnd30
scp -r cs4224d@192.168.48.250:/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/results ~/Desktop/results_xcnd31
scp -r cs4224d@192.168.48.251:/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/results ~/Desktop/results_xcnd32
scp -r cs4224d@192.168.48.252:/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/results ~/Desktop/results_xcnd33
scp -r cs4224d@192.168.48.253:/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/results ~/Desktop/results_xcnd34
```