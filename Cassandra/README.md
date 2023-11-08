# Supplier Distributed Database Design

# Cassandra
## Step 1
1. Before starting the Cassandra cluster, copy these files into the HOME directory:
   - app.py
   - calculate_result.py
   - initialize_csv_files.py
   - batch_start1.sh
   - install_cass.sh
   - start_cass.sh
   - startup.cql
   - load_data.cql
2. Download a stable version of Cassandra tarball into the HOME directory from https://cassandra.apache.org/_/download.html, we are using ```apache-cassandra-4.1.3-bin.tar.gz```.
3. Copy the content in .bashrc file and paste it into the .bashrc file in the HOME directory, and run ```source .bashrc```.

## Step 2
1. Identify 5 available nodes with at least 24 CPUS using ```sinfo```. You can check a node's hardware specs with ```scontrol show node <nodename>```
2. Replace ```#SBATCH --nodelist=xcnc[37-41]``` in batch_start1.sh with the desired nodes.
3. Run ```bstart init <node1> <node2> <node3> <node4> <node5>```, using the example above, our node1 will be xcnc37 and so on.
4. You should see a jobid returned. You may track the progress of tasks inside the batch job using ```sacct -j <job_id> --format=JobID,Start,End,Elapsed,REQCPUS,ALLOCTRES%30,Node```.
5. Wait for message like this to show up for successful startup on each node ```INFO  [Messaging-EventLoop-3-9] 2023-11-07 17:21:01,003 OutboundConnection.java:1153 - /192.168.48.189:7000(/192.168.48.189:58814)->/192.168.48.190:7000-LARGE_MESSAGES-f2d7811d successfully connected, version = 12, framing = LZ4, encryption = unencrypted```
6. Run ```scancel <jobid>``` to stop the batch job.

## Step 3
1. Run ```bstart run <node1> <node2> <node3> <node4> <node5>``` with the same set of nodes you have used in Step 2.
2. You should see Cassandra getting started on different nodes again, followed by creation of schema and data loading, and finally 20 concurrent client executions.
3. Batch job will auto shutdown Cassandra on all nodes after 1 hour.

## Step 4
1. Run ```bstart calculate <node1> <node2> <node3> <node4> <node5>```with the same set of nodes you have used in Step 2.
2. You should see Cassandra getting started on different nodes again, followed by computation of throughput.csv and dbstate.csv.
3. Batch job will auto shutdown Cassandra on all nodes after 30 minutes.

## Step 5
1. Run ```bstart collect <node1> <node2> <node3> <node4> <node5>```with the same set of nodes you have used in Step 2.
2. You should see the stdout being collected and copied in the cass_log directory from different nodes.

## Step 6
1. Run ```zip -r cass_log cass_log``` in HOME directory.
2. Run ```bash paste_file.sh``` on your local machine to copy the zipped cass_log to your local machine.

## Step 7:
This step will remove all the configuration and installation files from the /temp directory:
1. Run ```bstart delete <node1> <node2> <node3> <node4> <node5>```with the same set of nodes you have used in Step 2.
