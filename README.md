# Supplier Distributed Database Design

## Cassandra
Assuming that all the Cassandra servers have been started in the designated nodes,
execute the following script to start the client servers for randomized transaction inputs.
```
bash run_clients.sh
```
The execution includes randomly assigning the 20 transaction files to the 5 servers, 
and invoke the app.py main function in each of the server. The main functions would 
first establish connection to the Cassandra server, then scan through their assigned 
transaction files to execute the read or write transactions.