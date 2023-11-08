#!/bin/bash
# Check if the directory exists, if not create it
source .bashrc
dir="/temp/teamd-cass"
if [ -d "/temp/teamd-cass" ]; then
    rm -r "/temp/teamd-cass"
    echo "Folder '/temp/teamd-cass' has been removed."
else
    echo "Folder '/temp/teamd-cass' does not exist."
fi

if [ ! -d "$dir" ]; then
 mkdir "$dir"
 mkdir "$dir/client"
 cp ~/cass_data_files/apache-cassandra-4.1.3-bin.tar.gz /temp/teamd-cass/
 tar -xvzf /temp/teamd-cass/apache-cassandra-4.1.3-bin.tar.gz -C /temp/teamd-cass/
 cd $CASS_DIR
 mkdir data_files
 mkdir xact_files

 cp -r ~/cass_data_files/data_files $CASS_DIR/
 cp -r ~/cass_data_files/xact_files $CASS_DIR/
 cp ~/cass_data_files/app.py $CASS_DIR/xact_files/
 cp ~/cass_data_files/calculate_result.py $CASS_DIR/xact_files/
 cp ~/cass_data_files/initialize_csv_files.py $CASS_DIR/data_files/
 cp ~/cass_data_files/load_data.cql $CASS_DIR/data_files/
 cp ~/cass_data_files/startup.cql $CASS_DIR/data_files/
 pip install pytz
 python $CASS_DIR/data_files/initialize_csv_files.py
fi

node_ip=$(hostname -I | awk '{print $1}')
echo "node ip: $node_ip"
seed_ip1=$1
seed_ip2=$2
seed_ip3=$3
echo "Seed ip: $seed_ip1 $seed_ip2 $seed_ip3"

sed -i "s/listen_address:.*/listen_address: $node_ip/" /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra.yaml
sed -i "s/rpc_address:.*/rpc_address: $node_ip/" /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra.yaml
sed -i "s/read_request_timeout:.*/read_request_timeout: 300000ms/" /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra.yaml
sed -i "s/range_request_timeout:.*/range_request_timeout: 300000ms/" /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra.yaml
sed -i "s/write_request_timeout:.*/write_request_timeout: 300000ms/" /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra.yaml
sed -i "s/request_timeout:.*/request_timeout: 300000ms/" /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra.yaml
sed -i "s/endpoint_snitch:.*/endpoint_snitch: GossipingPropertyFileSnitch/" /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra.yaml
sed -i "s/materialized_views_enabled:.*/materialized_views_enabled: true/" /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra.yaml
sed -i "s/- seeds:.*/- seeds: \"$seed_ip1,$seed_ip2,$seed_ip3\"/" /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra.yaml
#sed -i 's/^#MAX_HEAP_SIZE.*/MAX_HEAP_SIZE="4G"/' /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra-env.sh
#sed -i 's/^#HEAP_NEWSIZE.*/HEAP_NEWSIZE="800M"/' /temp/teamd-cass/apache-cassandra-4.1.3/conf/cassandra-env.sh
echo "Finish setting up cassandra.yaml in $hostname"

