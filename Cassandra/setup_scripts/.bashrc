export JAVA_HOME=~/jdk11
export PATH=$JAVA_HOME/bin:$PATH
export PYTHONPATH=~/Python
export CASS_DIR=/temp/teamd-cass/apache-cassandra-4.1.3/bin
export CITUS_DIR=/temp/teamd-data/project_files
export cassandra=$CASS_DIR/cassandra
export nodetool="$CASS_DIR/nodetool"

bstart() {
   if [ $# -lt 1 ]; then
      echo "Usage: bstart <node_name1> [<node_name2> ...]"
      return 1
   fi
   action="$1"
   shift
   node_names=("$@")
   ip_addresses=()

   for node_name in "${node_names[@]}"; do
      ip_address=$(getent hosts "$node_name" | awk '{print $1}')
      if [ -n "$ip_address" ]; then
         ip_addresses+=("$ip_address")
      fi
   done

   if [ ${#ip_addresses[@]} -eq 0 ]; then
      echo "No valid IP addresses found for the specified node names."
      return 1
   fi
   echo ${ip_addresses[@]}

   rm -rf cass_log
   mkdir -p cass_log
   sbatch batch_start1.sh $action "${ip_addresses[@]}"
   cd cass_log
}