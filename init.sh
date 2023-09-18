#!/bin/bash

# Define the path to the startup CQL file
cql_file="./setup_scripts/startup.cql"

# Check if the CQL file exists
if [ -f "$cql_file" ]; then
  # Run cqlsh and execute commands from the CQL file
  cqlsh cassandra1 9042 -f "$cql_file"
else
  echo "Error: $cql_file not found."
fi