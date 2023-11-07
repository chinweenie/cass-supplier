#!/bin/bash
remote_user="cs4224d"
remote_password="Test@123"
remote_host="xlog0.comp.nus.edu.sg"
local_dir="/Users/chinweenie/Desktop/CassandraProj/cass_stats"
remote_dir="/home/stuproj/cs4224d/cass_log.zip"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
new_local_dir="${local_dir}/${TIMESTAMP}_cass_log.zip"

sshpass -p "$remote_password" scp "${remote_user}@${remote_host}:${remote_dir}" "${new_local_dir}"

#remote_addresses=("192.168.48.249" "192.168.48.250" "192.168.48.251" "192.168.48.252" "192.168.48.253")
#files=("stderr" "stdout")
#TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
#new_local_dir="${local_dir}/${TIMESTAMP}"
#mkdir -p "$new_local_dir"
#
## Perform the copy
#for remote_host in "${remote_addresses[@]}"; do
#    for remote_file in "${files[@]}"; do
#        # Determine the node's name based on its address
#        case $remote_host in
#            "192.168.48.249")
#                node="xcnd30"
#                ;;
#            "192.168.48.250")
#                node="xcnd31"
#                ;;
#            "192.168.48.251")
#                node="xcnd32"
#                ;;
#            "192.168.48.252")
#                node="xcnd33"
#                ;;
#            "192.168.48.253")
#                node="xcnd34"
#                ;;
#            *)
#                echo "Unknown remote host: $remote_host"
#                continue
#                ;;
#        esac
#        # Create a local file name by appending the node's name.
#        local_filename="${remote_file}_${node}"
#        sshpass -p "$remote_password" scp "${remote_user}@${remote_host}:${remote_dir}${remote_file}" "${new_local_dir}/${local_filename}"
#    done
#done

