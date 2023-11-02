#!/bin/bash
remote_user="cs4224d"
remote_password="Test@123"
remote_host="xlog0.comp.nus.edu.sg"
local_dir="/mnt/c/Users/Jan/source/repos/cass-supplier/cass_stats"
remote_dir="/home/stuproj/cs4224d/cass_log/"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
new_local_dir="${local_dir}/${TIMESTAMP}"

sshpass -p "$remote_password" scp -r "${remote_user}@${remote_host}:${remote_dir}${remote_file}" "${new_local_dir}"
echo "done!"