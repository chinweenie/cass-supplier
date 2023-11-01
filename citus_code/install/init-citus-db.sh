#!/usr/bin/env bash

# Install Citus database directory  & configure Citus extension

# INSTALLDIR - directory containing installed binaries, libraries, etc.
INSTALLDIR=$HOME/pgsql

TEAM_ID=d # IMPORTANT: change x to your actual team identifier
# DATADIR - directory containing database files
DATADIR=/temp/team${TEAM_ID}-data


mkdir -p ${DATADIR}
${INSTALLDIR}/bin/initdb -D ${DATADIR}
if [ -e ${DATADIR}/postgresql.conf ]; then
	grep -q citus ${DATADIR}/postgresql.conf
	if [ $? -ne 0 ]; then
		echo "shared_preload_libraries = 'citus'" >> ${DATADIR}/postgresql.conf
	fi
else
	echo "ERROR: ${DATADIR}/postgresql.conf missing!"
fi
