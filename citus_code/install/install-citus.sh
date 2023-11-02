#!/usr/bin/env bash

# Script to install citus
#
# References
# https://wiki.postgresql.org/wiki/Compile_and_Install_from_source_code
# https://github.com/citusdata/citus/blob/main/CONTRIBUTING.md


# Download PostgreSQL source code
function download_postgres ()
{
	mkdir -p ${INSTALLDIR}
	cd $HOME
	wget https://ftp.postgresql.org/pub/source/v${PG_VERSION}/${PG_FILE}
	tar xvfz ${PG_FILE}
}

# Install PostgreSQL source code
function install_postgres ()
{
	if [ ! -d ${PG_SRCDIR} ]; then
		echo "ERROR: ${PG_SRCDIR} missing!"
		exit 1
	fi
	cd ${PG_SRCDIR}
	export CFLAGS="-O2"
	./configure --prefix=${INSTALLDIR}
	make clean
	make world
	make install
	# make install-docs
}

# Download Citus extension
function download_citus ()
{
	cd $HOME
	git clone https://github.com/citusdata/citus.git
}

# Install Citus extension
function install_citus ()
{
	if [ ! -d ${CITUS_SRCDIR} ]; then
		echo "ERROR: ${CITUS_SRCDIR} missing!"
		exit 1
	fi
	cd ${CITUS_SRCDIR}
	export PATH=${INSTALLDIR}/bin:$PATH
	./configure --prefix=${INSTALLDIR} --exec_prefix=${INSTALLDIR}
	make
	make install-all prefix=${INSTALLDIR} exec_prefix=${INSTALLDIR}
}



# Update environment variables
function update_bashrc ()
{
	cat >> ${BASHFILE} <<EOF
export PATH=${INSTALLDIR}/bin:\$PATH
export PGDATA=${DATADIR}
export PGDATABASE=project
export PGUSER=cs4224${TEAM_ID}
export PGPORT=${PORT_NUM}
EOF
}

PG_VERSION=15.3
PG_FILE=postgresql-${PG_VERSION}.tar.gz
PG_SRCDIR=$HOME/postgresql-${PG_VERSION}
CITUS_SRCDIR=$HOME/citus
# INSTALLDIR - directory containing installed binaries, libraries, etc.
INSTALLDIR=$HOME/pgsql
BASHFILE=$HOME/.bash_profile


TEAM_ID=d # IMPORTANT: change x to your actual team identifier (a/b/.../y/z) 
TEAM_ASCII=$(echo -n "${TEAM_ID}" | od -An -tuC)
PORT_NUM=$(( 5000+${TEAM_ASCII} ))
# DATADIR - directory containing database files
DATADIR=/temp/team${TEAM_ID}-data


download_postgres
install_postgres
download_citus
install_citus
update_bashrc 


