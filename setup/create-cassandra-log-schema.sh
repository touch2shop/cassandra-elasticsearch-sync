#!/usr/bin/env bash

set -e

cql_file_name="create-log-schema.cql"
cql_download_url="https://raw.github.com/felipead/cassandra-logger/v0.2/${cql_file_name}"

if [ ! $1 ]; then
	echo "First argument should be a Cassandra 2.1+ root directory. You need to have write access to it."
	exit 1
fi

cassandra_dir=${1%/}
if [ ! -d ${cassandra_dir} ]; then
	echo "Directory does not exist - ${cassandra_dir}"
	exit 1
fi

user=`whoami`
cassandra_pid=`pgrep -u ${user} -f cassandra || true`
if [ -z "${cassandra_pid}" ]; then
    echo "Cassandra is not running. Please start it and run this script again."
    exit 1
fi

wget ${cql_download_url}

echo "Loading CQL Script into Cassandra..."
${cassandra_dir}/bin/cqlsh --file ${cql_file_name}

echo "Done."
exit 0