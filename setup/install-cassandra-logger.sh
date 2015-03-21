#!/usr/bin/env bash

set -e

jar_file_name="cassandra-logger-0.2.jar"
jar_download_url="https://github.com/felipead/cassandra-logger/releases/download/v0.2/${jar_file_name}"

if [ ! $1 ]; then
	echo "First argument should be a Cassandra 2.1+ root directory. You need to have write access to it."
	exit 1
fi

cassandra_dir=${1%/}
if [ ! -d ${cassandra_dir} ]; then
	echo "Directory does not exist - ${cassandra_dir}"
	exit 1
fi

cassandra_conf_dir=${cassandra_dir}/conf
cassandra_triggers_dir=${cassandra_conf_dir}/triggers
if [ ! -d ${cassandra_triggers_dir} ]; then
	echo "Triggers directory does not exist ($cassandra_triggers_dir)."
	echo "Are you sure this is a valid Cassandra 2.1+ installation?"
	exit 1
fi

wget ${jar_download_url} -O ${jar_file_name} && cp ${jar_file_name} ${cassandra_triggers_dir}

echo "The trigger was successfully installed."

if [ ! ${CONTINUOUS_INTEGRATION} ]; then
    user=`whoami`
    cassandra_pid=`pgrep -u ${user} -f cassandra || true`
    if [ ! -z "${cassandra_pid}" ]; then
        echo "Cassandra is running for current user with PID ${cassandra_pid}. Atempting to reload triggers..."
        if ${cassandra_dir}/bin/nodetool -h localhost reloadtriggers; then
            echo "Trigger loaded successfuly. You can already use it on the CQL sheel."
        else
            echo "Something went wrong. Could not reload triggers. Try restarting Cassandra manually."
            exit 1
        fi
    fi
fi

exit 0