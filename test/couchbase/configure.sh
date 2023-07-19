#!/bin/bash

# Enables job control
set -m

# Enables error propagation
set -e

# Run the server and send it to the background
/entrypoint.sh couchbase-server &

# Check if couchbase server is up
check_db() {
  curl --silent http://127.0.0.1:8091/pools > /dev/null
  echo $?
}

# Variable used in echo
i=1
# Echo with
log() {
  echo "[$i] [$(date +"%T")] $@"
  i=`expr $i + 1`
}

# Wait until it's ready
until [[ $(check_db) = 0 ]]; do
  >&2 log "Waiting for Couchbase Server to be available ..."
  sleep 1
done

couchbase-cli cluster-init -c localhost --cluster-name Cluster --cluster-username user \
  --cluster-password 123456 --services data --cluster-ramsize 1024

couchbase-cli bucket-create -c couchbase --username user --password 123456 --bucket dcp-test --bucket-type couchbase --bucket-ramsize 1024

cbimport json -c couchbase://127.0.0.1 -u user -p 123456 --bucket-quota 1024 -b dcp-test -d file://opt/couchbase/samples/travel-sample.zip -f sample

echo "couchbase-dev started"

fg 1
