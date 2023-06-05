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

# Setup index and memory quota
log "$(date +"%T") Init cluster ........."
couchbase-cli cluster-init -c 127.0.0.1:8091 --cluster-username $USERNAME --cluster-password $PASSWORD \
  --cluster-name $CLUSTER_NAME --cluster-ramsize 1024 --cluster-index-ramsize 512 --services data,index,query,fts

echo "cbloader"
cbdocloader -c couchbase://127.0.0.1 -u $USERNAME -p $PASSWORD -m 1024 -b dcp-test -d /opt/couchbase/samples/travel-sample.zip

echo "couchbase-dev started"

fg 1
