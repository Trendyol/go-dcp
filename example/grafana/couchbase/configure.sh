#!/bin/bash

# Start Couchbase server in background
/entrypoint.sh couchbase-server &

# Wait for Couchbase to start
sleep 10

# Setup cluster
couchbase-cli cluster-init -c 127.0.0.1:8091 \
  --cluster-username user \
  --cluster-password password \
  --services data,index,query,fts,eventing,analytics \
  --cluster-ramsize 1024 \
  --cluster-index-ramsize 256 \
  --cluster-fts-ramsize 256 \
  --cluster-eventing-ramsize 256 \
  --cluster-analytics-ramsize 1024

# Create a bucket
couchbase-cli bucket-create -c 127.0.0.1:8091 \
  --username user \
  --password password \
  --bucket dcp-test \
  --bucket-type couchbase \
  --bucket-ramsize 256

# Keep the container running
tail -f /dev/null
