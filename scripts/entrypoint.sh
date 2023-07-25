#!/bin/bash

rm -rf /opt/couchbase/var/lib/couchbase/config/config.dat

echo '{rest_port, '${REST_PORT}'}.' >> /opt/couchbase/etc/couchbase/static_config
echo '{capi_port, '${CAPI_PORT}'}.' >> /opt/couchbase/etc/couchbase/static_config
echo '{query_port, '${QUERY_PORT}'}.' >> /opt/couchbase/etc/couchbase/static_config
echo '{fts_http_port, '${FTS_PORT}'}.' >> /opt/couchbase/etc/couchbase/static_config
echo '{memcached_ssl_port, '${MEMCACHED_SSL_PORT}'}.' >> /opt/couchbase/etc/couchbase/static_config
echo '{memcached_port, '${MEMCACHED_PORT}'}.' >> /opt/couchbase/etc/couchbase/static_config
echo '{ssl_rest_port, '${SSL_REST_PORT}'}.' >> /opt/couchbase/etc/couchbase/static_config

sleep 1

set -m

/entrypoint.sh couchbase-server &

check_db() {
  curl --silent http://127.0.0.1:${REST_PORT}/pools > /dev/null
  echo $?
}

check_cluster_init(){
	bash /opt/couchbase/bin/couchbase-cli cluster-init -c 127.0.0.1:${REST_PORT} \
      --cluster-username ${USERNAME} --cluster-password ${PASSWORD} \
      --services data,index,query,fts,analytics,eventing \
      --cluster-ramsize ${CLUSTER_RAMSIZE} --cluster-index-ramsize ${CLUSTER_INDEX_RAMSIZE} \
      --cluster-eventing-ramsize ${CLUSTER_EVENTING_RAMSIZE} --cluster-fts-ramsize ${CLUSTER_FTS_RAMSIZE} \
      --cluster-analytics-ramsize ${CLUSTER_ANALYTICS_RAMSIZE} --index-storage-setting ${INDEX_STORAGE_SETTING} > /dev/null
  echo $?
}

until [[ $(check_db) = 0 ]]; do
  >&2 echo "Waiting for Couchbase Server to be available"
  sleep 1
done

echo "Couchbase server is online"

until [[ $(check_cluster_init) = 0 ]]; do
  >&2 echo "Waiting for cluster init"
  sleep 1
done

echo "Cluster initialized"

bash /opt/couchbase/bin/couchbase-cli user-manage -c 127.0.0.1:${REST_PORT} -u ${USERNAME} -p ${PASSWORD} \
    --set --rbac-username ${BUCKET_NAME} --rbac-password ${PASSWORD} \
    --rbac-name ${BUCKET_NAME} --roles admin --auth-domain local
sleep 3

echo "Bucket create"
bash /opt/couchbase/bin/couchbase-cli bucket-create -c 127.0.0.1:${REST_PORT} --username ${USERNAME} --password ${PASSWORD} \
--bucket ${BUCKET_NAME} --bucket-type ${BUCKET_TYPE} --bucket-ramsize ${BUCKET_RAMSIZE} --enable-flush 1 --bucket-replica 0 --wait

sleep 3

# Attach to couchbase entrypoint
echo "Attaching to couchbase-server entrypoint"
echo "couchbase-dev started"
fg 1