#!/bin/bash

POSITIONAL_ARGS=()

VERSION=7.6.3
NAME_PREFIX="couchbase_node_"
SUBNET="13.37.11.0/24"
TOTAL_NODE=3
REST_PORT=8091
MEMORY_AS_MB=512
USERNAME=user
PASSWORD=123456
HOSTS_PATH=""

while [[ $# -gt 0 ]]; do
  case $1 in
    -v|--version)
      VERSION="$2"
      shift
      shift;;
    -np|--name-prefix)
      NAME_PREFIX="$2"
      shift
      shift;;
    -s|--subnet)
      SUBNET="$2"
      shift
      shift;;
    -tn|--total-node)
      TOTAL_NODE="$2"
      shift
      shift;;
    -rp|--rest-port)
      REST_PORT="$2"
      shift
      shift;;
    -mam|--memory-as-mb)
      MEMORY_AS_MB="$2"
      shift
      shift;;
    -u|--username)
      USERNAME="$2"
      shift
      shift;;
    -p|--password)
      PASSWORD="$2"
      shift
      shift;;
    -hp|--hosts-path)
      HOSTS_PATH="$2"
      shift
      shift;;
    -*|--*)
      echo "Unknown option $1"
      exit 1;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}"

echo "NAME_PREFIX = $NAME_PREFIX"
echo "VERSION = $VERSION"
echo "SUBNET = $SUBNET"
echo "TOTAL_NODE = $TOTAL_NODE"
echo "REST_PORT = $REST_PORT"
echo "MEMORY_AS_MB = $MEMORY_AS_MB"
echo "USERNAME = $USERNAME"
echo "PASSWORD = $PASSWORD"
echo "HOSTS_PATH = $HOSTS_PATH"
echo "---------------------------"

MASTER=""
KNOWN_NODES=""

echo "creating network"
docker network create couchbase --subnet=$SUBNET

check_db() {
  URL="http://$1:$REST_PORT/pools"
  curl --silent --max-time 5 "$URL" > /dev/null
  echo $?
}

get_buckets() {
  URL="http://$1:$REST_PORT/pools/default/buckets"
  RESPONSE=$(curl --silent --max-time 5 "$URL" -X 'GET' -u "$USERNAME:$PASSWORD")
  echo "$RESPONSE"
}

get_rebalance_logs() {
  URL="http://$1:$REST_PORT/logs/rebalanceReport"
  RESPONSE=$(curl --silent --max-time 5 "$URL" -X 'GET' -u "$USERNAME:$PASSWORD")
  echo "$RESPONSE"
}

# url, data
do_post() {
  RESPONSE=$(curl -s -w ",status_code_%{http_code}" "$1" -X 'POST' -u "$USERNAME:$PASSWORD" --data "$2")
  if [[ "$RESPONSE" == *"status_code_200" ]] || [[ "$RESPONSE" == *"status_code_202" ]]; then
    echo "$RESPONSE"
  else
    echo "error when POST $1 with $2 = $RESPONSE"
  fi
}

install_travel_sample() {
  URL="http://$1:$REST_PORT/sampleBuckets/install"
  RESPONSE=$(do_post "$URL" '["travel-sample"]')
  echo "install_travel_sample > $RESPONSE"
}

node_init() {
  URL="http://$1:$REST_PORT/nodeInit"
  RESPONSE=$(do_post "$URL" "hostname=$1&dataPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&indexPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&eventingPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&javaHome=&analyticsPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata")
  echo "node_init > $RESPONSE"
}

create_cluster() {
  URL="http://$1:$REST_PORT/clusterInit"
  RESPONSE=$(do_post "$URL" "hostname=$1&dataPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&indexPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&eventingPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&sendStats=false&services=kv&analyticsPath=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&javaHome=&clusterName=home&memoryQuota=$MEMORY_AS_MB&afamily=ipv4&afamilyOnly=false&nodeEncryption=off&username=$USERNAME&password=$PASSWORD&port=SAME")
  echo "create_cluster > $RESPONSE"
}

join_cluster() {
  URL="http://$2:$REST_PORT/node/controller/doJoinCluster"
  RESPONSE=$(do_post "$URL" "hostname=$1&user=$USERNAME&password=$PASSWORD&newNodeHostname=$2&services=kv")
  echo "join_cluster > $RESPONSE"
}

rebalance() {
  URL="http://$1:$REST_PORT/controller/rebalance"
  RESPONSE=$(do_post "$URL" "knownNodes=$2&ejectedNodes=")
  echo "rebalance > $RESPONSE"
}

echo "creating nodes"
for (( i=1; i<=TOTAL_NODE; i++ ))
do
  NAME="$NAME_PREFIX$i"
  docker run -d --pull missing --name $NAME --net couchbase "couchbase:$VERSION"
done

echo "waiting nodes"
for (( i=1; i<=TOTAL_NODE; i++ ))
do
  NAME="$NAME_PREFIX$i"
  IP=$(docker inspect "$NAME" -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
  KNOWN_NODES="${KNOWN_NODES}ns_1%40${IP}"
  if [ "$i" != "$TOTAL_NODE" ]; then
    KNOWN_NODES="${KNOWN_NODES}%2C"
  fi

  echo "$NAME IP=$IP"

  until [[ $(check_db "$IP") = 0 ]]; do
    >&2 echo "waiting $NAME to be available"
    sleep 5
  done

  echo "$NAME ready"

  if [ "$i" == 1 ]; then
    echo "creating cluster"
    create_cluster "$IP"
    MASTER="$IP"
  else
    echo "$NAME joining cluster"
    node_init "$IP"
    join_cluster "$MASTER" "$IP"
  fi
done

echo "rebalancing"

rebalance "$MASTER" "$KNOWN_NODES"

until [[ $(get_rebalance_logs "$MASTER") == *"Rebalance completed"* ]]; do
  >&2 echo "waiting rebalance to be done"
  sleep 5
done

echo "rebalance done"

echo "installing travel-sample"

install_travel_sample "$MASTER"

until [[ $(get_buckets "$MASTER") == *"\"itemCount\":63288"* ]]; do
  >&2 echo "waiting travel-sample to be installed"
  sleep 15
done

echo "travel-sample installed"

URL="http://$MASTER:$REST_PORT"

echo "---------------------------"
echo "username $USERNAME"
echo "password $PASSWORD"
echo "---------------------------"
echo "ui       $URL"
echo "---------------------------"
RAW=""
HOSTS="hosts:"
HOSTS_AS_GO_ARR="[]string{"
for (( i=1; i<=TOTAL_NODE; i++ ))
do
  NAME="$NAME_PREFIX$i"
  IP=$(docker inspect "$NAME" -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
  RAW="$RAW$IP:$REST_PORT"
  HOSTS="$HOSTS\n  - $IP:$REST_PORT"
  HOSTS_AS_GO_ARR="${HOSTS_AS_GO_ARR}\"$IP:$REST_PORT\""
  if [ "$i" != "$TOTAL_NODE" ]; then
    RAW="$RAW\n"
    HOSTS_AS_GO_ARR="${HOSTS_AS_GO_ARR}, "
  fi
done
HOSTS_AS_GO_ARR="${HOSTS_AS_GO_ARR}}"
echo "$HOSTS"
echo "---------------------------"
echo "$HOSTS_AS_GO_ARR"
echo "---------------------------"

if [ "$HOSTS_PATH" != "" ]; then
  echo "writing hosts to $HOSTS_PATH"
  echo -e "$RAW" >> "$HOSTS_PATH"
fi