#!/bin/bash

echo "deleting nodes"

NODES=$(docker network inspect couchbase --format '{{range .Containers}}{{printf "%s\n" .Name}}{{end}}')

echo "$NODES" | while read -r NAME ; do
   docker rm -f "$NAME"
done

echo "deleting network"

docker network rm couchbase