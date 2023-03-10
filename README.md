# Go Dcp Client

This repository contains go implementation of a Couchbase Database Change Protocol (DCP) client.

### Contents

* [Why?](#why)
* [Usage](#usage)
* [Configuration](#configuration)
* [Examples](#examples)

### Why?

+ Our main goal is to build a dcp client for faster and stateful systems. We're already using this repository in below
  implementations:
    + [Elastic Connector](https://github.com/Trendyol/go-elasticsearch-connect-couchbase)
    + [Kafka Connector](https://github.com/Trendyol/go-kafka-connect-couchbase)

### Example

```go
package main

import (
	"log"

	"github.com/Trendyol/go-dcp-client"

	"github.com/Trendyol/go-dcp-client/models"
)

func listener(ctx *models.ListenerContext) {
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		log.Printf("mutated(vb=%v) | id: %v, value: %v | isCreated: %v", event.VbID, string(event.Key), string(event.Value), event.IsCreated())
	case models.DcpDeletion:
		log.Printf("deleted(vb=%v) | id: %v", event.VbID, string(event.Key))
	case models.DcpExpiration:
		log.Printf("expired(vb=%v) | id: %v", event.VbID, string(event.Key))
	}

	ctx.Ack()
}

func main() {
	dcp, err := godcpclient.NewDcp("config.yml", listener)
	if err != nil {
		panic(err)
	}

	defer dcp.Close()

	dcp.Start()
}
```

### Usage

```
$ go get github.com/Trendyol/go-dcp-client

```

### Configuration

| Variable                            | Type              | Required    | Default               |
|-------------------------------------|-------------------|-------------|-----------------------|
| `hosts`                             | []string          | yes         |                       |
| `username`                          | string            | yes         |                       |
| `password`                          | string            | yes         |                       |
| `bucketName`                        | string            | yes         |                       |
| `dcp.group.name`                    | string            | yes         |                       |
| `dcp.group.membership.type`         | string            | yes         |                       |
| `scopeName`                         | string            | no          | _default              |
| `collectionNames`                   | []string          | no          | _default              |
| `metadataBucket`                    | string            | no          | *same with bucketName |
| `metadataScope`                     | string            | no          | _default              |
| `metadataCollection`                | string            | no          | _default              |
| `dcp.bufferSizeKb`                  | int               | no          | 16384                 |
| `dcp.listener.bufferSize`           | int               | no          | 1                     |
| `dcp.group.membership.memberNumber` | int               | no          | *not set              |
| `dcp.group.membership.totalMembers` | int               | no          | *not set              |
| `api.port`                          | int               | no          | 8080                  |
| `api.enabled`                       | bool              | no          | true                  |
| `metric.averageWindowSec`           | float64           | no          | 10.0                  |
| `metric.path`                       | string            | no          | /metrics              |
| `leaderElection.enabled`            | bool              | no          | false                 |
| `leaderElection.type`               | string            | no          | *not set              |
| `leaderElection.config`             | map[string]string | no          | *not set              |
| `leaderElection.rpc.port`           | int               | no          | 8081                  |
| `logger.level`                      | string            | no          | info                  |
| `checkpoint.type`                   | string            | no          | auto                  |
| `checkpoint.interval`               | time.Duration     | no          | 10s                   |
| `checkpoint.timeout`                | time.Duration     | no          | 5s                    |
| `healthCheck.interval`              | time.Duration     | no          | 10s                   |
| `healthCheck.timeout`               | time.Duration     | no          | 5s                    |

### Examples

- [example with static membership](example/main.go)
- [static membership config](example/config.yml)
- [kubernetesStatefulSet membership config](example/config_k8s_stateful_set.yml)
- [kubernetesHa membership config](example/config_k8s_leader_election.yml)
- [couchbase membership config](example/config_couchbase.yml) - thanks to [@onursak](https://github.com/onursak)
