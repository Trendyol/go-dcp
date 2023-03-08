# Go Dcp Client

This repository contains go implementation of a Couchbase Database Change Protocol (DCP) client.

### Contents
---

* [Why?](#why)
* [Usage](#usage)
* [Configuration](#configuration)
* [Examples](#examples)

### Why?

+ Our main goal is to build a dcp client for faster and stateful systems. We want to use this repository in below
  implementations:
    + Couchbase Elastic Connector
    + Kafka Connector

---

### Example

```go
package main

import (
	"log"

	"github.com/Trendyol/go-dcp-client"
)

func listener(ctx *godcpclient.ListenerContext) {
	switch event := ctx.Event.(type) {
	case godcpclient.DcpMutation:
		log.Printf("mutated(%v) | id: %v, value: %v | isCreated: %v", event.VbID, string(event.Key), string(event.Value), event.IsCreated())
	case godcpclient.DcpDeletion:
		log.Printf("deleted | id: %v", string(event.Key))
	case godcpclient.DcpExpiration:
		log.Printf("expired | id: %v", string(event.Key))
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

---

### Configuration

| Variable                            | Type                        | Is Required |
|-------------------------------------|-----------------------------|-------------|
| `hosts`                             | array                       | yes         |
| `username`                          | string                      | yes         |
| `password`                          | string                      | yes         |
| `bucketName`                        | string                      | yes         |
| `scopeName`                         | string                      | no          |
| `collectionNames`                   | array                       | no          |
| `metadataBucket`                    | string                      | no          |
| `dcp.group.name`                    | string                      | yes         |
| `dcp.group.membership.type`         | string                      | yes         |
| `dcp.group.membership.memberNumber` | integer                     | no          |
| `dcp.group.membership.totalMembers` | integer                     | no          |
| `api.port`                          | integer                     | no          |
| `metric.enabled`                    | boolean *(true/false)*      | no          |
| `metric.path`                       | string                      | no          |
| `leaderElection.enabled`            | boolean *(true/false)*      | no          |
| `leaderElection.type`               | string                      | no          |
| `leaderElection.config`             | string/string key value map | no          |
| `leaderElection.rpc.port`           | integer                     | no          |
| `logger.level`                      | string                      | no          |
| `checkpoint.type`                   | string                      | no          |
| `checkpoint.interval`               | integer                     | no          |
| `checkpoint.timeout`                | integer                     | no          |

---

### Examples

- [example with static membership](example/main.go)
- [static membership config](example/config.yml)
- [kubernetesStatefulSet membership config](example/config_k8s_stateful_set.yml)
- [kubernetesHa membership config](example/config_k8s_leader_election.yml)
- [couchbase membership config](example/config_couchbase.yml) - thanks to @onursak
