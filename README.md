# Go Dcp Client [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp-client.svg)](https://pkg.go.dev/github.com/Trendyol/go-dcp-client) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-dcp-client)](https://goreportcard.com/report/github.com/Trendyol/go-dcp-client)

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
	"github.com/Trendyol/go-dcp-client"
	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/models"
)

func listener(ctx *models.ListenerContext) {
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		logger.Log.Printf("mutated(vb=%v) | id: %v, value: %v | isCreated: %v", event.VbID, string(event.Key), string(event.Value), event.IsCreated())
	case models.DcpDeletion:
		logger.Log.Printf("deleted(vb=%v) | id: %v", event.VbID, string(event.Key))
	case models.DcpExpiration:
		logger.Log.Printf("expired(vb=%v) | id: %v", event.VbID, string(event.Key))
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

| Variable                              |       Type        | Required |  Default   | Description                                                                                                         |
|---------------------------------------|:-----------------:|:--------:|:----------:|---------------------------------------------------------------------------------------------------------------------|
| `hosts`                               |     []string      |   yes    |     -      | Couchbase host like `localhost:8091`.                                                                               |
| `username`                            |      string       |   yes    |     -      | Couchbase username.                                                                                                 |
| `password`                            |      string       |   yes    |     -      | Couchbase password.                                                                                                 |
| `bucketName`                          |      string       |   yes    |     -      | Couchbase DCP bucket.                                                                                               |
| `secureConnection`                    |       bool        |    no    |   false    | Enable TLS connection of Couchbase.                                                                                 |
| `rootCAPath`                          |      string       |    no    |  *not set  | if `secureConnection` set `true` this field is required.                                                            |
| `scopeName`                           |      string       |    no    |  _default  | Couchbase scope name.                                                                                               |
| `collectionNames`                     |     []string      |    no    |  _default  | Couchbase collection names.                                                                                         |
| `debug`                               |       bool        |    no    |   false    | For debugging purpose.                                                                                              |
| `dcp.bufferSize`                      |        int        |    no    |  16777216  | Go DCP listener pre-allocated buffer size. `16mb` is default. Check this if you get OOM Killed.                     |
| `dcp.connectionBufferSize`            |       uint        |    no    |  20971520  | [gocbcore](github.com/couchbase/gocbcore) library buffer size. `20mb` is default. Check this if you get OOM Killed. |
| `dcp.connectionTimeout`               |   time.Duration   |    no    |     5s     | DCP connection timeout.                                                                                             |
| `dcp.listener.bufferSize`             |       uint        |    no    |     1      | Go DCP listener buffered channel size.                                                                              |
| `dcp.group.name`                      |      string       |   yes    |            | DCP group name for vbuckets.                                                                                        |
| `dcp.group.membership.type`           |      string       |    no    | couchbase  | DCP membership types. `couchbase`, `kubernetesHa`, `kubernetesStatefulSet` or `static`. Check examples for details. |
| `dcp.group.membership.memberNumber`   |        int        |    no    |     1      | Set this if membership is `static`. Other methods will ignore this field.                                           |
| `dcp.group.membership.totalMembers`   |        int        |    no    |     1      | Set this if membership is `static`. Other methods will ignore this field.                                           |
| `dcp.group.membership.rebalanceDelay` |   time.Duration   |    no    |    20s     | Works for autonomous mode.                                                                                          |
| `leaderElection.enabled`              |       bool        |    no    |   false    | Set this true for memberships  `kubernetesHa`.                                                                      |
| `leaderElection.type`                 |      string       |    no    | kubernetes |                                                                                                                     |
| `leaderElection.config`               | map[string]string |    no    |  *not set  | Set lease key-values like `leaseLockName`,`leaseLockNamespace`.                                                     |
| `leaderElection.rpc.port`             |        int        |    no    |    8081    | This field is usable for `kubernetesStatefulSet` membership.                                                        |
| `checkpoint.type`                     |      string       |    no    |    auto    | Set checkpoint type `auto` or `manual`.                                                                             |
| `checkpoint.autoReset`                |      string       |    no    |  earliest  | Set checkpoint start point to `earliest` or `latest`.                                                               |
| `checkpoint.interval`                 |   time.Duration   |    no    |    20s     | Checkpoint checking interval.                                                                                       |
| `checkpoint.timeout`                  |   time.Duration   |    no    |     5s     | Checkpoint checking timeout.                                                                                        |
| `healthCheck.enabled`                 |       bool        |    no    |    true    | Enable Couchbase connection health check.                                                                           |
| `healthCheck.interval`                |   time.Duration   |    no    |    20s     | Couchbase connection health checking interval duration.                                                             |
| `healthCheck.timeout`                 |   time.Duration   |    no    |     5s     | Couchbase connection health checking timeout duration.                                                              |
| `rollbackMitigation.enabled`          |       bool        |    no    |    true    | Enable reprocessing for roll-backed Vbucket offsets.                                                                |
| `metadata.type`                       |      string       |    no    | couchbase  | Metadata storing types.  `file` or `couchbase`.                                                                     |
| `metadata.readOnly`                   |       bool        |    no    |   false    | Set this for debugging state purposes.                                                                              |
| `metadata.config`                     | map[string]string |    no    |  *not set  | Set key-values of config. `bucket` for `couchbase` type.                                                            |
| `api.enabled`                         |       bool        |    no    |    true    | Enable metric and debug pprof endpoints                                                                             |
| `api.port`                            |        int        |    no    |    8080    | Set API port                                                                                                        |
| `metric.path`                         |      string       |    no    |  /metrics  | Set metric endpoint path.                                                                                           |
| `metric.averageWindowSec`             |      float64      |    no    |    10.0    | Set metric window range.                                                                                            |

### Examples

- [example with couchbase membership](example/main.go)
- [couchbase membership config](example/config.yml) - thanks to [@onursak](https://github.com/onursak)
- [kubernetesStatefulSet membership config](example/config_k8s_stateful_set.yml)
- [kubernetesHa membership config](example/config_k8s_leader_election.yml)
- [static membership config](example/config_static.yml)
