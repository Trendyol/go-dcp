# Go Dcp [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp.svg)](https://pkg.go.dev/github.com/Trendyol/go-dcp) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-dcp)](https://goreportcard.com/report/github.com/Trendyol/go-dcp) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/go-dcp/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/go-dcp)

This repository contains go implementation of a Couchbase Database Change Protocol (DCP) client.

### Contents

* [Why?](#why)
* [Usage](#usage)
* [Configuration](#configuration)
* [Examples](#examples)

### Why?

+ Our main goal is to build a dcp client for faster and stateful systems. We're already using this repository in below
  implementations:
    + [Elastic Connector](https://github.com/Trendyol/go-dcp-elasticsearch)
    + [Kafka Connector](https://github.com/Trendyol/go-dcp-kafka)
    + [Couchbase Connector](https://github.com/Trendyol/go-dcp-couchbase)
    + [SQL Connector](https://github.com/Trendyol/go-dcp-sql)

### Example

```go
package main

import (
  "github.com/Trendyol/go-dcp"
  "github.com/Trendyol/go-dcp/logger"
  "github.com/Trendyol/go-dcp/models"
)

func listener(ctx *models.ListenerContext) {
  switch event := ctx.Event.(type) {
  case models.DcpMutation:
    logger.Log.Info(
      "mutated(vb=%v,eventTime=%v) | id: %v, value: %v | isCreated: %v",
      event.VbID, event.EventTime, string(event.Key), string(event.Value), event.IsCreated(),
    )
  case models.DcpDeletion:
    logger.Log.Info(
      "deleted(vb=%v,eventTime=%v) | id: %v",
      event.VbID, event.EventTime, string(event.Key),
    )
  case models.DcpExpiration:
    logger.Log.Info(
      "expired(vb=%v,eventTime=%v) | id: %v",
      event.VbID, event.EventTime, string(event.Key),
    )
  }

  ctx.Ack()
}

func main() {
  connector, err := dcp.NewDcp("config.yml", listener)
  if err != nil {
    panic(err)
  }

  defer connector.Close()

  connector.Start()
}
```

### Usage

```
$ go get github.com/Trendyol/go-dcp

```

### Configuration

| Variable                                 |       Type        | Required |  Default   | Description                                                                                                                                                                                                                             |
|------------------------------------------|:-----------------:|:--------:|:----------:|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hosts`                                  |     []string      |   yes    |     -      | Couchbase host like `localhost:8091`.                                                                                                                                                                                                   |
| `username`                               |      string       |   yes    |     -      | Couchbase username.                                                                                                                                                                                                                     |
| `password`                               |      string       |   yes    |     -      | Couchbase password.                                                                                                                                                                                                                     |
| `bucketName`                             |      string       |   yes    |     -      | Couchbase DCP bucket.                                                                                                                                                                                                                   |
| `dcp.group.name`                         |      string       |   yes    |            | DCP group name for vbuckets.                                                                                                                                                                                                            |
| `scopeName`                              |      string       |    no    |  _default  | Couchbase scope name.                                                                                                                                                                                                                   |
| `collectionNames`                        |     []string      |    no    |  _default  | Couchbase collection names.                                                                                                                                                                                                             |
| `connectionBufferSize`                   |   uint, string    |    no    |    20mb    | Source Bucket tcp connection buffer size (x Node Count). Check this if you get OOM Killed.                                                                                                                                              |
| `maxQueueSize`                           |        int        |    no    |    2048    | The maximum number of requests that can be queued waiting to be sent to a node. Check this if you get queue overflowed or queue full.                                                                                                   |
| `connectionTimeout`                      |   time.Duration   |    no    |     1m     | Couchbase connection timeout.                                                                                                                                                                                                           |
| `secureConnection`                       |       bool        |    no    |   false    | Enable TLS connection of Couchbase.                                                                                                                                                                                                     |
| `rootCAPath`                             |      string       |    no    |  *not set  | if `secureConnection` set `true` this field is required.                                                                                                                                                                                |
| `debug`                                  |       bool        |    no    |   false    | For debugging purpose.                                                                                                                                                                                                                  |
| `dcp.bufferSize`                         |        int        |    no    |    16mb    | DCP internal queue buffer size (x Node Count). Check this if you get OOM Killed.                                                                                                                                                        |
| `dcp.mode`                               |      string       |    no    |  infinite  | Set DCP mode `finite` If you want to listen to DCP events until now. Set DCP mode `infinite` If you want to listen to DCP events infinitely.                                                                                      |
| `dcp.connectionBufferSize`               |   uint, string    |    no    |    20mb    | DCP tcp connection buffer size (x Node Count). Check this if you get OOM Killed.                                                                                                                                                        |
| `dcp.connectionTimeout`                  |   time.Duration   |    no    |     1m     | DCP connection timeout.                                                                                                                                                                                                                 |
| `dcp.maxQueueSize`                       |        int        |    no    |    2048    | The maximum number of requests that can be queued waiting to be sent to a node. Check this if you get queue overflowed or queue full.                                                                                                   |
| `dcp.listener.skipUntil`                 |     time.Time     |    no    |            | Set this if you want to skip events until certain time.                                                                                                                                                                                 |
| `dcp.group.membership.type`              |      string       |    no    |            | DCP membership types. `couchbase`, `kubernetesHa`, `kubernetesStatefulSet`, `static` or `dynamic`. Check examples for details.                                                                                                          |
| `dcp.group.membership.memberNumber`      |        int        |    no    |     1      | Set this if membership is `static`. Other methods will ignore this field.                                                                                                                                                               |
| `dcp.group.membership.totalMembers`      |        int        |    no    |     1      | Set this if membership is `static` or `kubernetesStatefulSet`. Other methods will ignore this field.                                                                                                                                    |
| `dcp.group.membership.rebalanceDelay`    |   time.Duration   |    no    |    30s     | Works for autonomous mode. If membership is `dynamic`, it is ignored and set to `0s`.                                                                                                                                                   |
| `dcp.group.membership.config`            | map[string]string |    no    |  *not set  | Set key-values of config. `expirySeconds`,`heartbeatInterval`,`heartbeatToleranceDuration`,`monitorInterval`,`timeout` for `couchbase` type                                                                                             |
| `dcp.config.disableChangeStreams`        |       bool        |    no    |   false    | Set this to true if you did not want to get [older versions of changes](https://docs.couchbase.com/server/current/learn/data/change-history.html) for Couchbase Server 7.2.0+ using Magma storage buckets                               |
| `leaderElection.enabled`                 |       bool        |    no    |   false    | Set this true for memberships  `kubernetesHa`.                                                                                                                                                                                          |
| `leaderElection.type`                    |      string       |    no    | kubernetes | Leader Election types. `kubernetes`                                                                                                                                                                                                     |
| `leaderElection.config`                  | map[string]string |    no    |  *not set  | Set key-values of config. `leaseLockName`,`leaseLockNamespace`, `leaseDuration`, `renewDeadline`, `retryPeriod` for `kubernetes` type.                                                                                                  |
| `leaderElection.rpc.port`                |        int        |    no    |    8081    | This field is usable for `kubernetesStatefulSet` membership.                                                                                                                                                                            |
| `checkpoint.type`                        |      string       |    no    |    auto    | Set checkpoint type `auto` or `manual`.                                                                                                                                                                                                 |
| `checkpoint.autoReset`                   |      string       |    no    |  earliest  | Set checkpoint start point to `earliest` or `latest`.                                                                                                                                                                                   |
| `checkpoint.interval`                    |   time.Duration   |    no    |     1m     | Checkpoint checking interval.                                                                                                                                                                                                           |
| `checkpoint.timeout`                     |   time.Duration   |    no    |     1m     | Checkpoint checking timeout.                                                                                                                                                                                                            |
| `healthCheck.disabled`                   |       bool        |    no    |   false    | Disable Couchbase connection health check.                                                                                                                                                                                              |
| `healthCheck.interval`                   |   time.Duration   |    no    |     1m     | Couchbase connection health checking interval duration.                                                                                                                                                                                 |
| `healthCheck.timeout`                    |   time.Duration   |    no    |     1m     | Couchbase connection health checking timeout duration.                                                                                                                                                                                  |
| `rollbackMitigation.disabled`            |       bool        |    no    |   false    | Disable reprocessing for roll-backed Vbucket offsets.                                                                                                                                                                                   |
| `rollbackMitigation.interval`            |   time.Duration   |    no    |     1s     | Persisted sequence numbers polling interval.                                                                                                                                                                                            |
| `rollbackMitigation.configWatchInterval` |   time.Duration   |    no    |    10s     | Cluster config changes listener interval.                                                                                                                                                                                               |
| `metadata.type`                          |      string       |    no    | couchbase  | Metadata storing types.  `file` or `couchbase`.                                                                                                                                                                                         |
| `metadata.readOnly`                      |       bool        |    no    |   false    | Set this for debugging state purposes.                                                                                                                                                                                                  |
| `metadata.config`                        | map[string]string |    no    |  *not set  | Set key-values of config. `hosts`, `username`, `password`, `bucket`,`scope`,`collection`,`maxQueueSize`,`connectionBufferSize` 5mb is default (x Node Count),`connectionTimeout`, `secureConnection`, `rootCAPath` for `couchbase` type |
| `api.disabled`                           |       bool        |    no    |   false    | Disable metric endpoints                                                                                                                                                                                                                |
| `api.port`                               |        int        |    no    |    8080    | Set API port                                                                                                                                                                                                                            |
| `metric.path`                            |      string       |    no    |  /metrics  | Set metric endpoint path.                                                                                                                                                                                                               |
| `logging.level`                          |      string       |    no    |    info    | Set logging level.                                                                                                                                                                                                                      |

### Environment Variables

These environment variables will **overwrite** the corresponding configs.

| Variable                                    | Type |       Corresponding Config        |                         Description                          |
|---------------------------------------------|:----:|:---------------------------------:|:------------------------------------------------------------:|
| `GO_DCP__DCP_GROUP_MEMBERSHIP_MEMBERNUMBER` | int  | dcp.group.membership.memberNumber | To be able to prevent making deployment to scale up or down. |
| `GO_DCP__DCP_GROUP_MEMBERSHIP_TOTALMEMBERS` | int  | dcp.group.membership.totalMembers | To be able to prevent making deployment to scale up or down. |

### Monitoring

The client offers an API that handles different endpoints and expose several metrics.

### API

| Endpoint                | Description                                                                              | Debug Mode | Body                                            |
|-------------------------|------------------------------------------------------------------------------------------|------------|-------------------------------------------------|
| `GET /status`           | Returns a 200 OK status if the client is able to ping the couchbase server successfully. |            |                                                 |
| `GET /rebalance`        | Triggers a rebalance operation for the vBuckets.                                         |            |                                                 |
| `GET /states/offset`    | Returns the current offsets for each vBucket.                                            | x          |                                                 |
| `GET /states/followers` | Returns the list of follower clients if service discovery enabled                        | x          |                                                 |
| `GET /debug/pprof/*`    | [Fiber Pprof](https://docs.gofiber.io/api/middleware/pprof/)                             | x          |                                                 |
| `PUT /membership/info`  | Updates membership info and applies rebalance.                                           |            | ```{"memberNumber": 1,"totalMembers": 3 }```    |  

The Client collects relevant metrics and makes them available at /metrics endpoint.
In case you haven't configured a metric.path, the metrics will be exposed at the /metrics.

### Exposed metrics

| Metric Name                          | Description                                             | Labels                                   | Value Type |
|--------------------------------------|---------------------------------------------------------|------------------------------------------|------------|
| cbgo_mutation_total                  | The total number of mutations on a specific vBucket     | vbId: ID of the vBucket                  | Counter    |
| cbgo_deletion_total                  | The total number of deletions on a specific vBucket     | vbId: ID of the vBucket                  | Counter    |
| cbgo_expiration_total                | The total number of expirations on a specific vBucket   | vbId: ID of the vBucket                  | Counter    |
| cbgo_agent_queue_current             | The current number of agent queue                       | address: Couchbase, is dcp: Is Dcp Agent | Gauge      |
| cbgo_agent_queue_max                 | The max number of agent queue                           | address: Couchbase, is dcp: Is Dcp Agent | Gauge      |
| cbgo_seq_no_current                  | The current sequence number on a specific vBucket       | vbId: ID of the vBucket                  | Gauge      |
| cbgo_start_seq_no_current            | The starting sequence number on a specific vBucket      | vbId: ID of the vBucket                  | Gauge      |
| cbgo_end_seq_no_current              | The ending sequence number on a specific vBucket        | vbId: ID of the vBucket                  | Gauge      |
| cbgo_persist_seq_no_current          | The persist sequence number on a specific vBucket       | vbId: ID of the vBucket                  | Gauge      |
| cbgo_lag_current                     | The current lag on a specific vBucket                   | vbId: ID of the vBucket                  | Gauge      |
| cbgo_total_lag_current               | The current total lag                                   | N/A                                      | Gauge      |
| cbgo_process_latency_ms_current      | The latest process latency in milliseconds              | N/A                                      | Gauge      |
| cbgo_dcp_latency_ms_current          | The latest consumed dcp message latency in milliseconds | N/A                                      | Counter    |
| cbgo_rebalance_current               | The number of total rebalance                           | N/A                                      | Counter    |
| cbgo_active_stream_current           | The number of total active stream                       | N/A                                      | Gauge      |
| cbgo_total_members_current           | The total number of members in the cluster              | N/A                                      | Gauge      |
| cbgo_member_number_current           | The number of the current member                        | N/A                                      | Gauge      |
| cbgo_membership_type_current         | The type of membership of the current member            | Membership type                          | Gauge      |
| cbgo_offset_write_current            | The latest number of the offset write                   | N/A                                      | Gauge      |
| cbgo_offset_write_latency_ms_current | The latest offset write latency in milliseconds         | N/A                                      | Gauge      |

### Compatibility

| Go DCP Version | Minimum Couchbase Server Version |
|----------------|----------------------------------|
| x<1.1.16       | 6.5.x                            |
| 1.1.16>=x      | 5.x.x                            |

## Breaking Changes

| Date taking effect | Version | Change                                                                                 | How to check        |
|--------------------|---------|----------------------------------------------------------------------------------------|---------------------| 
| December 14, 2023  | v1.1.19 | dcp.config.[DisableExpiryOpcode,DisableStreamEndByClient, EnableChangeStreams] removed | Review your configs |

### Examples

- [example with couchbase membership](example/main.go)
- [couchbase membership config](example/config.yml) - thanks to [@onursak](https://github.com/onursak)
- [kubernetesStatefulSet membership config](example/config_k8s_stateful_set.yml)
- [kubernetesHa membership config](example/config_k8s_leader_election.yml)
- [static membership config](example/config_static.yml)
- [dynamic membership config](example/config_dynamic.yml)

## Grafana Metric Dashboard

[Grafana & Prometheus Example](example/grafana)

## Distributed Tracing

[otel-go-dcp](https://github.com/Trendyol/otel-go-dcp)