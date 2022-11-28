# Go Dcp Client

This repository contains go implementation of a Couchbase Database Change Protocol (DCP) client.

### Contents
---

* [Why?](#why)
* [Features](#features)
* [Usage](#usage)
* [Configuration](#configuration)
* [Examples](#examples)

### Why?

+ Our main goal is to build a dcp client for faster and stateful systems. We want to use this repository in below
  implementations:
    + Couchbase Elastic Connector
    + Kafka Connector

---

### Features

- [X] Metrics calculator
- [X] Kubernetes StatefulSet membership
- [ ] Kubernetes Replica change watcher
- [ ] Auto membership
- [ ] Durable connection
- [ ] Auto restart

---

### Usage

```
$ go get github.com/Trendyol/go-dcp-client

```

---

### Configuration

| Variable                         | Type                    | Is Required |
|----------------------------------|-------------------------|-------------|
| `hosts`                          | array                   | yes         |
| `username`                       | string                  | yes         |
| `password`                       | string                  | yes         |
| `bucketName`                     | string                  | yes         |
| `userAgent`                      | string                  | yes         |
| `compression`                    | boolean *(true/false)*  | yes         |
| `metadataBucket`                 | string                  | yes         |
| `connectTimeout`                 | integer *(second)*      | yes         |
| `dcp.connectTimeout`             | integer *(second)*      | yes         |
| `dcp.flowControlBuffer`          | integer                 | yes         |
| `dcp.persistencePollingInterval` | integer *(millisecond)* | yes         |
| `dcp.group.name`                 | string                  | yes         |
| `dcp.membership.type`            | string                  | yes         |
| `dcp.membership.memberNumber`    | integer                 | yes         |
| `dcp.membership.totalMembers`    | integer                 | yes         |
| `api.port`                       | integer                 | yes         |
| `metric.enabled`                 | boolean *(true/false)*  | yes         |
| `metric.path`                    | string                  | yes         |

---

### Examples

- [main.go](example/main.go)
- [config.yml](example/config.yml)
