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

+ Our main goal is to build a dcp client for faster and stateful systems. We want to use this repository in below implementations:
  + Couchbase Elastic Connector
  + Kafka Connector

---


### Features
- [X] Metrics calculator
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

| Variable                           | Type                                | Is Required                          |
|------------------------------------|-------------------------------------|--------------------------------------|
| `hosts`                     | comma seperated values in a string  | yes  |
| `username`                  | string                              | yes  |
| `password`                  | string                              | yes  |
| `bucketName`                | string                              | yes  |
| `metadataBucket`            | string                              | yes  |
| `compression`               | boolean *(true/false)*              | no   |
| `connectTimeout`            | integer *(second)*                  | no   |
| `api.port`                  | string                              | yes  |
| `metric.path`               | string                              | no   |
 

---
### Examples

- [main.go](example/main.go)
- [config.yml](example/config.yml)


