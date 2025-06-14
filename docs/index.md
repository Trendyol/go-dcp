# Tutorial: go-dcp

`go-dcp` is a Go library for consuming Couchbase Database Change Protocol (DCP) feeds.
It acts as an *orchestrator* (**Dcp Client**) managing connections (**Couchbase Client**),
stream processing (**Stream**, **Observer**), user event handling (**Consumer**),
saving progress (**Checkpoint**), coordinating multiple instances (**Membership**),
and providing a management interface (**API**), while also handling complexities like rollbacks (**Rollback Mitigation**).


## Visual Overview

```mermaid
flowchart TD
    A0["Dcp Client
"]
    A1["Configuration
"]
    A2["Couchbase Client (gocbcore wrapper)
"]
    A3["Stream
"]
    A4["Observer
"]
    A5["Consumer / Listener
"]
    A6["Checkpoint
"]
    A7["Membership / VBucket Discovery
"]
    A8["API
"]
    A9["Rollback Mitigation
"]
    A0 -- "Reads Config" --> A1
    A0 -- "Uses Client" --> A2
    A0 -- "Manages Stream" --> A3
    A0 -- "Receives Consumer" --> A5
    A0 -- "Uses Membership/Discovery" --> A7
    A0 -- "Starts API" --> A8
    A3 -- "Manages Observers" --> A4
    A3 -- "Uses Checkpointing" --> A6
    A3 -- "Uses Rollback Mitigation" --> A9
    A4 -- "Forwards Events" --> A5
    A2 -- "Sends Raw Events" --> A4
    A6 -- "Uses Client for Sync" --> A2
    A9 -- "Uses Client for Sync" --> A2
    A7 -- "Informs Rebalance" --> A3
    A8 -- "Exposes Stream Info" --> A3
    A8 -- "Exposes Membership Info" --> A7
```

## Chapters

1. [Dcp Client
   ](01_dcp_client_.md)
2. [Configuration
   ](02_configuration_.md)
3. [Consumer / Listener
   ](03_consumer___listener_.md)
4. [Couchbase Client (gocbcore wrapper)
   ](04_couchbase_client__gocbcore_wrapper__.md)
5. [Stream
   ](05_stream_.md)
6. [Checkpoint
   ](06_checkpoint_.md)
7. [Rollback Mitigation
   ](07_rollback_mitigation_.md)
8. [Observer
   ](08_observer_.md)
9. [Membership / VBucket Discovery
   ](09_membership___vbucket_discovery_.md)
10. [API
    ](10_api_.md)
