# Chapter 10: API

Welcome to the final chapter of our `go-dcp` tutorial! We've journeyed from the central [Dcp Client](01_dcp_client_.md), through [Configuration](02_configuration_.md), event processing with the [Consumer / Listener](03_consumer___listener_.md), low-level communication via the [Couchbase Client (gocbcore wrapper)](04_couchbase_client__gocbcore_wrapper__.md), event flow management with the [Stream](05_stream_.md), reliable resuming using [Checkpointing](06_checkpoint_.md), handling cluster history changes with [Rollback Mitigation](07_rollback_mitigation_.md), internal event processing via the [Observer](08_observer_.md), and finally how multiple instances coordinate using [Membership / VBucket Discovery](09_membership___vbucket_discovery_.md).

Now that you have a `go-dcp` client instance happily running and processing changes, you might wonder: **how can I check on it?** Is it connected? Is it keeping up with changes? What vBuckets is it processing? Can I tell it to do something, like rebalance the workload?

This is where the **API** component comes in.

Think of the API as a **remote control** or a **status dashboard** for your running `go-dcp` client. It exposes specific HTTP endpoints that you can access from your web browser or a tool like `curl` to monitor the client's health, inspect its internal state (especially its progress), and even trigger certain actions.

Its main purpose is to provide a convenient interface for **monitoring and controlling** your `go-dcp` instance after it has started.

## Enabling and Configuring the API

The API component is optional and disabled by default. To enable it, you need to add configuration in your `config.yml` (refer back to [Chapter 2: Configuration](02_configuration_.md)).

The relevant settings are under the `api` section:

```yaml
# config.yml snippet to enable API
# ... other config ...

api:
  disabled: false # Set to true to disable (default)
  port: 8080      # The HTTP port the API will listen on

# You can also configure the path for metrics specifically
metric:
  path: /metrics # The path for Prometheus metrics (default is /metrics)

# ... rest of config ...
```

By setting `api.disabled` to `false` and specifying a `api.port`, your `go-dcp` client will start an HTTP server on that port and expose the built-in endpoints. The `metric.path` allows you to change the endpoint where Prometheus metrics are exposed if the default `/metrics` conflicts with something else.

## Key API Endpoints

Once enabled, the API exposes several useful endpoints. Let's look at the main ones described in the README:

1.  **`GET /status`**
    *   **What it does:** Checks if the `go-dcp` client can successfully communicate with the Couchbase cluster.
    *   **Use case:** Health checks! You can point monitoring systems or Kubernetes readiness/liveness probes at this endpoint.
    *   **Output:** Returns a simple "OK" with a 200 status code if the connection is healthy, or an error message and status code if there's a problem pinging Couchbase.

2.  **`GET /rebalance`**
    *   **What it does:** Triggers a rebalance operation.
    *   **Use case:** Manually force the client to recalculate its vBucket assignments based on the current [Membership / VBucket Discovery](09_membership___vbucket_discovery_.md) state and redistribute the workload. This is primarily useful with dynamic membership types where you might want to manually initiate a rebalance after adding or removing instances, though dynamic types often handle this automatically.
    *   **Output:** Returns "OK" if the rebalance process is started, or a message indicating the stream is not open.

3.  **`GET /states/offset`** (Requires `debug: true` in config)
    *   **What it does:** Returns the current, in-memory offsets (sequence numbers) that the client has processed for each vBucket it's assigned. This is based on your `ctx.Ack()` calls ([Chapter 6: Checkpoint](06_checkpoint_.md)).
    *   **Use case:** Debugging and monitoring. See exactly how far along each vBucket stream the client is. Note that these are *in-memory* offsets and might be slightly ahead of what's been saved to persistent [Checkpoint](06_checkpoint_.md) storage if you're using `auto` checkpointing.
    *   **Output:** A JSON object mapping vBucket IDs to their current offset details (sequence number, snapshot info, etc.).

    ```json
    # Example Output (simplified) for GET /states/offset
    {
      "512": {
        "vbId": 512,
        "seqNo": 12345,
        "vbUUID": "...",
        "startSeqNo": 12000,
        "endSeqNo": 13000,
        "snapshotType": 1
      },
      "513": {
        "vbId": 513,
        "seqNo": 67890,
        "vbUUID": "...",
        "startSeqNo": 67000,
        "endSeqNo": 68000,
        "snapshotType": 1
      }
      // ... other vBuckets assigned to this instance
    }
    ```

4.  **`GET /states/followers`** (Requires `debug: true` in config and Service Discovery enabled)
    *   **What it does:** If you are using a membership type that involves service discovery (like `couchbase` or `kubernetesHa`), this endpoint lists the other active members in the group that this instance knows about.
    *   **Use case:** Debugging and monitoring distributed deployments. Verify that all your `go-dcp` instances are properly discovered by each other.
    *   **Output:** A JSON object listing the discovered followers.

5.  **`PUT /membership/info`**
    *   **What it does:** Allows you to manually update the client's understanding of its `MemberNumber` and `TotalMembers`.
    *   **Use case:** Primarily used with the `dynamic` [Membership / VBucket Discovery](09_membership___vbucket_discovery_.md) type where an external orchestrator is responsible for assigning member roles. You would send a PUT request to this endpoint with the new membership information, which triggers a rebalance if the assignments change.
    *   **Input Body:** A JSON object like `{"memberNumber": 1, "totalMembers": 3}`.
    *   **Output:** Returns "OK" if the update is processed, or an error if the input is invalid.

6.  **`GET /metrics`** (Path configurable via `metric.path`)
    *   **What it does:** Exposes internal metrics in a format consumable by Prometheus.
    *   **Use case:** Integrate with monitoring systems. Scrape this endpoint using Prometheus to collect detailed performance and state metrics about your `go-dcp` client.
    *   **Output:** Text output in Prometheus exposition format.

    ```text
    # Example Output (simplified) for GET /metrics
    # HELP cbgo_mutation_total The total number of mutations on a specific vBucket
    # TYPE cbgo_mutation_total counter
    cbgo_mutation_total{vbId="512"} 1000
    cbgo_mutation_total{vbId="513"} 1500
    # HELP cbgo_lag_current The current lag on a specific vBucket
    # TYPE cbgo_lag_current gauge
    cbgo_lag_current{vbId="512"} 50
    cbgo_lag_current{vbId="513"} 120
    # HELP cbgo_total_members_current The total number of members in the cluster
    # TYPE cbgo_total_members_current gauge
    cbgo_total_members_current 3
    # ... many other metrics ...
    ```
    The README lists the extensive set of metrics available. These provide deep insight into event counts, processing lag, queue sizes, checkpoint write latency, membership status, and more.

## How the API Works Internally

The API component is implemented using the [Fiber](https://gofiber.io/) web framework, which is a fast and lightweight Go web server.

When the [Dcp Client](01_dcp_client_.md) starts (`connector.Start()`) and the API is enabled in the [Configuration](02_configuration_.md), the `Dcp Client` calls `api.NewAPI()`. This function creates the Fiber application instance, registers the configured endpoints (`/status`, `/rebalance`, etc.), wires them up to methods within the `api` struct, and includes middleware for metrics and debugging (`pprof`) if enabled.

The `api` struct holds references to other key `go-dcp` components it needs to interact with:

*   `client`: The [Couchbase Client](04_couchbase_client__gocbcore_wrapper__.md) to call `Ping()`.
*   `stream`: The [Stream](05_stream_.md) manager to call `GetOffsets()` and `Rebalance()`.
*   `serviceDiscovery`: The Service Discovery interface used by dynamic membership to get follower info.
*   `bus`: The internal EventBus (discussed briefly in [Chapter 9: Membership / VBucket Discovery](09_membership___vbucket_discovery_.md)) used to publish events like membership changes (e.g., from the `/membership/info` endpoint).
*   `config`: The loaded [Configuration](02_configuration_.md) to know the port and enabled features.
*   `registerer`: Used for registering Prometheus metrics collectors.

After setup, `NewAPI()` returns an `API` interface instance. The `Dcp Client` then calls `api.Listen()` on this instance, which starts the Fiber HTTP server. This server runs in a separate goroutine, allowing the main `go-dcp` processing loop (driven by `connector.Start()`) to continue uninterrupted while the API waits for incoming requests.

When an HTTP request arrives at an API endpoint, the Fiber framework routes it to the corresponding method in the `api` struct (e.g., a `GET /status` request goes to the `api.status()` method). This method then performs the required action by calling methods on the relevant `go-dcp` components and formats the HTTP response.

Here's a simplified sequence diagram for an API request, like checking status:

```mermaid
sequenceDiagram
    Participant User as User/Monitoring Tool
    Participant API as API Component (Fiber)
    Participant DcpClient as Dcp Client
    Participant CBCli as Couchbase Client (gocbcore)
    Participant CBSrv as Couchbase Server

    User->API: GET /status
    API->API: Call api.status() method
    api.status()->DcpClient: Access internal client instance
    DcpClient->CBCli: Call Ping()
    CBCli->CBSrv: Send Ping request
    CBSrv-->CBCli: Ping response
    CBCli-->DcpClient: Ping result
    DcpClient-->api.status(): Ping result
    api.status()-->API: Format response ("OK" or error)
    API-->User: Send HTTP Response (200 OK or error)
```

This shows how the API acts as a gateway, receiving external requests and translating them into calls to the internal `go-dcp` components.

For metrics (`/metrics`), the setup is slightly different. The `newMetricMiddleware` function registers Prometheus collectors (which are linked to the `go-dcp` components that expose metrics, like [Observer](08_observer_.md) for event counts or [Stream](05_stream_.md) for lag). When Prometheus (or a user) hits the `/metrics` endpoint, the metric middleware gathers the current values from these registered collectors and formats them for the HTTP response.

## Conclusion

The API component provides valuable visibility and control over your running `go-dcp` client via standard HTTP endpoints. By enabling and configuring it in your `config.yml`, you gain access to health checks (`/status`), debugging information about offsets and membership (`/states/offset`, `/states/followers`), the ability to manually trigger a rebalance (`/rebalance`), and crucially, a rich set of performance and state metrics exposed via the `/metrics` endpoint for integration with monitoring systems like Prometheus and Grafana.

Understanding the role of the API empowers you to effectively operate and observe your `go-dcp` applications in production.

This concludes our introductory tutorial to the core concepts of the `go-dcp` library. We've explored the major components and how they work together to reliably stream change events from Couchbase. From here, you can delve deeper into the specific configurations, explore advanced topics like distributed tracing, and build powerful data processing applications using `go-dcp`.
