# Long-running heartbeating Activity

A Temporal .NET stack for one pattern. A long-running Activity heartbeats a progress checkpoint,
and when its Worker dies mid-flight the retry resumes from that checkpoint instead of starting over.

## Start here

```bash
docker compose up -d --build
```

Then open http://localhost:3000 and find the **Heartbeat Demo** folder.

`--build` compiles the solution inside Docker on the first run. Later runs reuse the image.

Dashboards fill in about 90 seconds. The load generator starts 0.5 jobs per second and a default
job runs about 50 seconds, so nothing completes before then.

Open **Heartbeat and Resume** first. If **resumed attempt share** is above zero, the pattern is
working.

Copy `.env` if you want to change anything. Every value has a built-in default, so this is
optional.

```bash
cp .env.example .env
```

## Force a resume

Chaos runs at 2% per item by default, so resumes happen on their own. To trigger one on demand:

1. In one terminal: `docker compose logs -f worker | grep -i resum`
2. In another: `docker compose restart worker`
3. Watch **Heartbeat and Resume**. `resumed="true"` attempts appear and **resume offset** goes
   non-zero.

The Worker gets 10 seconds of graceful shutdown. In-flight Activities see cancellation with reason
`WorkerShutdown` and fail retryably instead of reporting a cancelled Activity. That distinction
matters. A cancelled Activity makes the Workflow give up, and here we want it to retry onto the
replacement Worker.

That path needs `stop_grace_period: 30s` on the `worker` service. Docker's 10 second default equals
the Worker's own graceful shutdown timeout exactly, so SIGKILL lands at the instant the SDK starts
cancelling Activities. The `WorkerShutdown` handling never runs and the drill turns into an abrupt
kill that only the heartbeat timeout catches. Resume still works, which is the whole point of
heartbeats, but you would not be testing graceful shutdown.

Check graceful shutdown in the logs, not on a dashboard. The `attempt_failed{reason="WorkerShutdown"}`
counter increments milliseconds before the process exits, and a 10 second scrape interval almost
never catches it. The logs show the full path: `cancelled at item N, reason WorkerShutdown`, then a
retryable `Worker shut down at item N`.

To make the payoff obvious, set `CHAOS_FAILURE_RATE=0.05` and compare resume offset against zero.
The offset is work you did not have to redo.

## Teardown and reset

Five levels. Only the last one destroys data.

### 1. Stop the load

```bash
docker compose stop loadgen
docker compose start loadgen
```

`loadgen` starts jobs at `LOADGEN_TARGET_RATE_PER_SEC` forever and carries `restart: unless-stopped`,
so this is how you quiet it. Everything else keeps running. Jobs already started run to completion on
the Worker, and the load generator drains its own result tracking on the way out, which is what
`stop_grace_period: 30s` buys it.

Reach for this when you want a settled dashboard to read.

### 2. Stop the stack

```bash
docker compose stop     # containers stop, all three volumes survive
docker compose start    # picks up where it left off
docker compose down     # also removes the containers, still keeps the volumes
```

None of these lose data. Only `down -v` in level 5 does.

### 3. Reset the metrics

```bash
docker compose restart worker loadgen
```

Every `heartbeat_demo_*` and `loadgen_*` counter lives in the process that emits it, so restarting
those two zeroes them. Workflow history and Prometheus history both survive.

Prometheus keeps the samples it already scraped. That is harmless for the `rate()` panels, because it
detects counter resets on its own, but the cumulative-total panels go on showing the old peaks. To
clear those as well:

```bash
curl -X POST -g 'http://localhost:9090/api/v1/admin/tsdb/delete_series?match[]={__name__=~".+"}'
curl -X POST 'http://localhost:9090/api/v1/admin/tsdb/clean_tombstones'
```

Both return `204 No Content`. Dashboards read empty for a scrape interval or two, then refill from
zero. These endpoints exist only because `--web.enable-admin-api` is set on the `prometheus` service
in `docker-compose.yml`. Prometheus disables them by default.

To start Prometheus on an empty database instead:

```bash
docker compose rm -sf prometheus
docker volume rm heartbeat-demo_prometheus-data
docker compose up -d prometheus
```

### 4. Reset dashboard edits

```bash
docker compose rm -sf grafana
docker volume rm heartbeat-demo_grafana-data
docker compose up -d grafana
```

`observability/grafana/provisioning/dashboards/dashboards.yml` sets `allowUiUpdates: true`, so
Grafana lets you save panel edits from the browser and keeps them in the `grafana-data` volume. Those
saved copies then beat the files on disk permanently. Grafana bumps the stored version past the
file's, and the provisioner leaves anything newer alone. Nudging one threshold in the UI is enough to
trigger it, and nothing in the interface tells you the file is now being ignored.

Dropping the volume is the way back. All four dashboards reprovision from files within about 20
seconds and the stored version resets to 1.

### 5. Full wipe

```bash
docker compose down -v
```

The only step here that destroys data. It removes the containers and all three named volumes:
Workflow history in Postgres, metric history in Prometheus, and Grafana's own state.

Dashboards and the datasource are provisioned from files under `observability/grafana/`, so they
survive a full wipe and reappear on the next `up`.

## Ports

| Port | Service |
| --- | --- |
| 3000 | Grafana |
| 7233 | Temporal frontend (gRPC) |
| 8080 | Temporal Web UI |
| 9090 | Prometheus |
| 9091 | Temporal Service metrics (`/metrics`) |
| 9464 | Worker SDK metrics (`/metrics`) |
| 9465 | Load generator SDK metrics (`/metrics`) |

Server metrics sit on host 9091 rather than 9090 because Prometheus takes 9090. Inside the Compose
network everything is addressed by service name.

## Why this needs a whole stack

The pattern is three lines of code. Its failure modes are not, and they only show up under load.
Heartbeat throttling silently causes duplicate work. Resume offsets tell you how much progress you
actually kept. Tuning the heartbeat timeout trades detection speed against server request rate.

So the repo ships the Activity next to a local Temporal Service, Prometheus, Grafana and a load
generator, and puts the server, SDK and load layers on their own dashboards.

## How the pattern works

`src/Shared/ChunkProcessingActivities.cs` holds the entire pattern.

1. **Read the checkpoint.** `ctx.Info.HeartbeatDetails` is empty on attempt 1 and holds the last
   flushed heartbeat on every later attempt. `HeartbeatDetailAtAsync<Checkpoint>(0)` decodes it.
2. **Resume.** Start the loop at `checkpoint.NextIndex` instead of 0.
3. **Heartbeat absolute progress.** `Checkpoint` records where the job is, never a delta. The server
   keeps only the most recent heartbeat value, so a delta would be unrecoverable.
4. **Pass the cancellation token into every await.** The server delivers cancellation on heartbeat,
   so an Activity that never heartbeats cannot be cancelled, and one that ignores the token cannot
   stop.

`src/Shared/ChunkedJobWorkflow.cs` supplies the timeouts that make all of that reachable.
`HeartbeatTimeout` (10s by default) detects a dead Worker. `StartToCloseTimeout` is the outer
backstop and stays strictly larger. The retry policy sets `MaximumAttempts = 0`, so injected chaos
delays a job instead of failing it.

### Throttling means duplicate work

The SDK throttles heartbeats to roughly 80% of the heartbeat timeout. Calling `Heartbeat()` on every
item is free, but only some of those calls reach the server, so the items between the last flushed
heartbeat and the crash get processed twice. No setting drives the overlap to zero. You bound it by
lowering the heartbeat timeout, and you pay for that in server request rate. Per-item work has to be
idempotent.

Two panels on **Heartbeat and Resume** make this concrete. Reprocessing overhead is items processed
over items requested, minus one. Heartbeat throttle ratio is `Heartbeat()` calls per
`RecordActivityTaskHeartbeat` RPC that actually reached the server, and it sits well above 1 by
design.

## Configuration

Configuration is environment variables, read once at startup. Workflow code never reads them.
Workflows get replayed, and configuration read inside a Workflow would make replay depend on the
environment at replay time. Job parameters travel in the Workflow input instead.

| Variable | Default | Meaning |
| --- | --- | --- |
| `TEMPORAL_TARGET` | `local` | `local` or `cloud`; affects defaults and the startup banner only |
| `TEMPORAL_ADDRESS` | `temporal:7233` | Frontend endpoint |
| `TEMPORAL_NAMESPACE` | `default` | Namespace |
| `TEMPORAL_API_KEY` | none | Cloud API key auth |
| `TEMPORAL_TLS_CLIENT_CERT_PATH` / `_KEY_PATH` | none | Cloud mTLS auth |
| `TASK_QUEUE` | `long-activity-heartbeat` | Shared by Worker and load generator |
| `JOB_ITEM_COUNT` | `200` | Items per job |
| `JOB_PER_ITEM_MILLIS` | `250` | Simulated work per item |
| `HEARTBEAT_TIMEOUT_SECONDS` | `10` | Heartbeat timeout; heartbeats flush at ~80% of it |
| `CHAOS_FAILURE_RATE` | `0.02` | Per-item probability the Activity throws; 0 disables |
| `WORKER_MAX_CONCURRENT_ACTIVITIES` | `100` | Worker Activity ceiling |
| `LOADGEN_TARGET_RATE_PER_SEC` | `0.5` | Workflow start rate target |
| `LOADGEN_MAX_IN_FLIGHT` | `50` | Ceiling on simultaneously running Workflows |
| `LOADGEN_DURATION_SECONDS` | `0` | `0` runs until stopped |
| `METRICS_BIND_ADDRESS` | `0.0.0.0:9464` | SDK Prometheus bind; must be an IP literal on its own port |

Defaults are sized for a laptop-class Docker VM with 8 GB and leave deliberate headroom. A job takes
roughly `JOB_ITEM_COUNT * JOB_PER_ITEM_MILLIS`, longer once chaos forces retries, so steady-state
concurrency is about rate times job duration. That works out near 40 at the defaults against a cap
of 50, which keeps rate debt flat. Flat debt is what makes it a useful signal once you push harder.

To find the knee, raise `LOADGEN_TARGET_RATE_PER_SEC` and watch **rate debt** and **task
schedule-to-start latency**. If in-flight pins to `LOADGEN_MAX_IN_FLIGHT`, the load generator is the
limit rather than the system. Raise the cap and `WORKER_MAX_CONCURRENT_ACTIVITIES` together.

## Load generator

`src/LoadGenerator/Program.cs` paces starts with a token bucket at `LOADGEN_TARGET_RATE_PER_SEC`,
capped by a semaphore at `LOADGEN_MAX_IN_FLIGHT`.

When the semaphore or the server holds a start back, the shortfall accumulates in the `rate_debt`
gauge rather than getting quietly re-based. Debt is the clearest saturation signal in the stack.
Flat means the system is keeping up. Climbing means it is not.

## Dashboards

Four, provisioned from `observability/grafana/dashboards/` into the **Heartbeat Demo** folder. Each
answers a different question. Start with the first and drop a layer when it looks wrong.

- **Heartbeat and Resume** (`heartbeat.json`). Is the pattern working, and what is it costing?
  Resumed attempt share, resume offset percentiles and distribution, items rescued per resume,
  `Heartbeat()` calls against `RecordActivityTaskHeartbeat` RPCs, throttle ratio, reprocessing
  overhead, attempt failures by reason, and single-attempt against all-attempts Activity latency.
  This is the dashboard for the restart drill.
- **Temporal Worker SDK** (`worker-sdk.json`). Is the Worker healthy? Task slots used against
  available and slot utilisation per pool, pollers by type, Activity schedule-to-start and execution
  latency, Workflow task execution and replay latency, sticky cache behaviour, and the gRPC client's
  request, failure and long-poll views broken out by operation.
- **Temporal Server** (`temporal-server.json`). Is the cluster healthy? Frontend and persistence
  availability, traffic and errors by service and operation, the Workflow and Activity task
  lifecycle as the server sees it, task queue backlog and matching, persistence latency and the
  Postgres pool, shard and history internals, and Go runtime counters. The **Service** picker at the
  top scopes the per-operation panels to frontend, history or matching.
- **Load Generator** (`load-generator.json`). Is the load landing? Target against achieved start
  rate, rate debt, in-flight against cap, start latency, completions by outcome, end-to-end duration.

Panel layout and PromQL on the server and Worker dashboards follow
[temporalio/dashboards](https://github.com/temporalio/dashboards), specifically
`server/server-general.json` and `sdk/temporal-core-sdks-otel.json`. Two things differ for this
stack. Server 1.29.7 emits `service_error_with_type` and `persistence_error_with_type` rather than
the `service_errors` and `persistence_errors` the upstream dashboard queries, so availability is
computed from those. And `auto-setup` runs all four roles in one container, so the `service_name`
label separates services instead of the scrape target.

Every `temporal_*` expression on the Worker SDK and Heartbeat dashboards is pinned to
`component="worker"`. That label comes from the Prometheus scrape config, and dropping the filter
breaks the numbers. The `auto-setup` container runs its own internal Go SDK Worker for system
Workflows and publishes the same `temporal_*` metric names on its own target, so an unfiltered
`sum()` blends the server's Workers into ours. Measured on a running stack, poller count reads 68
unfiltered against 10 for our Worker alone.

Two exporter settings in `src/Shared/TemporalStack.cs` that the queries depend on:

- `UseSecondsForDuration` is on, so every duration histogram holds float seconds rather than integer
  milliseconds. Server-side histograms are already in seconds, so both layers agree.
- Histogram bucket boundaries are overridden for the long-running metrics. The SDK's defaults stop
  at 10, which suits request latency and nothing else here. A default job runs about 50 seconds and
  resume offsets are counted in items, so without overrides most samples land in `+Inf` and every
  percentile panel reads as a flat line.

Counters carry no `_total` suffix. The SDK has a `HasCounterTotalSuffix` option, but it has no
observable effect in SDK 1.18.0, so it is left unset rather than implying the names differ from what
they are.

## Running against Temporal Cloud

No rebuild and no code change. In `.env`:

```bash
TEMPORAL_TARGET=cloud
TEMPORAL_ADDRESS=your-ns.a1b2c.tmprl.cloud:7233
TEMPORAL_NAMESPACE=your-ns.a1b2c
TEMPORAL_API_KEY=your-api-key
```

For mTLS instead, set `TEMPORAL_TLS_CLIENT_CERT_PATH` and `TEMPORAL_TLS_CLIENT_KEY_PATH`, then mount
the files into the `worker` and `loadgen` containers.

Then run only the pieces you still need locally:

```bash
docker compose up -d --build worker loadgen prometheus grafana
```

The Temporal Server dashboard reads empty, because Cloud exposes its metrics through a separate
Prometheus HTTP API rather than the scrape target this stack points at. The other three work
unchanged.

## Tests

```bash
dotnet test
```

`ActivityHeartbeatTests` drives the Activity through `ActivityEnvironment`, which lets a checkpoint
be seeded straight into `HeartbeatDetails`. That is how resume gets tested without killing a Worker.
`WorkflowTests` runs against a real local server, downloaded and cached on first run, and asserts
the invariant that matters: however many times chaos interrupts a job, reported progress adds up to
exactly the job size.

Both use `StartLocalAsync` rather than the time-skipping environment, on purpose. Heartbeat timeouts
and throttling are wall-clock behaviours, and skipping time past them produces retry patterns that
never occur in production.

## Layout

```
src/Shared/          Workflow, Activity, config, metrics, connection setup
src/Worker/          Worker entry point
src/LoadGenerator/   Load generator entry point
tests/Tests/         Activity and Workflow tests
docker/              Dockerfile and Temporal dynamic config
observability/       Prometheus scrape config, Grafana provisioning and dashboards
```

Next: `docker compose restart worker`, then watch **resume offset** on the heartbeat dashboard.
