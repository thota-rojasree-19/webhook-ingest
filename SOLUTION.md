# SOLUTION.md

## 1. Defect Analysis: What Was Broken and Why

### Problem 1: Idempotent Webhook Ingestion & Duplicate Deliveries
- **Root Cause**: `events.event_id` lacked a `UNIQUE` constraint in `migrations/001_init.sql`, and `internal/ingest/service.go` performed a check-then-act (`EventExists` read followed by `InsertEvent`) anti-pattern.
- **Impact**: Concurrent duplicate webhook deliveries evaluated `EventExists` as `false` simultaneously, causing duplicate rows in `events`, redundant updates to `calls`, double-incremented `account_stats`, and duplicate background recording tasks.
- **Fix**: Added a `UNIQUE` index on `events.event_id` and implemented atomic `IngestEventTx` in `internal/store/store.go` using `INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING id` inside a PostgreSQL transaction (`pgx.Tx`). Only the request that successfully inserts the event triggers downstream side effects.

### Problem 2: Recording Processing Lifecycle & Shutdown Recovery
- **Root Cause**: In `internal/ingest/service.go`, background recording tasks inherited the HTTP request context (`r.Context()`), which was cancelled as soon as the response was returned. `s.store.MarkRecordingProcessed` failed with `context.Canceled`, and errors were swallowed silently (`// TODO: handle`). Furthermore, goroutines were unmonitored during SIGTERM/SIGINT service restarts.
- **Impact**: Calls remained permanently marked with `recording_processed = FALSE` with zero error visibility in logs, and in-flight tasks were killed on deployment.
- **Fix**: Decoupled background context using `context.WithoutCancel(ctx)`, added structured error logging (`s.log.Error(...)`), tracked active goroutines with a `sync.WaitGroup` to wait for completion during `Service.Shutdown(ctx)`, and added `RecoverPendingRecordings(ctx)` on startup to pick up unprocessed database records.

### Problem 3: Account Statistics Concurrency & Recovery
- **Root Cause**: `stats.Cache.Record` mutated the map `c.m` and counters without mutex locking (`c.mu.Lock()`), leading to data races and lost updates under concurrency. Additionally, `stats.NewCache()` initialized an empty map, returning zero values on cold starts/restarts without consulting PostgreSQL.
- **Impact**: Account call-counts drifted, produced data races under parallel load, and after restart the in-memory cache was empty and could report zero totals for existing accounts even though durable PostgreSQL statistics remained available.
- **Fix**: Mutex-protected `Cache.Record` and `Cache.Set`, added `LoadAccountStats(ctx)` on service startup to populate cache from PostgreSQL `account_stats`, and implemented on-demand DB fallback on cache misses.

---

## 2. Deduplication Strategy Selection & Trade-Offs

PostgreSQL was selected as the source of truth for deduplication using a `UNIQUE` index on `events.event_id` and atomic `INSERT ... ON CONFLICT (event_id) DO NOTHING`.

### Why PostgreSQL Over Redis for Deduplication:
- **Consistency & Single Source of Truth**: Primary delivery data (`events`, `calls`, `account_stats`) resides in PostgreSQL. Enforcing idempotency at the database level guarantees strict transactional consistency within a single `pgx.Tx` transaction.
- **Elimination of Dual-Write Issues**: Storing deduplication keys in Redis separately from Postgres introduces dual-write race conditions (e.g. key set in Redis but Postgres insert fails, or vice versa).
- **Atomicity**: PostgreSQL row-level locks on the `UNIQUE` index natively serialize concurrent identical `event_id` insertions.
- **Why Redis Was Not Necessary for Correctness**: Redis is useful for fast ephemeral caching or distributed lock primitives, but for single-node ACID guarantees, PostgreSQL's `ON CONFLICT` provides atomic deduplication without extra network overhead or cache invalidation complexity.

---

## 3. Scaling to 10,000 Webhooks/Second

Handling 10,000 webhooks/second would require shifting from in-process background goroutines to a distributed, horizontally scalable architecture:

- **Durable Message Queue / Task Worker Architecture**: Replace in-process goroutines (`go processRecording`) with a durable distributed task queue (e.g., Redis Streams via Asynq/River, Apache Kafka, or RabbitMQ). Ingestion nodes persist webhooks and acknowledge the request fast, while dedicated worker processes handle recording work asynchronously.
- **Database Write Scaling & Batching**: At 10k req/sec, direct synchronous PostgreSQL writes per request will saturate connection pools and disk I/O. Implement write buffering and bulk upserts (`INSERT ... VALUES (...), (...)`), or partition the `events` and `calls` tables by time/account.
- **Distributed Caching & Multi-Node State**: Replace node-local `stats.Cache` with a shared distributed cache (e.g., Redis using `HINCRBY` for atomic counters) or stream aggregation workers to avoid in-memory state fragmentation across multiple API nodes.
- **Connection Pooling & Observability**: Use PgBouncer for PostgreSQL connection pooling and implement Prometheus metrics and distributed tracing (OpenTelemetry) for queue lag and processing latency.
