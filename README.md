# Switch: RC Task

A Python 3.11+ take-home pipeline: a Snowflake adapter publishes versioned order changes to Redpanda; a separate consumer stores every delivery and materializes the latest order in PostgreSQL; FastAPI exposes the delivery log, entity history and statistics.

**Capture scope:** this implementation reads a controlled, immutable Snowflake journal. Every source write must use the supplied writer and its PostgreSQL lock/fence. Direct DML on an arbitrary Snowflake table is not captured reliably. Multi-tenancy is a design proposal below, not an implemented feature.

## Architecture

```text
Snowflake (external trial)
  SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS JOIN CUSTOMER
                     | prepare: bounded copy, 10k–50k orders
                     v
  ORDERS_SOURCE <---- controlled writer (prepare / mutate)
  ORDERS_SOURCE_CHANGES <--- immutable versions in the same DML transaction
                     |
                     v
  adapter: keyset pages -> Pydantic validation -> ACK per event
       |                                  |
       | source_lock + durable fence      v
       |                         Redpanda: source.orders.v1
       |                         3 partitions, key=entity_key, RF=1
       |                                  |
       v                                  v
  PostgreSQL <------------------------- consumer
    sync_state / sync_runs / sync_batches   |
    source_entity_versions                 | one DB transaction, then Kafka commit
    source_write_fences / source_quarantine |
    event_deliveries / event_rejections <---+
    processed_events / orders_current
                     ^
                     | read-only queries
                  FastAPI -> /events, /entities/{key}, /stats
```

Compose also runs one-shot migrations and topic initialization. The adapter, consumer and API are separate processes: API restarts do not rebalance the consumer. PostgreSQL and Redpanda use named volumes; ordinary `make down` preserves them. Ports bind to localhost: API `8000`, PostgreSQL `5433`, Kafka `19092`. Containers use `postgres:5432` and `redpanda:9092`.

## Prerequisites and startup: five commands

Use Docker Engine/Desktop with Compose v2 supporting `!reset` for the isolated demo, Bash, Make, curl, and a reachable Snowflake account. The sample share and a warehouse must be available. Host Python/uv are optional for the application; Python 3.11+ and uv are needed for host tests. Run from the repository root. Network access is needed to pull images/build dependencies and connect to Snowflake.

Create the trial first and configure `.env` between commands 1 and 2 as described below. These commands assume a fresh source and fresh local volumes:

```sh
cp .env.example .env
make up
bash scripts/prepare_snowflake.sh 20000
curl -fsS http://localhost:8000/stats
curl -fsS 'http://localhost:8000/events?limit=3'
```

Edit `.env` privately after copying it and restrict it to the owner (`chmod 600 .env`). Required values: `POSTGRES_PASSWORD`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`; set a suitable `SNOWFLAKE_ROLE` if the default role is insufficient. Do not copy another person's account details. The empty passwords in `.env.example` are intentional.

`make up` builds and starts PostgreSQL, migrations, Redpanda, topic initialization, adapter, consumer and API. Before prepare creates the journal, the adapter may log `sync_failed`; it retries every `SYNC_INTERVAL_SECONDS` (default 30). Initial sync is asynchronous and sequential, so the first statistics response can be partial. Wait for `counts.unique_events=20000` and a non-null `watermarks[].last_sync_completed_at`, with `adapter_rejected_records=0`, before mutating. Worker health means a live process, not a successful sync. `/health/ready` checks the database and migrations, not sync completion.

`make config` validates Compose without rendering secrets. `make logs` follows JSON worker/API logs. `make down` stops the stack. Existing tables are deliberately not reset by prepare; use the isolated recording scenario for a fresh run without disturbing an existing demo.

For subsequent starts, keep the configured `.env`, run `make up` and check `/stats`; skip `prepare_snowflake.sh`. Snowflake tables survive local container restarts and volume removal. If prepare reports `Prepare requires empty tables`, at least one configured source table already contains rows. Continue with the existing source or use a fresh isolated demo.

## Snowflake trial

1. Sign up using the [Snowflake trial form](https://signup.snowflake.com/), activate the account, and sign into Snowsight. Select a cloud/region and edition. Trial usage is bounded by time and credits; see [trial documentation](https://docs.snowflake.com/en/user-guide/admin-trial-account).
2. Select or create an X-Small warehouse, for example `COMPUTE_WH`, with auto-resume and a short auto-suspend interval. Verify access to `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS` and `CUSTOMER` in a worksheet. This pipeline copies sample rows into writable tables because the shared sample data cannot be changed by our demo.
3. Use a role with warehouse USAGE, SELECT on the sample data, permission to create the configured database/schema/tables during prepare, and SELECT/INSERT/UPDATE on the demo tables. An administrative role simplifies a disposable trial; the code does not require the role name `ACCOUNTADMIN`. A production deployment needs separate provisioning, reader and writer roles.
4. Keep credentials out of screenshots and the repository. Trial creation and login are preparation steps, not part of the five local startup commands.

## Credentials and authentication compatibility

Set the following in the local ignored `.env`:

| Variable | Meaning |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Connector account identifier, preferably `organization-account`; no `https://`, Snowsight path or `.snowflakecomputing.com` suffix. Use the connection details for your account; [identifier formats](https://docs.snowflake.com/en/user-guide/admin-account-identifier). |
| `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` | Your connector login and secret; these are not supplied with the submission. |
| `SNOWFLAKE_WAREHOUSE` | Existing warehouse used for queries. |
| `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_TABLE` | Defaults: `SWITCH_DEMO`, `PUBLIC`, `ORDERS_SOURCE`. Only plain SQL identifiers are supported. The reader uses `<table>_CHANGES`. |
| `SNOWFLAKE_ROLE` | Optional role; otherwise Snowflake uses the user's default. |
| `SOURCE_NAME` | Stable logical source identity, default `snowflake.orders_source`; do not rename it over an already synced journal. |

`connect_source()` currently passes **account/user/password**, warehouse, optional role and database/schema; it does not expose MFA, key-pair, OAuth or browser SSO configuration. A successful Snowsight login does not prove this connector can authenticate. Snowflake's [strong authentication rollout](https://docs.snowflake.com/en/user-guide/security-mfa-rollout) can make this password-only configuration incompatible with a new trial. If your policy requires another method, this version needs an authentication extension before its end-to-end demo can run. Do not weaken an account policy to work around that limitation. [Python connector authentication options](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect) describe the supported alternatives; the recorded trial verification is evidence for that account at that time, not a promise for every new account.

`.env` is ignored by Git and Docker context rules; the Dockerfile copies only selected application/dependency files. Pydantic uses `SecretStr` for passwords and hides validation input in configuration errors. Application logs avoid connector exception text; local container environment inspection can still reveal runtime credentials. Do not record `docker inspect`, expanded `docker compose config`, `env`, or `.env`.

## Initialize and demonstrate changes

`prepare_snowflake.sh 20000` invokes `app.adapter.demo prepare`. It creates the configured writable current table and `_CHANGES` journal, then copies a bounded, ordered ORDERS/CUSTOMER join into both in one data transaction. Every seed row has version 1 and type `insert`; all seed rows share one UTC timestamp. The accepted row range is 10000–50000. DDL precedes the transaction because Snowflake DDL commits independently.

After initial sync completes:

```sh
bash scripts/mutate_snowflake.sh
curl -fsS 'http://localhost:8000/events?event_type=update&limit=10'
curl -fsS http://localhost:8000/entities/order:1
curl -fsS http://localhost:8000/stats
```

One mutation inserts one new order, updates the three lowest order keys by `+1.00`, then updates the first again by `+2.00`. It produces **1 insert + 4 update events**, even if the adapter has not read between updates. For an untouched 20000-row demo, convergence means 20005 unique events, 20001 current entities and versions 1/2/3 in `order:1` history. Later mutations continue versions and add another entity. Delivery counts may exceed unique counts after retries; business versions remain deduplicated.

A second CLI prepare against nonempty tables refuses before any data write, reports both row counts and clears its own write fence after the Snowflake connection closes successfully. It never clears an earlier writer's fence. Do not rerun it as a reset. Older images left a fence even on this refusal; rebuilding does not clear that existing fence. For an existing fence or an interrupted writer, stop adapter/writers, determine whether the Snowflake transaction committed, verify current/journal consistency and unique cursor/version pairs, then clear the fence only after reconciliation.

## Event contract and partitioning

The enforced contract is [EventEnvelope / OrderPayload](app/domain/envelope.py), a frozen Pydantic model with unknown fields forbidden. JSON fields:

| Field | Contract |
|---|---|
| `event_id` | SHA-256 of the compact JSON array `[source, entity_key, source_version]`. Stable across batch retries, restarts and recapture. |
| `event_type`, `schema_version` | `insert` or `update`; schema version 1. |
| `source`, `entity_key` | Logical source and `order:<positive numeric order_key>`, validated against payload. |
| `payload` | Order/customer keys, customer name, status O/F/P, decimal price, order date. |
| `source_version` | Strict positive bigint; monotonically increasing per entity. |
| `occurred_at`, `source_updated_at` | Equal, timezone-aware source version timestamp; normalized to UTC with microsecond precision. This is a technical version time, not guaranteed commit time. |
| `captured_at` | Adapter capture time; may change on recapture. |
| `batch_id`, `sync_run_id`, `batch_sequence` | UUID correlation IDs and positive batch sequence. |

Money is a two-decimal string in JSON and `numeric(18,2)` in PostgreSQL; binary floating-point money is rejected. Kafka key bytes are `entity_key.encode()`: every version of an order goes to the same partition, preserving its publication order. The single source demo has three partitions and replication factor 1. Different entities have no global ordering; numeric `source_version` protects materialization independently of arrival order. Changing partition count can change key routing, so it requires a deliberate ordering/migration strategy.

## Cursor, checkpoint and capture boundaries

[run_sync](app/adapter/sync.py) reads the immutable journal in ascending lexicographic order of `C=(source_updated_at, order_key)`. Each run freezes the visible high watermark `H`; pages satisfy `C < row <= H`, ordered by timestamp then **numeric** key. The tie-breaker prevents loss at batch boundaries when many rows share a timestamp. The reader checks duplicate cursor multiplicity using a window function **before LIMIT**; a collision fails instead of checkpointing past a hidden row. A journal regression or failure to reach H also fails closed.

The adapter and every controlled writer hold a shared session advisory lock keyed by `SOURCE_NAME` for the whole operation. Before source work the writer persists `source_write_fences.blocked=true`; it clears the flag only after a confirmed Snowflake commit and connection close. A crash or ambiguous outcome retains the fence. Mutation timestamps are at least `max(now, journal_max + 1 microsecond)`; repeated versions of one entity get separate timestamps. This prevents cooperating writes from appearing behind an advanced cursor, and preserves intermediate versions.

[PostgresCheckpoint](app/adapter/checkpoint.py) stores `sync_state`, run/batch metadata and last entity versions in PostgreSQL. After every valid record has a broker ACK, one real DB transaction stores source quarantine, version bookkeeping, batch completion and the new cursor. It refuses a nested transaction/savepoint. Checkpoint C means every journal entry through C was ACKed or durably quarantined. A restart resumes the last completed batch, marks abandoned attempts failed and uses a new H. A completed no-change run returns 0 without publishing or creating a new run.

PostgreSQL was chosen for durable transactional coordination and inspection alongside the sink; its named volume survives worker/container restarts. It also couples adapter availability to the sink DB. Volume deletion is a state-loss event, not a routine restart; stop writers and restore/rebuild consistently. Direct DML, late/backdated writes, edits to old journal entries, resetting versions/source identity, journal truncation and hard deletes fall outside the guarantee. Long initial sync holds the source lock and blocks the demo writer. This is polling a controlled journal, not arbitrary-table CDC.

## Delivery guarantee and failure windows

The guarantee is **at-least-once delivery with idempotent current-state materialization**, conditional on retained source/broker/DB data. Kafka producer idempotence alone does not deduplicate a restarted adapter's application-level publications.

| Failure window | Result and recovery |
|---|---|
| Broker unavailable before/during a batch | Sequential publish waits for ACK; bounded delivery attempts use exponential full-jitter backoff. Failure retains the batch checkpoint; next sync retries from there. Earlier ACKed records may be republished. |
| Broker writes but ACK is uncertain | The adapter fails without advancing C; retry may deliver the same logical event again. Missing callbacks do not enqueue overlapping retries. |
| ACK succeeds, then adapter/DB fails before checkpoint commit | The whole uncheckpointed batch is retried with the same logical IDs, possibly new capture/correlation metadata. |
| Consumer fails before DB commit | Audit, dedup, rejection and upsert roll back together; the offset is not acknowledged. |
| DB commit succeeds, then Kafka offset commit fails/crash/rebalance occurs | Delivery may repeat. It creates another audit row; the same ID/hash becomes `duplicate` and does not reapply state. |
| Writer fails or Snowflake commit is uncertain | Durable fence blocks further cooperating sync/writes until manual reconciliation. |

Producer defaults: ACK timeout 10s, 4 application attempts, jitter base 0.5s/cap 5s, shutdown drain 5s. Exhaustion is visible in logs; adapter retries on its next interval. The consumer disables automatic offset commit/store and commits synchronously **after** `materialize()` returns from its transaction. A consumer error exits; Compose restarts it. There is no distributed transaction spanning Snowflake, Kafka and PostgreSQL, and RF=1 provides no broker HA.

## Delivery log, logical events and current state

- `event_deliveries` is append-only and records **every committed delivery**, including repeats of the same offset, malformed bytes and stale versions. `delivery_id` identifies a receipt; `event_id` identifies a logical change. Raw bytes, key, topic, partition, offset, timestamps and disposition remain auditable.
- `processed_events` records unique valid logical changes (accepted and stale) and their content hash. The hash excludes `captured_at`, batch/run IDs and sequence. Same ID/hash gives `duplicate`; changed content under the same ID gives quarantined `source_version_conflict`.
- `orders_current` has primary key `(source, entity_key)`. A transaction advisory lock serializes entity decisions; upsert updates only when incoming `source_version` is **strictly greater**. Version sequence 1,3,2 ends at 3; version 2 remains in the log/dedup registry as `stale`.

Audit, deduplication, rejection and current-state writes share one DB transaction. Append-only triggers protect audit tables during normal operation; an administrative owner can disable them for maintenance. Replay keeps the business projection and unique-event count unchanged while audit rows/duplicates increase. With an empty sink and retained full topic, replay reconstructs the maximum version per entity; processing/capture metadata may differ.

## Quality checks and quarantine

| Check | Handling |
|---|---|
| Required payload values, positive keys, valid status/date, nonnegative finite decimal amount | Adapter validation -> `source_quarantine`; consumer validation -> `business_quality_violation`. |
| JSON/envelope schema, version, explicit timezone and stable identity | Consumer -> `invalid_json` or `invalid_envelope`. |
| Wire partition key equals entity key | Consumer -> `partition_key_mismatch`. |
| Same event ID with changed content | Consumer -> `source_version_conflict`; expected duplicates are logged as duplicate, not rejected. |
| Kafka tombstone | Consumer -> `unsupported_tombstone`; deletes are not implemented. |
| Duplicate/invalid cursor or missing structural source columns | Stop sync and retain checkpoint; do not skip past unsafe cursor data. |

`event_rejections` stores reason and sanitized validation locations/types, linked to the delivery with full raw bytes. Once rejection/audit commit, the offset may advance so a poison message does not block the partition. Adapter quarantine commits atomically with C; those records never enter Kafka. `/stats.adapter_rejected_records` exposes their count. There are no intentional silent drops. Repair requires a new source version after C; automatic quarantine reprocessing and a separate DLQ topic are omitted. Raw payload access would need authorization/redaction in production.

## API examples

Open [Swagger UI](http://localhost:8000/docs) or `/openapi.json` for response schemas.

```sh
curl -fsS http://localhost:8000/health/ready
curl -fsS -G http://localhost:8000/events \
  --data-urlencode 'entity_key=order:1' --data-urlencode 'event_type=update' \
  --data-urlencode 'since=2026-10-01T00:00:00Z' \
  --data-urlencode 'until=2026-10-03T00:00:00Z' --data-urlencode 'limit=2'
curl -fsS 'http://localhost:8000/events?limit=2&after_id=2'
curl -fsS http://localhost:8000/entities/order:1
curl -fsS http://localhost:8000/stats
```

Use dates enclosing your run. For subsequent pages, replace `after_id` with the response's `next_after_id` and preserve the same filters; the unfiltered example above is independent. `/events` returns delivery audit ordered by `delivery_id ASC`, default limit 100 (1–500). Source/entity/type filters combine with AND; `since/until` are inclusive **occurred_at** bounds and require timezones. The pagination cursor is distinct from the adapter watermark. It reads a live log, not a cross-request snapshot: a late commit of a lower ID can require re-reading from the start. It is an inspection API, not a reliable export protocol.

`/entities/{key}` defaults to `SOURCE_NAME` and returns current state plus paginated delivery history in one DB snapshot; unknown entities return 404. `/stats` uses one snapshot across all sources: counts by type/disposition, adapter rejections, per-source durable watermarks and first-delivery occurred-at → processed-at delay. Replay is excluded from delay samples. Clock skew can make delay negative; this is **not Kafka offset lag**. The local API has no authentication.

## Tests and verification evidence

```sh
make install             # uv sync --frozen --extra dev
make test                # unit tests; no Snowflake required
make verify-tests        # complete pytest regression with isolated PG + Redpanda
make verify              # real Snowflake + isolated pipeline + HTTP + replay
make verify-broker       # isolated transport failure scenarios
```

`uv.lock` and `requirements.lock` pin local/Docker dependencies; refresh both with `make lock` after dependency changes. Integration tests require a separate migrated DB configured with `TEST_POSTGRES_*`; the live broker test also requires `TEST_KAFKA_BOOTSTRAP_SERVERS` and explicit `TEST_KAFKA_TOPIC`. `make verify-tests` provisions these itself, uses localhost ports 55434/29093 (overridable), then removes its temporary volumes. Do not point integration tests at the demo sink.

Tests cover timestamp ties/page boundaries, repeated updates, frozen H, partial-batch restart, stable IDs, schema/money validation, atomic checkpoint/quarantine, real PostgreSQL upsert/rollback, out-of-order versions, replay, offset-commit failure, API filters/pagination/statistics and consumer exclusion. Unit readers/publishers are not represented as real Snowflake/broker integration.

A historical verification run on 2026-10-02 included 97 tests (58 unit, 39 integration), all passed, and a real Snowflake pipeline with replay. The full topic replay read 10014 deliveries from offset 0 in all three partitions; 10002 entities and 10010 unique events were unchanged, and the full business SHA-256 matched before/after. Audit deliveries grew from 10014 to 20028. This historical scenario includes two mutations and quality failures; its counts differ from the single-mutation demo above. A separate rehearsal on a fresh 10000-row Snowflake source confirmed 10001 unchanged entities, 10005 unchanged logical events and deliveries growing from 10005 to 20010 after replay. These historical results are not a new benchmark. Starlette currently emits one TestClient/httpx deprecation warning.

`make verify` creates unique source/table/topic/group/project names, preserves source tables and volumes for diagnosis, and shuts down its isolated stack on exit. It does not reset the main demo. Untested boundaries include a real interrupted Snowflake COMMIT, DB volume loss, expired topic retention, extended network partitions and multi-broker failure.

## Replay procedure

Replay uses the existing `scripts/verify_pipeline.py replay` implementation with a mounted evidence directory.

1. Stop the adapter, controlled writers and normal consumer; keep broker, DB and API running. Use `CONSUMER_EXCLUSIVE=true` in the recording stack so a shared PostgreSQL topic lock excludes a second cooperating consumer even with another group ID.
2. Snapshot business state using `app.db.state.state_fingerprint` and record unique-event counts/checkpoint. The replay helper does this automatically.
3. Create a unique replay group, require no committed offsets and low watermark 0, capture end offsets, explicitly assign offset 0 in **every** partition, and consume to those ends through the same `process_message`/materializer path. Commit offsets only after DB transactions. Fail if offset 0 has expired or a concurrent write invalidates the bounded run.
4. Compare all current entities, versions, payloads, exact amounts, source timestamps and last event IDs via SHA-256; compare unique-event count and checkpoint. Delivery IDs, capture/processing times and audit counts are intentionally excluded. A matching row count alone is insufficient.
5. Resume normal workers after PASS. The original consumer group's offsets are unchanged; it can redeliver an unacknowledged receipt safely. `auto.offset.reset=earliest` alone does not rewind an existing group's offsets.

For an empty-sink rebuild, `make db-clean-check` previews the scope; `make db-clean-execute` stops workers and clears only sink tables. This is explicitly destructive maintenance, **not needed for the recording**. Replay with a fresh group before resuming the old group; merely starting the old group does not restore deleted sink data. `SCOPE=pipeline` also deletes adapter checkpoints and causes republishing; neither scope changes Snowflake or Kafka offsets.

## Timebox, scope cuts and next improvements

The author's reported effort is **approximately 8 hours**, including implementation, tests and documentation. This is a self-reported estimate; Git dates do not measure active work. The assignment suggests 6–9 hours. AI assistance was used for review/documentation; the author must be ready to explain and modify the implementation live.

Scope cuts: no multi-tenancy implementation, arbitrary-table CDC, hard/soft delete, schema drift evolution, schema registry, transactional outbox, cross-system exactly-once delivery, automated fence recovery, API authentication, HA deployment, distributed worker leases, credential rotation or metrics dashboards. Consumer deduplication is implemented; no separate stretch-goal implementation is claimed. The fixed order schema and controlled writer favor demonstrable correctness within a bounded demo.

With more time: add policy-compatible key-pair/OAuth authentication and least-privilege roles first; move source capture to a Snowflake Stream plus durable outbox with an explicit retention/recovery policy; add bounded asynchronous publishing with batch ACK tracking, consumer connection pooling/batched transactions, proper Kafka lag metrics and alerts for fences/retries/quarantine; implement deletes and schema evolution with migrations/replay tests; add longer failure tests, backups and an access/retention model for raw audit data.

## Extension to multi-tenancy (design only)

At 100 tenants × 10 sources, deploy a bounded worker pool rather than 1000 continuously active Compose adapters. A scheduler with leases/fencing and tenant concurrency budgets must control source ownership, retries and snapshot work. Today's session lock holds through a whole run and can block writers for a long initial sync; it is not a production distributed lease.

| Isolation boundary | Proposed change |
|---|---|
| State | Add immutable `tenant_id` and `source_id` to cursor/run/batch/quarantine/version/fence keys and lock identities. Never share a cursor by human-readable source name. Scope every DB query and API authorization; use RLS or separate databases for stronger isolation. |
| Entity/event keys | Use `(tenant_id, source_id, entity_key)` as state identity and Kafka partition key; include tenant/source in stable event ID and hash identity. `order:1` across tenants or sources must not collide. Numeric version ordering remains local to each entity. |
| Topics/groups | Prefer a tenant/source topic when strict ACL/retention/replay isolation is required; 1000 sources × 3 partitions already means 3000 partitions before replication. Consider a shared topic per source family with namespaced keys for smaller workloads, accepting shared retention/operations. Group IDs are projection/replay identities, not tenant security boundaries. |
| Credentials | Separate secret references and least-privilege source/sink roles per tenant/source, stored in a secret manager. Resolve at task execution, isolate connector sessions/caches, rotate without putting secrets in events, logs or job metadata. |
| Resources | Per-tenant/source rate limits, bounded queues, retry budgets and warehouse/query caps; separate connection pools and fair scheduling. Isolate large snapshots from incremental jobs; dedicate topics/workers/warehouses for heavy tenants as needed. |
| Observability/replay | Tenant-scoped counters, Kafka lag, checkpoint age, fence/quarantine alerts and replay locks/evidence. Authorize one tenant's replay without exposing another tenant's raw deliveries. |

Current bottlenecks are per-record ACK publication, repeated journal window scans, per-message DB connection/transaction/offset commit and full-history `/stats` scans. Measure workload and lag before choosing pool sizes or adding partitions; preserve per-entity order and atomic DB decisions while batching.
