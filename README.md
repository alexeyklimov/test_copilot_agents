# Feature store read-api PoC

PoC read-api for Cassandra-backed online feature store access with:

- legacy JSON HTTP endpoint
- streaming JSON → Arrow request conversion
- tenant-scoped access control and quota enforcement
- Arrow-based slice read pipe
- streaming Arrow → JSON response conversion
- Simulacron-backed functional and load tests

## Layout

- `/app/src/main/java` — service, HTTP, Arrow, and Cassandra integration
- `/app/src/functionalTest/java` — functional tests against Simulacron
- `/app/src/jmh/java` — JMH latency/throughput benchmarks for read-path stages and full HTTP reads
- `/app/src/loadTest/java` — load tests for 100 entities × 600 features requests
- `/docker` — cpu-limited container entrypoint for per-core benchmark runs
- `/.github/workflows/performance-regression.yml` — per-PR performance regression workflow

## Architecture overview

The PoC is built as a narrow read pipeline that translates legacy JSON requests into a structured Arrow form, executes slice reads in Cassandra, and streams the response back as JSON.

### Main flow

1. `FeatureStoreApplication` wires the catalog, authorization, codec, Cassandra pipe, and HTTP server.
2. `FeatureStoreHttpServer` accepts `POST /read`, validates the tenant header, and delegates the request to the read service.
3. `LegacyReadService` parses the payload, checks tenant permissions and quotas, and orchestrates the read execution.
4. `LegacyJsonArrowCodec` converts:
   - incoming JSON keys into Arrow request tables grouped by key type
   - outgoing Arrow result batches into the legacy JSON response shape
5. `TenantAccessController` enforces:
   - known tenant validation
   - allowed key types per tenant
   - request quotas for keys, feature references, and Arrow payload size
6. `CassandraSliceReadPipe` performs per-entity slice reads in Cassandra, currently builds literal CQL for each entity, and emits Arrow result batches.

### Data model

- `FeatureCatalog` is the central runtime registry for:
  - key types
  - feature definitions
  - tenant policies
- `FeatureCatalogDefaults` provides the built-in demo catalog used by the application entry point.

Each key type maps to a Cassandra slice table and a numeric `key_id`. Features are resolved by name from the legacy API and then translated to numeric IDs for storage access.

### Arrow boundary

Arrow is used as the in-memory request and result representation across parsing, authorization checks, Cassandra reads, and response serialization:

- request tables store request ordinal + entity bytes
- result tables store request ordinal + entity + feature ID + raw value bytes
- `ArrowMessages` owns the shared schemas, builders, request containers, and result streamers

This keeps the internal pipeline columnar and stream-oriented while preserving the legacy JSON contract at the HTTP edge.

## Build

```bash
./gradlew check
```

Useful tasks:

```bash
./gradlew functionalTest
./gradlew jmh
./gradlew loadTest
./gradlew heapSaturationBenchmark
```

Benchmark knobs:

- `-Pbenchmark.profile=ci` — shorter JMH warmup/measurement profile for CI
- `-Pjmh.resultFile=/absolute/path/results.json` — writes JMH JSON results for comparison
- `-Pheap.profile=ci` — smaller heap saturation scenario tuned for CI
- `-Pheap.maxHeap=256m` — overrides the dedicated heap-saturation JVM size

Per-core benchmarks in Docker:

```bash
docker build -f docker/performance.Dockerfile -t feature-store-perf .
docker run --rm --cpus 1 --memory 2g feature-store-perf jmh -Pbenchmark.profile=ci
```

## Runtime

Environment variables:

- `PORT` — HTTP port, default `8080`
- `CASSANDRA_HOST` — Cassandra host, default `127.0.0.1`
- `CASSANDRA_PORT` — Cassandra port, default `9042`
- `CASSANDRA_DATACENTER` — local DC, default `datacenter1`