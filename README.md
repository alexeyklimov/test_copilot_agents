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
- `/app/src/loadTest/java` — load tests for 100 entities × 600 features requests

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
./gradlew loadTest
```

## Runtime

Environment variables:

- `PORT` — HTTP port, default `8080`
- `CASSANDRA_HOST` — Cassandra host, default `127.0.0.1`
- `CASSANDRA_PORT` — Cassandra port, default `9042`
- `CASSANDRA_DATACENTER` — local DC, default `datacenter1`
- `CASSANDRA_SLICE_PAGE_SIZE_BYTES` — max in-memory Arrow result page size per Cassandra slice read, default `1048576`