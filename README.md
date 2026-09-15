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