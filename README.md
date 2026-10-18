# storage-simulator

Small FastAPI file store for the Kubernetes Application Observability Lab (Simulation 1).

**Everything about quota and throttling is SIMULATED.** The app-level quota (`QUOTA_BYTES`)
is not the real volume size, and HTTP 503 `ServerBusy` is produced by this app on demand.
These are **not Azure metrics** and must never be presented as such.

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness (process only, no storage) |
| `GET /metrics` | Prometheus metrics |
| `POST /storage/write` | `{"filename": "...", "size_bytes": N}` writes N random bytes (atomic replace) |
| `GET /storage/read/{filename}` | Returns the file |
| `DELETE /storage/delete/{filename}` | Removes a file (cleanup) |
| `GET /storage/status` | Used / capacity / throttling state |
| `POST /simulation/throttling/enable` | Optional `{"rate": 0..1}` (default 1.0) |
| `POST /simulation/throttling/disable` | |

Status codes: 201/200/204 ok, 400 bad name, 404 missing, 413 write too large,
507 simulated quota exceeded, 503 simulated ServerBusy (`Retry-After: 1`), 500 I/O error.

Metrics: `storage_simulator_up`, `storage_simulator_transactions_total{operation,status_code}`,
`storage_simulator_requests_total{method,route,status_code}`, `storage_simulator_throttled_requests_total{operation}`,
`storage_simulator_used_bytes`, `storage_simulator_capacity_bytes`, `storage_simulator_files`,
`storage_simulator_operation_duration_seconds{operation}`, `storage_simulator_throttling_enabled`,
`storage_simulator_throttling_rate`, `storage_simulator_last_check_timestamp_seconds`.

Run as **one replica, one process**: counters and the throttling switch live in memory
(throttling resets to disabled on restart). Used bytes are measured on disk every 15 s.
