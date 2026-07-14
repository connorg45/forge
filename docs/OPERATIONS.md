# Operations

The included Compose file is a local development and evaluation environment. It binds published ports to `127.0.0.1`, uses known development credentials, and enables anonymous Grafana administration. Do not expose it directly to another host or use it as a production deployment specification.

## Failure behavior

| Failure | Expected behavior | Recovery bound |
| --- | --- | --- |
| API exits during enqueue | Transaction commits the full job or nothing | Client retry and API restart |
| Worker exits during handler | Lease expires, abandoned run closes, job becomes eligible | Lease duration |
| Scheduler exits | PostgreSQL releases its session lock; another replica takes leadership | Next one-second tick |
| Redis is unavailable | Live events and rate limiting degrade; durable queue remains intact | Redis recovery |
| PostgreSQL restarts | Committed state survives; clients reconnect; leases expire normally | Database restart plus lease |

## Production checklist

- Put the API behind authenticated TLS termination.
- Put the gRPC endpoint and dashboard behind the same authenticated boundary.
- Set `CORS_ALLOWED_ORIGINS` to exact trusted browser origins; leave it empty when cross-origin access is unnecessary.
- Store credentials in a secret manager and rotate them.
- Restrict PostgreSQL and Redis to private networks.
- Back up PostgreSQL and regularly test restores.
- Alert on queue depth, oldest-ready age, dead-letter growth, error rate, and lease expiry.
- Use idempotent handlers for all external side effects.
- Tune pool size, worker concurrency, lease, and timeout against representative workloads.
- Treat caller-supplied tenant IDs as namespaces only; add authenticated authorization before relying on tenant isolation.
