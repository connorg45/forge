# Benchmarks

## Workloads

The k6 workload defaults to 32 virtual users for 20 seconds against `POST /v1/jobs`. The chaos workload defaults to 10,000 idempotent jobs and kills workers every five seconds for 60 seconds while checking queue state and an idempotent side-effect table.

Run `make bench` and `make chaos` against a clean local stack. Record hardware, Docker resources, PostgreSQL settings, payload size, worker count, and commit SHA with every result. Do not compare results across different environments without those controls. This repository does not publish a canonical performance result.
