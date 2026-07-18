# QueueGuard

[![CI](https://github.com/Jones-Peprah-Nsiah/QueueGuard/actions/workflows/ci.yml/badge.svg)](https://github.com/Jones-Peprah-Nsiah/QueueGuard/actions/workflows/ci.yml)

Distributed API rate limiter and queue management system, modeled on the kind of admission-control and job-processing infrastructure used by high-traffic APIs.

## Architecture

```
Client
  |
  v
api-service (Spring Boot)
  - Sliding-window-log rate limiter (Redis + Lua, atomic)
  - Fail-open circuit breaker (Resilience4j) if Redis is unavailable
  - Enqueues admitted requests onto Redis Streams
  |
  v
Redis Streams (premium-stream / free-stream, consumer group)
  |
  v
worker-service (Spring Boot)
  - Weighted 3:1 premium:free dequeue (starvation-free scheduling)
  - Consumer-group ack/retry via XACK
  - Reaper sweeps stale pending entries (XPENDING -> XCLAIM) for crashed workers
  |
  v
PostgreSQL (job_history)
  |
  v
Prometheus + Grafana (throughput, queue depth, worker lag, rate-limit violations)
```

## Locked-in design decisions

| Concern | Choice | Why |
|---|---|---|
| Rate limiter algorithm | Sliding window log, atomic Lua script | Avoids the fixed-window boundary-burst problem; Lua gives true atomicity over `MULTI`/`WATCH` retries |
| Queue reliability | Redis Streams + consumer groups | Native ack/retry/pending-entry tracking instead of hand-rolled lease management on Sorted Sets |
| Priority scheduling | Two streams (`premium-stream`, `free-stream`), worker polls 3:1 | Streams have no native priority; weighted polling avoids free-tier starvation |
| Redis outage behavior | Fail open via Resilience4j circuit breaker | Availability prioritized over strict enforcement — the limiter shouldn't be a SPOF for the whole API |
| Observability | Micrometer -> Prometheus -> Grafana | Production-standard instead of a hand-rolled dashboard |
| Deployment target | AWS ECS (Fargate), not yet built | See "Status" below — deferred until the core system was proven correct |

## Tech stack

Java 21, Spring Boot 3.4, Redis (Lettuce client) + Lua scripts + Streams, PostgreSQL + Spring Data JPA, Resilience4j, Micrometer/Prometheus/Grafana, Docker, Maven multi-module, JUnit 5 + Testcontainers, k6, GitHub Actions.

## Project layout

```
queueguard/
├── shared-library/         # DTOs/constants shared by api-service and worker-service
├── api-service/            # public API: rate limiting + enqueueing
├── worker-service/         # consumer-group workers: dequeue, process, ack, reap
├── infra/prometheus/       # Prometheus scrape config
├── load-tests/             # k6 load test
├── .github/workflows/      # CI: test, package, build Docker images
└── docker-compose.yml      # local dev stack: redis, postgres, prometheus, grafana, both services
```

## Running locally

Requires Docker running (`brew install --cask docker`, then open Docker.app once to finish setup and grant permissions).

```bash
docker compose up --build
```

- `api-service` on `:8080` — try `POST /api/jobs` with headers `X-User-Id`, `X-User-Tier` (`PREMIUM`/`FREE`) and a JSON body `{"payload": "..."}`
- Prometheus on `:9090`, Grafana on `:3000` (login `admin`/`admin`)
- Swagger UI on `api-service` at `/swagger-ui.html`

To run just the infra and the services locally via Maven instead:

```bash
docker compose up redis postgres prometheus grafana
mvn -pl api-service -am spring-boot:run
mvn -pl worker-service -am spring-boot:run
```

## Testing

```bash
mvn test
```

Runs all unit and integration tests, including the Testcontainers-backed ones that spin up real Redis and Postgres containers — nothing here is mocked. Also runs automatically on every push via [GitHub Actions](.github/workflows/ci.yml).

- `RateLimiterServiceTest` — proves the sliding-window admits up to the limit, denies past it, re-admits once the window elapses, and (the core claim behind choosing a Lua script over `MULTI`/`WATCH`) admits **exactly** the configured limit when 50 threads hit it concurrently.
- `JobProcessorTest` — the enqueue → claim → process → ack → persist round trip against real Redis and Postgres.
- `PendingJobReaperTest` — a regression test locking in a real bug found during manual crash testing (see below): a job abandoned by a dead consumer gets reclaimed and completed, and a job still genuinely in flight is left alone.

## Load testing

```bash
k6 run load-tests/queueguard-load-test.js
```

Measured locally (single machine, loopback — not a distributed load generator, so treat as a lower bound rather than a production benchmark):

- **~9,700 req/sec** sustained, **p95 latency 6.2ms**, **0% error rate** on admitted traffic (throughput scenario: many distinct users, each isolated by their own per-user rate limit)
- Sustained concurrent load against a single fixed user correctly produced `429`s once its limit was hit (66,976 of them over 30s), proving the limiter holds under real concurrency, not just a single burst

## What testing actually found

Manually crash-testing the worker pool (kill a container mid-job, watch what happens) surfaced two real bugs that are worth naming rather than hiding:

1. **The reaper reclaimed stale jobs but never processed them.** `XCLAIM` reassigned ownership and logged a warning, then stopped — the job was stuck forever, since Streams only auto-delivers a message once via `XREADGROUP '>'`. Fixed by having the reaper drive reclaimed messages through the same process-and-ack path a live worker uses.
2. **A stale-threshold shorter than actual processing time caused duplicate concurrent processing.** With the threshold below the simulated job duration, three separate workers reclaimed and reprocessed the *same* in-flight job before one of them finally acked it — a classic at-least-once-delivery pitfall. Fixed by requiring the threshold to exceed worst-case processing time, and it's now enforced implicitly by `PendingJobReaperTest`.
3. **The rate limiter and the queue have no backpressure between them.** The k6 throughput run admitted and enqueued ~517,000 jobs in about a minute — all correctly, each caller was under its own per-user limit. But at 2 workers processing roughly one job per 15 simulated-work seconds each, that backlog would take *weeks* to drain. Admission control here only checks the caller's own rate limit, not whether the queue is already saturated, so a client can be legitimately "allowed" to enqueue work that won't be processed for a very long time. Not fixed — noted as the next real architectural gap to close (options: reject or shed load past a queue-depth threshold, expose queue depth so callers can back off, or make processing fast enough that it doesn't matter for real workloads).

## Status

**Done and verified** (not just written — exercised against real Redis/Postgres, real concurrent load, and a real crashed worker): sliding-window rate limiter, Streams + consumer groups, weighted premium/free scheduling, crash recovery via the reaper, Testcontainers test suite, k6 load test, GitHub Actions CI (green on GitHub's own infrastructure, not just locally), Swagger UI.

**Known gaps, stated rather than hidden:**
- Job "processing" is currently a `Thread.sleep` placeholder — there's no real business logic to execute yet, only the pipeline it would flow through.
- Auth is a stubbed `X-User-Tier` request header, not real authentication or a real user-tier lookup.
- `JobHistory` writes use a read-then-write pattern that isn't safe under truly concurrent writers (a lost-update risk) — identified while fixing bug #2 above, not yet hardened with optimistic locking or a unique constraint.
- No dead-letter cap: the `attempts` field is tracked on each job but nothing currently stops a permanently-failing job from being retried forever.
- ECS task definitions and a container registry push step (GHCR/ECR) aren't built — CI builds the Docker images to catch breakage, but doesn't push or deploy them anywhere yet.
