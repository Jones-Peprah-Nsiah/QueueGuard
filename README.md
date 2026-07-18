# QueueGuard

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
| Deployment target | AWS ECS (Fargate) | See `infra/` for local-dev equivalents; ECS task defs are a follow-up |

## Tech stack

Java 21, Spring Boot 3.4, Redis (Lettuce client) + Lua scripts + Streams, PostgreSQL + Spring Data JPA, Resilience4j, Micrometer/Prometheus/Grafana, Docker, Maven multi-module, JUnit 5 + Testcontainers.

## Project layout

```
queueguard/
├── shared-library/     # DTOs/constants shared by api-service and worker-service
├── api-service/        # public API: rate limiting + enqueueing
├── worker-service/      # consumer-group workers: dequeue, process, ack, reap
├── infra/prometheus/    # Prometheus scrape config
└── docker-compose.yml   # local dev stack: redis, postgres, prometheus, grafana, both services
```

## Running locally

Requires Docker running (`brew install --cask docker`, then open Docker.app once to finish setup and grant permissions).

```bash
docker compose up --build
```

- `api-service` on `:8080` — try `POST /api/jobs` with headers `X-User-Id`, `X-User-Tier` (`PREMIUM`/`FREE`) and a JSON body `{"payload": "..."}`
- `worker-service` on `:8081`
- Prometheus on `:9090`, Grafana on `:3000` (login `admin`/`admin`)
- Swagger UI on `api-service` at `/swagger-ui.html`

To run just the infra (Redis/Postgres/Prometheus/Grafana) and the services locally via Maven instead:

```bash
docker compose up redis postgres prometheus grafana
mvn -pl api-service -am spring-boot:run
mvn -pl worker-service -am spring-boot:run
```

## Status

Scaffolded: rate limiter, stream enqueue/dequeue, weighted scheduling, reaper, metrics wiring, local Docker Compose stack. Not yet built: ECS task definitions, GitHub Actions CI/CD, auth/tier lookup (currently a stubbed `X-User-Tier` header), dead-letter queue policy beyond the reaper's reclaim.
