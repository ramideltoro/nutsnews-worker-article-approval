# nutsnews-worker-article-approval

Deployable worker-uplift approval service shell for NutsNews.

## Responsibility

Consume approval jobs, call the Qwen approval endpoint with bounded concurrency and retries, store decisions in shadow state, and publish translation jobs only for approved articles.

This service uses a versioned editorial prompt with the configured Qwen-compatible endpoint, validates structured approval decisions, stores traceable shadow decisions, and emits translation work only for accepted articles.

## Runtime Surface

- Consumes the contracted `approval` route and asserts the downstream `translation` route for future publish work.
- Accepts payload schemas whose contract consumer is `approval`, including enrichment-produced `enrichmentResult` messages.
- Loads the bounded enrichment metadata record by reference, rejects missing-thumbnail inputs before model review, and calls Qwen only for hydrated article candidates.
- Limits in-process Qwen calls by configured per-worker capacity, queues only up to the RabbitMQ prefetch bound, and returns bounded retries when saturated.
- Validates accepted/rejected model responses, bounded scores, sanitized reason codes, and accepted source summaries before recording a decision.
- Stores article, message, prompt, model, trace, latency, token, review, and summary references without logging prompt bodies, credentials, or unrestricted model output.
- Reuses recorded decisions for the same article version, prompt version, and model, so delivery replays do not duplicate Qwen calls or translation publishes.
- Publishes contracted `translationTask` messages only when the stored decision is accepted and has not already been published downstream.
- Keeps legacy OpenAI fallback disabled unless an explicit protected flag, nonzero budget, provenance marker, and alert topic are all configured.
- Uses shared runtime broker lifecycle, in-flight drain, idempotency store, retry/DLQ destinations, health reports, and Prometheus metrics.
- Gives each production PostgreSQL idempotency claim a database-timed 300-second lease. A store-owned, single-flight heartbeat renews it at most every 60 seconds through a dedicated, bounded PostgreSQL connection and refreshes time from `clock_timestamp()`. Renewal miss, error, timeout, dependency close, and over-deadline shutdown abort the local ownership signal; database, Qwen, and broker operations consume that signal and use a 45-second maximum operation window. Completion requires the matching live local claim plus a token-and-unexpired-lease compare-and-set. Failure and release use the same compare-and-set as fail-closed cleanup. At the exact boundary owner mutations fail and reclaim becomes eligible; reclaim atomically compares status, token, and stored acquisition timestamp, issues a fresh token, and cannot downgrade completed or concurrently replaced work.
- Emits exactly one completing lifecycle event for each delivery (`accepted`, `duplicate`, `invalid`, `retry`, or `dlq`) with message identifiers retained only as structured log metadata and bounded shared labels used for Prometheus series. The metrics endpoint also exposes `nutsnews_worker_uplift_stage_events_total`, seeding the shared six-outcome contract (`success`, `duplicate`, `invalid`, `retry`, `dlq`, and `failure`) even when an outcome remains zero, a fixed-bucket `nutsnews_worker_uplift_stage_latency_seconds` histogram with the Runtime 1 boundaries from 5 milliseconds through 300 seconds, and Runtime-owned `nutsnews_worker_expected_active=0` while this deployment remains shadow-only. Accepted and duplicate completions advance the Runtime last-success timestamp monotonically.
- Uses Runtime 1.0 fixed-bucket processing and dependency histograms in seconds alongside the canonical stage histogram. Duration-less dependency events remain available as structured logs without fabricating zero-duration samples.
- Exposes one local, one-hot `liveness`, `startup`, and `readiness` gauge family from conservative pre-start defaults while forwarding health events to Runtime 1 for bounded per-check status and duration families. A `/metrics` scrape freshly evaluates all probes, including readiness, before collection.
- Keeps liveness independent from Qwen readiness; `/live` only checks process health, while `/ready` gates an active `approval` main-queue consumer, broker, state, outbox, Qwen, prompt registry, and shadow mode.
- Refuses startup before broker connection or consumer registration when a production environment is paired with test dependency mode, or when production dependency mode is paired with non-production adapters. Telemetry reports the adapter mode supplied by the actual dependency composition.
- Emits bounded structured events and Prometheus metrics when RabbitMQ cancels the consumer, drops its channel, or restores consumption.

## Configuration

The HTTP server exposes `/config-schema` with names, defaults, sensitivity, and production requirements only. Runtime config records dependency presence booleans and never retains database URLs, RabbitMQ URLs, Qwen endpoint URLs, or API keys.

| Variable | Default | Production | Sensitive |
| --- | --- | --- | --- |
| `NUTSNEWS_ENVIRONMENT` | `local` | must be `production` for production deployment | no |
| `NUTSNEWS_APPROVAL_DEPENDENCY_MODE` | `test` | must be `production` when the environment is production | no |
| `NUTSNEWS_APPROVAL_BUILD_REVISION` | `development` | required lowercase 40-character Git SHA | no |
| `NUTSNEWS_APPROVAL_DATABASE_URL` | unset | required | yes |
| `NUTSNEWS_APPROVAL_RABBITMQ_URL` | unset | required | yes |
| `NUTSNEWS_APPROVAL_QWEN_BASE_URL` | unset | required | yes |
| `NUTSNEWS_APPROVAL_QWEN_API_KEY` | unset | required | yes |
| `NUTSNEWS_APPROVAL_QWEN_MODEL` | `qwen2.5:3b` | optional | no |
| `NUTSNEWS_APPROVAL_PROMPT_ID` | `editorial-approval-v1` | optional | no |
| `NUTSNEWS_APPROVAL_TARGET_LANGUAGES` | `fr,ja,de-CH,de,el` | optional | no |
| `NUTSNEWS_APPROVAL_QWEN_TOTAL_TIMEOUT_MS` | `30000` | optional | no |
| `NUTSNEWS_APPROVAL_QWEN_MAX_INPUT_BYTES` | `32768` | optional | no |
| `NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS` | `1` | optional | no |
| `NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS` | `3` | optional | no |
| `NUTSNEWS_APPROVAL_QWEN_BACKPRESSURE_RETRY_AFTER_MS` | `5000` | optional | no |
| `NUTSNEWS_APPROVAL_SUMMARY_MIN_CHARS` | `40` | optional | no |
| `NUTSNEWS_APPROVAL_SUMMARY_MAX_CHARS` | `600` | optional | no |
| `NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ENABLED` | `false` | optional | no |
| `NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROTECTED_FLAG` | `false` | required if fallback enabled | no |
| `NUTSNEWS_APPROVAL_OPENAI_FALLBACK_BUDGET_USD` | `0` | required if fallback enabled | no |
| `NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROVENANCE_MARKER` | unset | must be `legacy_openai_fallback` if fallback enabled | no |
| `NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ALERT_TOPIC` | unset | required if fallback enabled | no |
| `NUTSNEWS_APPROVAL_CONCURRENCY` | `2` | optional | no |
| `NUTSNEWS_APPROVAL_PREFETCH` | `4` | optional | no |
| `NUTSNEWS_APPROVAL_SHADOW_MODE` | `true` | must remain true here | no |

## Local Verification

```sh
npm ci
npm run ci
NODE_AUTH_TOKEN=<github-packages-token> npm run container:build
```

`npm run ci` runs lint, typecheck, unit tests, the approval eval corpus, integration tests, build, SBOM generation, and a production dependency audit. The Dockerfile build stage also runs `npm run ci`, so published images are gated by the eval thresholds.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-article-approval:${GITHUB_SHA}`. The image runs as a non-root user and is deployable only through backend-owned infrastructure.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

This worker remains deliberately shadow-only: configuration rejects `NUTSNEWS_APPROVAL_SHADOW_MODE=false`, deployment identity remains `shadow`, and Runtime exports `nutsnews_worker_expected_active=0`. The lease heartbeat makes the current shadow observability rollout fail closed, but it is not a production-ownership cutover gate. Production ownership still requires server-fenced business mutations and a transactional outbox that durably records publish intent before RabbitMQ publication, eliminating the current publish/outbox ambiguity.

## Package / Image Access

Backend deployments consume immutable SHA-tagged GHCR images. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for GitHub Actions CI. Workflows use least-privilege permissions and request `packages: write` only for publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
