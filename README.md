# nutsnews-worker-article-approval

Deployable worker-uplift approval service shell for NutsNews.

## Responsibility

Consume approval jobs, call the Qwen approval endpoint with bounded concurrency and retries, store decisions in shadow state, and publish translation jobs only for approved articles.

This service uses a versioned editorial prompt with the configured Qwen-compatible endpoint, validates structured approval decisions, stores traceable shadow decisions, and emits translation work only for accepted articles.

## Runtime Surface

- Consumes the contracted `approval` route and asserts the downstream `translation` route for future publish work.
- Accepts payload schemas whose contract consumer is `approval`, including enrichment-produced `enrichmentResult` messages.
- Loads the bounded enrichment metadata record by reference, rejects missing-thumbnail inputs before model review, and calls Qwen only for hydrated article candidates.
- Validates accepted/rejected model responses, bounded scores, sanitized reason codes, and accepted source summaries before recording a decision.
- Stores article, message, prompt, model, trace, latency, token, review, and summary references without logging prompt bodies, credentials, or unrestricted model output.
- Reuses recorded decisions for the same article version, prompt version, and model, so delivery replays do not duplicate Qwen calls or translation publishes.
- Publishes contracted `translationTask` messages only when the stored decision is accepted and has not already been published downstream.
- Uses shared runtime broker lifecycle, in-flight drain, idempotency store, retry/DLQ destinations, health reports, and Prometheus metrics.
- Keeps liveness independent from Qwen readiness; `/live` only checks process health, while `/ready` gates broker, state, outbox, Qwen, prompt registry, and shadow mode.

## Configuration

The HTTP server exposes `/config-schema` with names, defaults, sensitivity, and production requirements only. Runtime config records dependency presence booleans and never retains database URLs, RabbitMQ URLs, Qwen endpoint URLs, or API keys.

| Variable | Default | Production | Sensitive |
| --- | --- | --- | --- |
| `NUTSNEWS_APPROVAL_DATABASE_URL` | unset | required | yes |
| `NUTSNEWS_APPROVAL_RABBITMQ_URL` | unset | required | yes |
| `NUTSNEWS_APPROVAL_QWEN_BASE_URL` | unset | required | yes |
| `NUTSNEWS_APPROVAL_QWEN_API_KEY` | unset | required | yes |
| `NUTSNEWS_APPROVAL_QWEN_MODEL` | `qwen2.5:3b` | optional | no |
| `NUTSNEWS_APPROVAL_PROMPT_ID` | `editorial-approval-v1` | optional | no |
| `NUTSNEWS_APPROVAL_TARGET_LANGUAGES` | `fr,ja,de-CH,de,el` | optional | no |
| `NUTSNEWS_APPROVAL_QWEN_TOTAL_TIMEOUT_MS` | `30000` | optional | no |
| `NUTSNEWS_APPROVAL_QWEN_MAX_INPUT_BYTES` | `32768` | optional | no |
| `NUTSNEWS_APPROVAL_SUMMARY_MIN_CHARS` | `40` | optional | no |
| `NUTSNEWS_APPROVAL_SUMMARY_MAX_CHARS` | `600` | optional | no |
| `NUTSNEWS_APPROVAL_CONCURRENCY` | `2` | optional | no |
| `NUTSNEWS_APPROVAL_PREFETCH` | `4` | optional | no |
| `NUTSNEWS_APPROVAL_SHADOW_MODE` | `true` | must remain true here | no |

## Local Verification

```sh
npm ci
npm run ci
NODE_AUTH_TOKEN=<github-packages-token> npm run container:build
```

`npm run ci` runs lint, typecheck, unit tests, integration tests, build, SBOM generation, and a production dependency audit.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-article-approval:${GITHUB_SHA}`. The image runs as a non-root user and is deployable only through backend-owned infrastructure.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

Backend deployments consume immutable SHA-tagged GHCR images. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for GitHub Actions CI. Workflows use least-privilege permissions and request `packages: write` only for publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
