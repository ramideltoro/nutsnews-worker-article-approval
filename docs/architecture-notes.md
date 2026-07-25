# Approval Service Bootstrap Notes

## Owned Boundary

This repository owns the approval worker process shell only. It does not own feed fetching, canonical identity, enrichment, translation implementation, persistence writes, publication gating, deployment secrets, or backend cutover controls.

## Runtime Shape

1. Load value-free config and reject disabled shadow mode.
2. Start the shared broker lifecycle for the `approval` consumer route and downstream `translation` route.
3. Register an approval consumer that accepts payload schemas whose contract consumer is `approval`.
4. Delegate work to an injected approval handler behind an in-flight drain controller.
5. Expose `/live`, `/startup`, `/ready`, `/metrics`, and `/config-schema`.
6. On shutdown, stop accepting deliveries, wait for in-flight handlers, cancel consumers, and close broker lifecycle.

## AI Readiness

Qwen readiness is a readiness dependency, not a liveness dependency. A degraded model endpoint must remove the worker from delivery readiness without causing process restarts by itself.

## Secrets

No prompt body, model output, endpoint URL, API key, database URL, or RabbitMQ URL is committed or emitted by `/config-schema`. Local test doubles use synthetic prompt identifiers only.
