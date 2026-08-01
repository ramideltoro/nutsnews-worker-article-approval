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

Runtime `1.0.0` owns consumer-aware payload validation, lifecycle telemetry, bounded retry/DLQ decisions, and token-aware idempotency transitions through the shared message processor. Approval passes its state store directly to that processor, so valid enrichment-to-approval handoffs use the common implementation instead of a service-local compatibility adapter.

The production state store records each active claim's opaque token and acquisition time from PostgreSQL's statement clock. The lease is exactly 300 seconds, longer than the service's bounded model and handler timeouts but no longer than five minutes. A later delivery can reclaim only when one atomic update still matches `processing`, the selected token, the selected timestamp, and the server-evaluated expiry; the replacement gets a fresh token. Fresh, malformed, foreign, and completed records are not reclaimed, and completion, failure, or explicit release removes active lease metadata through the same token-aware compare-and-set boundary.

Telemetry sinks are isolated at application, work-handler, transport, and service boundaries. Synchronous throws and rejected emission promises are swallowed independently because logs and metrics are non-semantic: they cannot change idempotency records, acknowledgement, retry, or DLQ decisions. Compatibility probe gauges initialize to liveness OK with startup/readiness unhealthy, then follow observed lifecycle state.

## AI Readiness

Qwen readiness is a readiness dependency, not a liveness dependency. A degraded model endpoint must remove the worker from delivery readiness without causing process restarts by itself.

## Secrets

No prompt body, model output, endpoint URL, API key, database URL, or RabbitMQ URL is committed or emitted by `/config-schema`. Local test doubles use synthetic prompt identifiers only.
