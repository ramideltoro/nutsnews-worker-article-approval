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

The service-local input processor mirrors the shared runtime's lifecycle events and retry/DLQ helpers while validating payload ownership by `definition.consumer`. The published runtime `0.5.0` processor validates `definition.stage` instead, which would reject the contracted `enrichmentResult` input because it is produced by `enrichment` and consumed by `approval`. Keep this compatibility adapter until the shared runtime exposes consumer-based payload validation.

Telemetry sinks are isolated at application, work-handler, transport, and service boundaries. Synchronous throws and rejected emission promises are swallowed independently because logs and metrics are non-semantic: they cannot change idempotency records, acknowledgement, retry, or DLQ decisions. Compatibility probe gauges initialize to liveness OK with startup/readiness unhealthy, then follow observed lifecycle state.

## AI Readiness

Qwen readiness is a readiness dependency, not a liveness dependency. A degraded model endpoint must remove the worker from delivery readiness without causing process restarts by itself.

## Secrets

No prompt body, model output, endpoint URL, API key, database URL, or RabbitMQ URL is committed or emitted by `/config-schema`. Local test doubles use synthetic prompt identifiers only.
