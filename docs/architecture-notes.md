# Approval Service Bootstrap Notes

## Owned Boundary

This repository owns the approval worker process shell only. It does not own feed fetching, canonical identity, enrichment, translation implementation, persistence writes, publication gating, deployment secrets, or backend cutover controls.

## Runtime Shape

1. Load value-free config and reject disabled shadow mode.
2. Start the shared broker lifecycle for the `approval` consumer route and downstream `translation` route.
3. Register an approval consumer that accepts payload schemas whose contract consumer is `approval`.
4. Delegate work to an injected approval handler behind an in-flight drain controller.
5. Expose `/live`, `/startup`, `/ready`, `/metrics`, and `/config-schema`.
6. On shutdown, cancel consumer intake, keep lease heartbeats active while in-flight handlers drain, and close the broker lifecycle. If the bounded drain deadline expires, abort active ownership before closing dependencies.

Runtime `1.0.0` owns consumer-aware payload validation, lifecycle telemetry, bounded retry/DLQ decisions, and token-aware idempotency transitions through the shared message processor. Approval passes its state store directly to that processor, so valid enrichment-to-approval handoffs use the common implementation instead of a service-local compatibility adapter.

The production state store records each active claim's opaque token and acquisition time from PostgreSQL's wall clock (`clock_timestamp()`). The lease is exactly 300 seconds. Completion, failure, and explicit release require the matching token and a server-evaluated lease age strictly below 300 seconds. Reclaim uses the complementary age predicate—greater than or equal to 300 seconds—and atomically matches `processing`, the selected token, and the selected timestamp before issuing a fresh token. At the exact boundary only reclaim is eligible. Fresh, malformed, foreign, and completed records are not reclaimed, and successful owner mutations remove active lease metadata.

The store owns a single-flight heartbeat for each claim. It renews at most every 60 seconds using a checked-out PostgreSQL client, a server-side statement timeout, the stored token, the `processing` status, and an unexpired-lease predicate. A missing row, query error, timeout, dependency close, or over-deadline shutdown aborts the claim's ownership signal. Database, Qwen, and RabbitMQ operations receive that signal and are bounded to 45 seconds; successful completion additionally requires the matching active local claim before its server-side compare-and-set. Claim acquisition is fenced against dependency close, and graceful shutdown keeps heartbeats alive until tracked work finishes.

No Runtime claim-context extension is required for this store-owned heartbeat. The service nevertheless remains hard-coded shadow-only: disabled shadow mode is rejected, deployment identity is `shadow`, and expected-active is zero. Production ownership remains blocked because decision writes are not yet conditioned on the inbox token/lease in the same server transaction, and RabbitMQ publication can precede durable outbox intent. A server-fenced transactional outbox is required before cutover so a paused or stale process cannot write or publish after lease transfer and broker-confirm ambiguity is resolved through durable reconciliation.

Telemetry sinks are isolated at application, work-handler, transport, and service boundaries. Synchronous throws and rejected emission promises are swallowed independently because logs and metrics are non-semantic: they cannot change idempotency records, acknowledgement, retry, or DLQ decisions. Compatibility probe gauges initialize to liveness OK with startup/readiness unhealthy, then follow observed lifecycle state. Runtime owns expected-active, per-check status/duration, and last-success metrics; the local wrapper removes Runtime's overlapping probe family so it is exported exactly once. Metrics scrapes re-evaluate liveness, startup, and readiness before collection.

## AI Readiness

Qwen readiness is a readiness dependency, not a liveness dependency. A degraded model endpoint must remove the worker from delivery readiness without causing process restarts by itself.

## Secrets

No prompt body, model output, endpoint URL, API key, database URL, or RabbitMQ URL is committed or emitted by `/config-schema`. Local test doubles use synthetic prompt identifiers only.
