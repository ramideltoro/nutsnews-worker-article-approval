import {
  getRetryDestination,
  getWorkerRoute,
  validateStagePayload,
  validateWorkerEnvelope,
  type StagePayloadValidationIssue,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createBrokerConsumerReadinessCheck,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type RuntimeHealthCheck,
  type RuntimeHealthReport,
  type RuntimeHealthProbeSet,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyStore,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetrySink,
  type RuntimeValidationIssue
} from "@ramideltoro/nutsnews-worker-runtime";

import type { ApprovalConfig } from "./config.js";
import type {
  ApprovalDependencies,
  ApprovalDependencyProbe
} from "./dependencies.js";
import type {
  ApprovalPrometheusTelemetrySink,
  ApprovalRuntimeMetricsSink
} from "./metrics.js";
import {
  bestEffortTelemetrySink,
  runTelemetryBestEffort
} from "./telemetry.js";

export interface ApprovalServiceOptions {
  readonly config: ApprovalConfig;
  readonly dependencies: ApprovalDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: ApprovalRuntimeMetricsSink;
}

export interface ApprovalService {
  readonly broker: BrokerLifecycle;
  readonly health: RuntimeHealthProbeSet;
  readonly isStarted: boolean;
  readonly isDraining: boolean;
  readonly consumer: BrokerConsumerHandle | undefined;
  start(): Promise<void>;
  stop(): Promise<void>;
  processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult>;
}

export function createApprovalService(options: ApprovalServiceOptions): ApprovalService {
  const approvalRoute = getWorkerRoute("approval");
  const translationRoute = getWorkerRoute("translation");
  const telemetry = bestEffortTelemetrySink(options.telemetry);
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      approvalRoute,
      translationRoute
    ],
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const processor = createApprovalInputProcessor({
    dependencies: options.dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    handler: async (context) => {
      try {
        return await drain.track(async () => {
          setInFlight(options.metrics, approvalRoute.mainQueue.name, drain.inFlight);
          const result = await options.dependencies.workHandler.handle(context, {
            publish: (command) => broker.publish(command),
            recordOutbox: (command, receipt) => options.dependencies.brokerOutbox.record(command, receipt),
            withTransaction: (operation) => options.dependencies.transactionRunner.withTransaction(operation)
          });

          await emitRuntimeTelemetry(telemetry, {
            name: "runtime.dependency.observed",
            level: result.status === "ok" ? "info" : "warn",
            at: runtimeNow(options.dependencies.clock),
            stage: "approval",
            queue: approvalRoute.mainQueue.name,
            outcome: result.status === "ok" ? "success" : result.status === "retry" ? "retry" : "failure",
            attributes: {
              event: "approval.message.delegated",
              dependency: options.dependencies.workHandler.name,
              shadowMode: options.config.shadowMode
            }
          });

          return result;
        });
      } finally {
        setInFlight(options.metrics, approvalRoute.mainQueue.name, drain.inFlight);
      }
    }
  });
  let started = false;
  let consumer: BrokerConsumerHandle | undefined;

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      const probes = createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started)
        ],
        readinessChecks: [
          brokerReadinessCheck(broker),
          createBrokerConsumerReadinessCheck(broker, "approval"),
          dependencyReadinessCheck("approval-state", options.dependencies.stateStore),
          dependencyReadinessCheck("database-transactions", options.dependencies.transactionRunner),
          dependencyReadinessCheck("broker-outbox", options.dependencies.brokerOutbox),
          dependencyReadinessCheck("qwen-client", options.dependencies.qwenClient),
          dependencyReadinessCheck("prompt-registry", options.dependencies.promptRegistry),
          shadowModeCheck(options.config)
        ],
        clock: options.dependencies.clock,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      });

      return observeHealthProbes(probes, options.metrics);
    },
    get isStarted(): boolean {
      return started;
    },
    get isDraining(): boolean {
      return drain.isDraining;
    },
    get consumer(): BrokerConsumerHandle | undefined {
      return consumer;
    },
    async start(): Promise<void> {
      if (started) {
        return;
      }

      await broker.start();
      const brokerConsumer = await broker.consume("approval", processor);
      consumer = {
        stage: brokerConsumer.stage,
        cancel: async () => {
          await brokerConsumer.cancel();
          setHealthProbe(options.metrics, "readiness", "unhealthy");
        }
      };
      started = true;
      setHealthProbe(options.metrics, "startup", "ok");
      setInFlight(options.metrics, approvalRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(telemetry, {
        name: "runtime.dependency.observed",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "approval",
        queue: approvalRoute.mainQueue.name,
        outcome: "success",
        attributes: {
          dependency: "approval-shell",
          mode: options.config.dependencyMode,
          prefetch: options.config.prefetch,
          concurrency: options.config.concurrency,
          qwenModel: options.config.qwen.model,
          shadowMode: options.config.shadowMode
        }
      });
      await refreshReadinessBestEffort(
        () => service.health.readiness(),
        options.metrics
      );
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      setShutdownDraining(options.metrics, true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      setShutdownDraining(options.metrics, false);
      setInFlight(options.metrics, approvalRoute.mainQueue.name, drain.inFlight);
      setHealthProbe(options.metrics, "startup", "unhealthy");
      setHealthProbe(options.metrics, "readiness", "unhealthy");
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies ApprovalService;

  return service;
}

function setHealthProbe(
  metrics: ApprovalRuntimeMetricsSink | undefined,
  probe: "liveness" | "startup" | "readiness",
  outcome: "ok" | "degraded" | "unhealthy"
): void {
  if (isApprovalMetrics(metrics)) {
    runTelemetryBestEffort(() => metrics.setHealthProbe(probe, outcome));
  }
}

function setInFlight(
  metrics: ApprovalRuntimeMetricsSink | undefined,
  queue: string,
  value: number
): void {
  runTelemetryBestEffort(() => metrics?.setInFlight(queue, value));
}

function setShutdownDraining(
  metrics: ApprovalRuntimeMetricsSink | undefined,
  draining: boolean
): void {
  runTelemetryBestEffort(() => metrics?.setShutdownDraining(draining));
}

function observeHealthProbes(
  probes: RuntimeHealthProbeSet,
  metrics: ApprovalRuntimeMetricsSink | undefined
): RuntimeHealthProbeSet {
  const observe = async <T extends RuntimeHealthReport>(
    probe: "liveness" | "startup" | "readiness",
    operation: () => Promise<T>
  ): Promise<T> => {
    const report = await operation();
    setHealthProbe(metrics, probe, report.status);

    return report;
  };

  return {
    liveness: () => observe("liveness", () => probes.liveness()),
    startup: () => observe("startup", () => probes.startup()),
    readiness: () => observe("readiness", () => probes.readiness())
  };
}

async function refreshReadinessBestEffort(
  operation: () => Promise<RuntimeHealthReport>,
  metrics: ApprovalRuntimeMetricsSink | undefined
): Promise<void> {
  try {
    await operation();
  } catch {
    setHealthProbe(metrics, "readiness", "unhealthy");
  }
}

function isApprovalMetrics(
  metrics: ApprovalRuntimeMetricsSink | undefined
): metrics is ApprovalPrometheusTelemetrySink {
  return metrics !== undefined
    && "setHealthProbe" in metrics
    && typeof metrics.setHealthProbe === "function";
}

interface ApprovalInputProcessorOptions {
  readonly dependencies: ApprovalDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  handler(context: RuntimeMessageContext): Promise<{ readonly status: "ok" } | { readonly status: "retry"; readonly reason: string; readonly retryAfterMs?: number } | { readonly status: "terminal-failure"; readonly reason: string }>;
}

// Runtime 0.5.0 validates payload definition.stage. Approval instead owns all
// schemas whose definition.consumer is approval, including enrichmentResult,
// so this processor preserves consumer-aware validation and mirrors runtime events.
function createApprovalInputProcessor(options: ApprovalInputProcessorOptions) {
  return async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const receivedAt = delivery.receivedAt ?? runtimeNow(options.dependencies.clock);
    const startedAtMs = options.dependencies.clock.now().getTime();
    const queue = getWorkerRoute("approval").mainQueue.name;
    await emitRuntimeTelemetry(options.telemetry, {
      name: "runtime.message.started",
      level: "info",
      at: runtimeNow(options.dependencies.clock),
      stage: "approval",
      queue,
      outcome: "started"
    });

    const envelopeResult = validateWorkerEnvelope(delivery.envelope);

    if (!envelopeResult.ok) {
      const issues = envelopeResult.issues.map(toRuntimeValidationIssue);
      await emitInvalid(options.telemetry, undefined, issues, options.dependencies.clock, queue, elapsedMs(options.dependencies.clock, startedAtMs));

      return {
        action: "dlq",
        reason: "invalid-envelope",
        issues
      };
    }

    const envelope = envelopeResult.value;

    if (envelope.route !== "approval") {
      const issues = [
        {
          path: "$.route",
          code: "stage-mismatch",
          message: `Envelope route ${envelope.route} does not match processor stage approval.`
        }
      ];
      await emitInvalid(options.telemetry, envelope, issues, options.dependencies.clock, queue, elapsedMs(options.dependencies.clock, startedAtMs));

      return terminalResult(envelope, "stage-mismatch", issues);
    }

    const payloadResult = validateStagePayload(delivery.payload);

    if (!payloadResult.ok) {
      const issues = payloadResult.issues.map(toRuntimeValidationIssue);
      await emitInvalid(options.telemetry, envelope, issues, options.dependencies.clock, queue, elapsedMs(options.dependencies.clock, startedAtMs));

      return terminalResult(envelope, "invalid-payload", issues);
    }

    if (payloadResult.definition.consumer !== "approval") {
      const issues = [
        {
          path: "$.schemaId",
          code: "payload-consumer-mismatch",
          message: `Payload schema consumer ${payloadResult.definition.consumer} does not match approval.`
        }
      ];
      await emitInvalid(options.telemetry, envelope, issues, options.dependencies.clock, queue, elapsedMs(options.dependencies.clock, startedAtMs));

      return terminalResult(envelope, "payload-consumer-mismatch", issues);
    }

    let claim: RuntimeIdempotencyClaimResult;

    try {
      claim = await options.dependencies.stateStore.claim(envelope.idempotencyKey, {
        envelope,
        stage: "approval",
        receivedAt
      });
    } catch {
      return completeWithRetryOrDlq(
        options.telemetry,
        envelope,
        "idempotency-claim-error",
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );
    }

    if (claim.status === "already-completed") {
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.message.duplicate",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "approval",
        ...envelopeTelemetryFields(envelope, queue, elapsedMs(options.dependencies.clock, startedAtMs)),
        outcome: "duplicate",
        attributes: {
          firstSeenAt: claim.firstSeenAt,
          completedAt: claim.completedAt
        }
      });

      return {
        action: "ack",
        reason: "duplicate",
        envelope
      };
    }

    if (claim.status === "in-progress") {
      const result = retryOrDlq(envelope, "idempotency-in-progress", 1_000);
      await emitRetryOrDlq(options.telemetry, result, options.dependencies.clock, queue, elapsedMs(options.dependencies.clock, startedAtMs));

      return result;
    }

    const context: RuntimeMessageContext = {
      envelope,
      payload: payloadResult.value,
      stage: "approval",
      receivedAt
    };

    let result: Awaited<ReturnType<ApprovalInputProcessorOptions["handler"]>>;

    try {
      result = await options.handler(context);
    } catch (error: unknown) {
      try {
        await markFailed(options.dependencies.stateStore, envelope, classifyHandlerError(error), true, options.dependencies.clock);
      } catch {
        return completeWithRetryOrDlq(
          options.telemetry,
          envelope,
          "idempotency-mark-failed-error",
          options.dependencies.clock,
          queue,
          elapsedMs(options.dependencies.clock, startedAtMs)
        );
      }

      return completeWithRetryOrDlq(
        options.telemetry,
        envelope,
        "handler-error",
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );
    }

    if (result.status === "ok") {
      try {
        await markCompleted(options.dependencies.stateStore, envelope, options.dependencies.clock);
      } catch {
        await markFailedBestEffort(
          options.dependencies.stateStore,
          envelope,
          "idempotency-mark-completed-error",
          options.dependencies.clock
        );

        return completeWithRetryOrDlq(
          options.telemetry,
          envelope,
          "idempotency-mark-completed-error",
          options.dependencies.clock,
          queue,
          elapsedMs(options.dependencies.clock, startedAtMs)
        );
      }

      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.message.accepted",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "approval",
        ...envelopeTelemetryFields(envelope, queue, elapsedMs(options.dependencies.clock, startedAtMs)),
        outcome: "success"
      });

      return {
        action: "ack",
        reason: "handled",
        envelope
      };
    }

    if (result.status === "retry") {
      try {
        await markFailed(options.dependencies.stateStore, envelope, result.reason, true, options.dependencies.clock);
      } catch {
        return completeWithRetryOrDlq(
          options.telemetry,
          envelope,
          "idempotency-mark-failed-error",
          options.dependencies.clock,
          queue,
          elapsedMs(options.dependencies.clock, startedAtMs)
        );
      }

      return completeWithRetryOrDlq(
        options.telemetry,
        envelope,
        result.reason,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs),
        result.retryAfterMs
      );
    }

    try {
      await markFailed(options.dependencies.stateStore, envelope, result.reason, false, options.dependencies.clock);
    } catch {
      return completeWithRetryOrDlq(
        options.telemetry,
        envelope,
        "idempotency-mark-failed-error",
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );
    }

    const processingResult = terminalResult(envelope, result.reason);
    await emitRetryOrDlq(options.telemetry, processingResult, options.dependencies.clock, queue, elapsedMs(options.dependencies.clock, startedAtMs));

    return processingResult;
  };
}

function livenessCheck(): RuntimeHealthCheck {
  return {
    name: "process",
    critical: true,
    check: () => "ok"
  };
}

function startupCheck(isStarted: () => boolean): RuntimeHealthCheck {
  return {
    name: "service-started",
    critical: true,
    check: () => isStarted() ? "ok" : "unhealthy"
  };
}

function brokerReadinessCheck(broker: BrokerLifecycle): RuntimeHealthCheck {
  return {
    name: "broker-lifecycle",
    critical: true,
    check: () => broker.state === "ready"
      ? {
          status: "ok",
          details: {
            state: broker.state
          }
        }
      : {
          status: "unhealthy",
          details: {
            state: broker.state
          }
        }
  };
}

function dependencyReadinessCheck(
  name: string,
  dependency: {
    readonly name: string;
    probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
  }
): RuntimeHealthCheck {
  return {
    name,
    critical: true,
    check: async () => {
      const probe = await dependency.probe();

      return {
        status: probe.status,
        details: {
          dependency: dependency.name,
          summary: probe.summary
        }
      };
    }
  };
}

function shadowModeCheck(config: ApprovalConfig): RuntimeHealthCheck {
  return {
    name: "shadow-mode",
    critical: true,
    check: () => config.shadowMode
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: "shadow-mode-disabled"
          }
        }
  };
}

function retryOrDlq(
  envelope: WorkerMessageEnvelope,
  reason: string,
  retryAfterMs?: number
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.count);

  if ("ttlMs" in destination) {
    if (retryAfterMs === undefined) {
      return {
        action: "retry",
        reason,
        envelope,
        destination
      };
    }

    return {
      action: "retry",
      reason,
      envelope,
      destination,
      retryAfterMs
    };
  }

  return {
    action: "dlq",
    reason,
    envelope,
    destination
  };
}

function terminalResult(
  envelope: WorkerMessageEnvelope,
  reason: string,
  issues?: readonly RuntimeValidationIssue[]
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.max);

  if (issues === undefined) {
    return {
      action: "dlq",
      reason,
      envelope,
      destination
    };
  }

  return {
    action: "dlq",
    reason,
    envelope,
    destination,
    issues
  };
}

function toRuntimeValidationIssue(issue: StagePayloadValidationIssue | RuntimeValidationIssue): RuntimeValidationIssue {
  return {
    path: issue.path,
    code: issue.code,
    message: issue.message
  };
}

async function markCompleted(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  clock: ApprovalDependencies["clock"]
): Promise<void> {
  await store.markCompleted(envelope.idempotencyKey, {
    completedAt: runtimeNow(clock),
    messageId: envelope.messageId,
    stage: "approval"
  });
}

async function markFailed(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  reason: string,
  retryable: boolean,
  clock: ApprovalDependencies["clock"]
): Promise<void> {
  await store.markFailed(envelope.idempotencyKey, {
    failedAt: runtimeNow(clock),
    messageId: envelope.messageId,
    stage: "approval",
    reason,
    retryable
  });
}

async function markFailedBestEffort(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  reason: string,
  clock: ApprovalDependencies["clock"]
): Promise<void> {
  try {
    await markFailed(store, envelope, reason, true, clock);
  } catch {
    // The delivery still follows the bounded retry/DLQ path below.
  }
}

async function completeWithRetryOrDlq(
  telemetry: RuntimeTelemetrySink | undefined,
  envelope: WorkerMessageEnvelope,
  reason: string,
  clock: ApprovalDependencies["clock"],
  queue: string,
  durationMs: number,
  retryAfterMs?: number
): Promise<RuntimeMessageProcessingResult> {
  const processingResult = retryOrDlq(envelope, reason, retryAfterMs);
  await emitRetryOrDlq(telemetry, processingResult, clock, queue, durationMs);

  return processingResult;
}

function classifyHandlerError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-handler-error";
}

async function emitInvalid(
  telemetry: RuntimeTelemetrySink | undefined,
  envelope: WorkerMessageEnvelope | undefined,
  issues: readonly RuntimeValidationIssue[],
  clock: ApprovalDependencies["clock"],
  queue: string,
  durationMs: number
): Promise<void> {
  const firstIssue = issues[0];
  const attributes = firstIssue === undefined
    ? undefined
    : {
        issueCode: firstIssue.code,
        issuePath: firstIssue.path
      };

  await emitRuntimeTelemetry(telemetry, {
    name: "runtime.message.invalid",
    level: "warn",
    at: runtimeNow(clock),
    stage: "approval",
    queue,
    durationMs,
    outcome: "failure",
    ...(envelope === undefined
      ? {}
      : envelopeTelemetryFields(envelope, queue, durationMs)),
    ...(attributes === undefined
      ? {}
      : {
          attributes
        })
  });
}

async function emitRetryOrDlq(
  telemetry: RuntimeTelemetrySink | undefined,
  result: RuntimeMessageProcessingResult,
  clock: ApprovalDependencies["clock"],
  queue: string,
  durationMs: number
): Promise<void> {
  if (result.action === "retry") {
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.retry",
      level: "warn",
      at: runtimeNow(clock),
      stage: "approval",
      ...envelopeTelemetryFields(result.envelope, queue, durationMs),
      outcome: "retry",
      attributes: {
        reason: result.reason,
        destination: result.destination.name
      }
    });

    return;
  }

  if (result.action === "dlq") {
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.dlq",
      level: "error",
      at: runtimeNow(clock),
      stage: "approval",
      ...(result.envelope === undefined
        ? {}
        : envelopeTelemetryFields(result.envelope, queue, durationMs)),
      outcome: "dlq",
      attributes: {
        reason: result.reason,
        destination: result.destination?.name ?? "unroutable"
      }
    });
  }
}

function elapsedMs(clock: ApprovalDependencies["clock"], startedAtMs: number): number {
  return Math.max(0, clock.now().getTime() - startedAtMs);
}

function envelopeTelemetryFields(
  envelope: WorkerMessageEnvelope,
  queue: string,
  durationMs: number
): Readonly<Record<string, string | number>> {
  const base = {
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    traceparent: envelope.traceparent,
    idempotencyKey: envelope.idempotencyKey,
    queue,
    attempt: envelope.attempt.count,
    durationMs
  } as const;

  if (envelope.tracestate === undefined) {
    return base;
  }

  return {
    ...base,
    tracestate: envelope.tracestate
  };
}
