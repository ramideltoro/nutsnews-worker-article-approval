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
  type PrometheusRuntimeTelemetrySink,
  type RuntimeHealthCheck,
  type RuntimeHealthProbeSet,
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

export interface ApprovalServiceOptions {
  readonly config: ApprovalConfig;
  readonly dependencies: ApprovalDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PrometheusRuntimeTelemetrySink;
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
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      approvalRoute,
      translationRoute
    ],
    clock: options.dependencies.clock,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const processor = createApprovalInputProcessor({
    dependencies: options.dependencies,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    }),
    handler: async (context) => {
      try {
        return await drain.track(async () => {
          options.metrics?.setInFlight(approvalRoute.mainQueue.name, drain.inFlight);
          const result = await options.dependencies.workHandler.handle(context, {
            publish: (command) => broker.publish(command),
            recordOutbox: (command, receipt) => options.dependencies.brokerOutbox.record(command, receipt),
            withTransaction: (operation) => options.dependencies.transactionRunner.withTransaction(operation)
          });

          await emitRuntimeTelemetry(options.telemetry, {
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
        options.metrics?.setInFlight(approvalRoute.mainQueue.name, drain.inFlight);
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
      return createRuntimeHealthProbeSet({
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
        ...(options.telemetry === undefined ? {} : {
          telemetry: options.telemetry
        })
      });
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
      consumer = await broker.consume("approval", processor);
      started = true;
      options.metrics?.recordDependencyLatency(approvalRoute.mainQueue.name, 0, "success");
      options.metrics?.setInFlight(approvalRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(options.telemetry, {
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
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      options.metrics?.setShutdownDraining(true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      options.metrics?.setShutdownDraining(false);
      options.metrics?.setInFlight(approvalRoute.mainQueue.name, drain.inFlight);
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies ApprovalService;

  return service;
}

interface ApprovalInputProcessorOptions {
  readonly dependencies: ApprovalDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  handler(context: RuntimeMessageContext): Promise<{ readonly status: "ok" } | { readonly status: "retry"; readonly reason: string; readonly retryAfterMs?: number } | { readonly status: "terminal-failure"; readonly reason: string }>;
}

function createApprovalInputProcessor(options: ApprovalInputProcessorOptions) {
  return async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const receivedAt = delivery.receivedAt ?? runtimeNow(options.dependencies.clock);
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
      return {
        action: "dlq",
        reason: "invalid-envelope",
        issues: envelopeResult.issues.map(toRuntimeValidationIssue)
      };
    }

    const envelope = envelopeResult.value;

    if (envelope.route !== "approval") {
      return terminalResult(envelope, "stage-mismatch", [
        {
          path: "$.route",
          code: "stage-mismatch",
          message: `Envelope route ${envelope.route} does not match processor stage approval.`
        }
      ]);
    }

    const payloadResult = validateStagePayload(delivery.payload);

    if (!payloadResult.ok) {
      return terminalResult(envelope, "invalid-payload", payloadResult.issues.map(toRuntimeValidationIssue));
    }

    if (payloadResult.definition.consumer !== "approval") {
      return terminalResult(envelope, "payload-consumer-mismatch", [
        {
          path: "$.schemaId",
          code: "payload-consumer-mismatch",
          message: `Payload schema consumer ${payloadResult.definition.consumer} does not match approval.`
        }
      ]);
    }

    const claim = await options.dependencies.stateStore.claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt
    });

    if (claim.status === "already-completed") {
      return {
        action: "ack",
        reason: "duplicate",
        envelope
      };
    }

    if (claim.status === "in-progress") {
      return retryOrDlq(envelope, "idempotency-in-progress", 1_000);
    }

    const context: RuntimeMessageContext = {
      envelope,
      payload: payloadResult.value,
      stage: "approval",
      receivedAt
    };

    try {
      const result = await options.handler(context);

      if (result.status === "ok") {
        await markCompleted(options.dependencies.stateStore, envelope, options.dependencies.clock);
        return {
          action: "ack",
          reason: "handled",
          envelope
        };
      }

      if (result.status === "retry") {
        await markFailed(options.dependencies.stateStore, envelope, result.reason, true, options.dependencies.clock);
        return retryOrDlq(envelope, result.reason, result.retryAfterMs);
      }

      await markFailed(options.dependencies.stateStore, envelope, result.reason, false, options.dependencies.clock);
      return terminalResult(envelope, result.reason);
    } catch (error: unknown) {
      await markFailed(options.dependencies.stateStore, envelope, classifyHandlerError(error), true, options.dependencies.clock);
      return retryOrDlq(envelope, "handler-error");
    }
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

function classifyHandlerError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-handler-error";
}
