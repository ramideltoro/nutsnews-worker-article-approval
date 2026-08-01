import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  RUNTIME_ALLOWED_METRIC_LABELS,
  RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS,
  createBufferedRuntimeTelemetrySink,
  type RuntimeMessageDelivery,
  type RuntimeTelemetryEvent
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadApprovalConfig } from "../src/config.js";
import {
  APPROVAL_STAGE_LATENCY_BUCKETS_SECONDS,
  createApprovalPrometheusTelemetrySink,
  type ApprovalPrometheusTelemetrySink
} from "../src/metrics.js";
import { createApprovalService } from "../src/service.js";
import { combineBestEffortTelemetrySinks } from "../src/telemetry.js";
import {
  LocalApprovalWorkHandler,
  LocalBrokerTransport,
  ManualApprovalClock,
  createLocalApprovalDependencies,
  createMinimalApprovalDelivery,
  createMinimalApprovalEnvelope,
  createMinimalApprovalPayload
} from "../src/test-doubles.js";

const COMPLETING_MESSAGE_EVENTS = new Set([
  "runtime.message.accepted",
  "runtime.message.duplicate",
  "runtime.message.invalid",
  "runtime.message.retry",
  "runtime.message.dlq"
]);

describe("approval lifecycle telemetry", () => {
  it("exports a deterministic bounded zero-valued stage family before traffic", async () => {
    const context = createTelemetryContext();
    const initial = context.metrics.collect();
    const initialStageSamples = canonicalStageSampleLines(initial);

    expect(context.metrics.collect()).toBe(initial);
    expect(initialStageSamples).toHaveLength(6 + APPROVAL_STAGE_LATENCY_BUCKETS_SECONDS.length + 3);
    expect(initialStageSamples.every((line) => line.endsWith(" 0"))).toBe(true);
    expect(approvalStageOutcomesFromMetrics(initial)).toEqual([
      "success",
      "duplicate",
      "invalid",
      "retry",
      "dlq",
      "failure"
    ]);
    expect(histogramBoundaries(initial)).toEqual([
      ...APPROVAL_STAGE_LATENCY_BUCKETS_SECONDS.map(String),
      "+Inf"
    ]);

    await context.metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "approval",
      queue: "nutsnews.worker.approval.v1",
      outcome: "success",
      durationMs: 25
    });

    const afterTraffic = context.metrics.collect();
    expect(canonicalStageSeriesKeys(afterTraffic)).toEqual(canonicalStageSeriesKeys(initial));
    expect(metricValue(afterTraffic, "nutsnews_worker_uplift_stage_events_total", "success")).toBe(1);
    expect(sampleValue(afterTraffic, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(1);
  });

  it("accepts an enrichment-stage payload contracted to the approval consumer", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    context.telemetry.clear();

    await expect(context.broker.deliverApproval(approvalDelivery(1))).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(context.workHandler.handled[0]?.payload).toMatchObject({
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.enrichmentResult,
      candidateId: "candidate-world-001"
    });
    expect(context.telemetry.events.filter((event) => event.name.startsWith("runtime.message.")).map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted"
    ]);

    await context.service.stop();
  });

  it("emits one completing lifecycle event for accepted, duplicate, invalid, retry, retry-exhausted, and terminal deliveries", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    context.telemetry.clear();
    await exerciseLifecycleOutcomes(context);

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate",
      "runtime.message.started",
      "runtime.message.invalid",
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);

    const started = messageEvents.filter((event) => event.name === "runtime.message.started");
    const completed = messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name));
    expect(started).toHaveLength(6);
    expect(completed).toHaveLength(started.length);
    expect(context.workHandler.handled).toHaveLength(4);

    expect(completed[0]).toMatchObject({
      name: "runtime.message.accepted",
      outcome: "success",
      messageId: messageId(1),
      idempotencyKey: idempotencyKey(1)
    });
    expect(completed[1]).toMatchObject({
      name: "runtime.message.duplicate",
      outcome: "duplicate",
      messageId: messageId(1),
      idempotencyKey: idempotencyKey(1)
    });
    expect(completed[2]).toMatchObject({
      name: "runtime.message.invalid",
      outcome: "failure",
      attributes: {
        issueCode: "payload-consumer-mismatch",
        issuePath: "$.schemaId"
      }
    });
    expect(completed[3]).toMatchObject({
      name: "runtime.message.retry",
      outcome: "retry",
      attributes: {
        reason: "transient-approval-error",
        destination: "nutsnews.worker.approval.v1.retry-30s"
      }
    });
    expect(completed[4]).toMatchObject({
      name: "runtime.message.dlq",
      outcome: "dlq",
      attributes: {
        reason: "retry-exhausted",
        destination: "nutsnews.worker.approval.v1.dlq"
      }
    });
    expect(completed[5]).toMatchObject({
      name: "runtime.message.dlq",
      outcome: "dlq",
      attributes: {
        reason: "terminal-approval-error",
        destination: "nutsnews.worker.approval.v1.dlq"
      }
    });

    await context.service.stop();
  });

  it.each([
    {
      operation: "claim",
      action: "retry",
      completion: "runtime.message.retry",
      reason: "idempotency-claim-error",
      attemptCount: 1
    },
    {
      operation: "claim",
      action: "dlq",
      completion: "runtime.message.dlq",
      reason: "idempotency-claim-error",
      attemptCount: WORKER_DELIVERY_BEHAVIOR.maxAttempts
    },
    {
      operation: "markCompleted",
      action: "retry",
      completion: "runtime.message.retry",
      reason: "idempotency-mark-completed-error",
      attemptCount: 1
    },
    {
      operation: "markFailed",
      action: "retry",
      completion: "runtime.message.retry",
      reason: "idempotency-mark-failed-error",
      attemptCount: 1
    }
  ] as const)("contains $operation failure as one $action completion", async ({
    operation,
    action,
    completion,
    reason,
    attemptCount
  }) => {
    const context = createTelemetryContext();

    if (operation === "claim") {
      vi.spyOn(context.dependencies.stateStore, "claim").mockRejectedValue(new Error("claim unavailable"));
    } else if (operation === "markCompleted") {
      vi.spyOn(context.dependencies.stateStore, "markCompleted").mockRejectedValue(new Error("completion unavailable"));
    } else {
      context.workHandler.result = {
        status: "retry",
        reason: "transient-approval-error"
      };
      vi.spyOn(context.dependencies.stateStore, "markFailed").mockRejectedValue(new Error("failure state unavailable"));
    }

    await context.service.start();
    context.telemetry.clear();

    const delivery = approvalDelivery(9, {
      attempt: {
        count: attemptCount,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T00:00:00.000Z"
      }
    });
    await expect(context.broker.deliverApproval(delivery)).resolves.toMatchObject({
      action,
      reason
    });

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      completion
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(1);
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_inflight", {
      queue: "nutsnews.worker.approval.v1"
    })).toBe(0);
    expect(metricValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_events_total", action)).toBe(1);
    expect(context.workHandler.handled).toHaveLength(operation === "claim" ? 0 : 1);

    await context.service.stop();
  });

  it("counts every lifecycle outcome once with bounded labels and keeps identifiers out of Prometheus", async () => {
    const context = createTelemetryContext();

    const initialOutput = context.metrics.collect();
    expectHealthOneHot(initialOutput, "liveness", "ok");
    expectHealthOneHot(initialOutput, "startup", "unhealthy");
    expectHealthOneHot(initialOutput, "readiness", "unhealthy");

    await context.service.start();
    const startedOutput = context.metrics.collect();
    expectHealthOneHot(startedOutput, "liveness", "ok");
    expectHealthOneHot(startedOutput, "startup", "ok");
    expectHealthOneHot(startedOutput, "readiness", "ok");
    expect(startedOutput).not.toContain("nutsnews_worker_dependency_duration_ms");

    await exerciseLifecycleOutcomes(context);
    await context.service.health.liveness();
    await context.service.health.startup();
    await context.service.health.readiness();

    const output = context.metrics.collect();
    expect(metricValue(output, "nutsnews_worker_messages_total", "success")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_messages_total", "duplicate")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_messages_total", "failure")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_messages_total", "retry")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_messages_total", "dlq")).toBe(2);
    expect(metricValue(output, "nutsnews_worker_retries_total", "retry")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_dlq_total", "failure")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_dlq_total", "dlq")).toBe(2);
    expect(metricValue(output, "nutsnews_worker_processing_duration_ms_count", "success")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_processing_duration_ms_count", "duplicate")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_processing_duration_ms_count", "failure")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_processing_duration_ms_count", "retry")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_processing_duration_ms_count", "dlq")).toBe(2);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "success")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "duplicate")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "invalid")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "retry")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "dlq")).toBe(2);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "30"
    })).toBe(6);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "+Inf"
    })).toBe(6);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_sum")).toBe(1);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(6);
    expect(output).toContain('nutsnews_worker_uplift_stage_events_total{environment="test",service="approval",outcome="success"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="test",service="approval",le="30"} 6');
    expectHealthOneHot(output, "liveness", "ok");
    expectHealthOneHot(output, "startup", "ok");
    expectHealthOneHot(output, "readiness", "ok");
    expect(output).not.toContain("nutsnews_worker_dependency_duration_ms");
    expect(context.metrics.allowedLabels).toEqual(RUNTIME_ALLOWED_METRIC_LABELS);

    await context.metrics.emit({
      name: "runtime.message.dlq",
      level: "error",
      at: "2026-07-23T00:10:00.000Z",
      stage: "approval",
      queue: "nutsnews.worker.approval.v1",
      outcome: "dlq"
    });
    const withoutDuration = context.metrics.collect();
    expect(metricValue(withoutDuration, "nutsnews_worker_uplift_stage_events_total", "dlq")).toBe(3);
    expect(sampleValue(withoutDuration, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(6);

    for (const line of metricSampleLines(output)) {
      expect(metricLabelNames(line)).toEqual(expectedMetricLabelNames(line));
    }

    for (const forbidden of RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS) {
      expect(output).not.toContain(`${forbidden}=`);
    }

    for (const identifier of [
      messageId(1),
      idempotencyKey(1),
      "article-001",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b4601",
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    ]) {
      expect(output).not.toContain(identifier);
    }

    await context.service.consumer?.cancel();
    const cancelledOutput = context.metrics.collect();
    expectHealthOneHot(cancelledOutput, "readiness", "unhealthy");

    await context.service.stop();
    const stoppedOutput = context.metrics.collect();
    expectHealthOneHot(stoppedOutput, "liveness", "ok");
    expectHealthOneHot(stoppedOutput, "startup", "unhealthy");
    expectHealthOneHot(stoppedOutput, "readiness", "unhealthy");
  });

  it.each([
    "throw",
    "reject"
  ] as const)("keeps %s telemetry failures best-effort for every delivery outcome", async (failureMode) => {
    const config = loadApprovalConfig({
      HOSTNAME: `approval-${failureMode}-telemetry-test`,
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_APPROVAL_HTTP_PORT: "0",
      NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
    });
    const clock = new ManualApprovalClock();
    const dependencies = createLocalApprovalDependencies({
      clock
    });
    const events: RuntimeTelemetryEvent[] = [];
    const rejectingSink = rejectingMetricsSink(failureMode, events);
    const service = createApprovalService({
      config,
      dependencies,
      telemetry: rejectingSink,
      metrics: rejectingSink
    });
    const workHandler = dependencies.workHandler as LocalApprovalWorkHandler;
    workHandler.onHandleStart = () => {
      clock.advance(250);
    };
    const context = {
      broker: dependencies.brokerTransport as LocalBrokerTransport,
      service,
      workHandler
    };

    await expect(service.start()).resolves.toBeUndefined();
    events.length = 0;
    await exerciseLifecycleOutcomes(context);

    const messageEvents = events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate",
      "runtime.message.started",
      "runtime.message.invalid",
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(6);
    expect(workHandler.handled).toHaveLength(4);
    await expect(service.health.readiness()).resolves.toMatchObject({
      status: "ok"
    });
    await expect(service.stop()).resolves.toBeUndefined();
  });

  it.each([
    "throw",
    "reject"
  ] as const)("fans out independently when one construction-time sink %ss", async (failureMode) => {
    const delivered: RuntimeTelemetryEvent[] = [];
    const combined = combineBestEffortTelemetrySinks(
      rejectingTelemetrySink(failureMode, []),
      {
        emit: (event) => {
          delivered.push(event);
        }
      }
    );
    const event = lifecycleEvent();

    await expect(combined?.emit(event)).resolves.toBeUndefined();
    expect(delivered).toEqual([
      event
    ]);
  });

  it("omits duration-less dependency samples but retains measured dependency latency", async () => {
    const metrics = createApprovalPrometheusTelemetrySink({
      identity: {
        service: "approval",
        version: "0.1.0",
        environment: "test",
        host: "approval-test"
      }
    });
    const base = {
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "approval",
      queue: "nutsnews.worker.approval.v1",
      outcome: "success"
    } as const satisfies RuntimeTelemetryEvent;

    await metrics.emit(base);
    expect(metrics.collect()).not.toContain("nutsnews_worker_dependency_duration_ms");

    await metrics.emit({
      ...base,
      durationMs: 37
    });
    const output = metrics.collect();
    expect(sampleValue(output, "nutsnews_worker_dependency_duration_ms_count", {
      queue: "nutsnews.worker.approval.v1",
      outcome: "success"
    })).toBe(1);
    expect(sampleValue(output, "nutsnews_worker_dependency_duration_ms_sum", {
      queue: "nutsnews.worker.approval.v1",
      outcome: "success"
    })).toBe(37);
  });
});

function rejectingMetricsSink(
  failureMode: "throw" | "reject",
  events: RuntimeTelemetryEvent[]
): ApprovalPrometheusTelemetrySink {
  const telemetry = rejectingTelemetrySink(failureMode, events);
  const fail = (): never => {
    throw new Error("metrics unavailable");
  };

  return {
    allowedLabels: RUNTIME_ALLOWED_METRIC_LABELS,
    emit: telemetry.emit,
    collect: fail,
    setInFlight: fail,
    setShutdownDraining: fail,
    setHealthProbe: fail
  };
}

function rejectingTelemetrySink(
  failureMode: "throw" | "reject",
  events: RuntimeTelemetryEvent[]
) {
  return {
    emit: (event: RuntimeTelemetryEvent): void | Promise<void> => {
      events.push(event);

      if (failureMode === "throw") {
        throw new Error("telemetry unavailable");
      }

      return Promise.reject(new Error("telemetry unavailable"));
    }
  };
}

function lifecycleEvent(): RuntimeTelemetryEvent {
  return {
    name: "runtime.message.started",
    level: "info",
    at: "2026-07-23T00:00:00.000Z",
    stage: "approval",
    queue: "nutsnews.worker.approval.v1",
    outcome: "started"
  };
}

function createTelemetryContext() {
  const config = loadApprovalConfig({
    HOSTNAME: "approval-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_APPROVAL_HTTP_PORT: "0",
    NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
  });
  const clock = new ManualApprovalClock();
  const dependencies = createLocalApprovalDependencies({
    clock
  });
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createApprovalPrometheusTelemetrySink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    }
  });
  const service = createApprovalService({
    config,
    dependencies,
    telemetry: {
      emit: async (event) => {
        await telemetry.emit(event);
        await metrics.emit(event);
      }
    },
    metrics
  });

  const workHandler = dependencies.workHandler as LocalApprovalWorkHandler;
  workHandler.onHandleStart = () => {
    clock.advance(250);
  };

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    clock,
    dependencies,
    metrics,
    service,
    telemetry,
    workHandler
  };
}

interface LifecycleContext {
  readonly broker: LocalBrokerTransport;
  readonly service: ReturnType<typeof createApprovalService>;
  readonly workHandler: LocalApprovalWorkHandler;
}

async function exerciseLifecycleOutcomes(context: LifecycleContext): Promise<void> {
  const accepted = approvalDelivery(1);
  await expect(context.broker.deliverApproval(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "handled"
  });
  await expect(context.broker.deliverApproval(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "duplicate"
  });

  await expect(context.broker.deliverApproval({
    ...approvalDelivery(2),
    payload: translationTaskPayload()
  })).resolves.toMatchObject({
    action: "dlq",
    reason: "payload-consumer-mismatch"
  });

  context.workHandler.result = {
    status: "retry",
    reason: "transient-approval-error",
    retryAfterMs: 2_000
  };
  await expect(context.broker.deliverApproval(approvalDelivery(3))).resolves.toMatchObject({
    action: "retry",
    reason: "transient-approval-error"
  });

  context.workHandler.result = {
    status: "retry",
    reason: "retry-exhausted"
  };
  await expect(context.broker.deliverApproval(approvalDelivery(4, {
    attempt: {
      count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T00:00:00.000Z",
      lastAttemptAt: "2026-07-23T00:05:00.000Z"
    }
  }))).resolves.toMatchObject({
    action: "dlq",
    reason: "retry-exhausted"
  });

  context.workHandler.result = {
    status: "terminal-failure",
    reason: "terminal-approval-error"
  };
  await expect(context.broker.deliverApproval(approvalDelivery(5))).resolves.toMatchObject({
    action: "dlq",
    reason: "terminal-approval-error"
  });
}

function approvalDelivery(
  sequence: number,
  envelopeOverrides: Partial<WorkerMessageEnvelope> = {}
): RuntimeMessageDelivery {
  return {
    ...createMinimalApprovalDelivery(),
    envelope: createMinimalApprovalEnvelope({
      messageId: messageId(sequence),
      idempotencyKey: idempotencyKey(sequence),
      ...envelopeOverrides
    }),
    payload: createMinimalApprovalPayload({
      idempotencyKey: idempotencyKey(sequence)
    })
  };
}

function messageId(sequence: number): string {
  return `018f1598-2dd5-7c4f-9f92-8f7a7f8b48${String(sequence).padStart(2, "0")}`;
}

function idempotencyKey(sequence: number): string {
  return `enrichment:approval:telemetry-${String(sequence)}:fingerprint001`;
}

function translationTaskPayload(): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4701",
    idempotencyKey: idempotencyKey(2),
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: "2026-07-23T00:00:00.000Z",
    articleId: "article-001",
    sourceLanguage: "en",
    targetLanguages: [
      "fr"
    ],
    reason: "new_article",
    existingLanguageCodes: []
  };
}

function metricValue(output: string, metric: string, outcome: string): number {
  return sampleValue(output, metric, {
    outcome
  });
}

function sampleValue(
  output: string,
  metric: string,
  requiredLabels: Readonly<Record<string, string>> = {}
): number {
  const matches = output
    .split("\n")
    .filter((line) => line.startsWith(`${metric}{`) && Object.entries(requiredLabels).every(([name, value]) => line.includes(`${name}="${value}"`)));

  expect(matches).toHaveLength(1);
  const value = matches[0]?.split(" ").at(-1);

  return Number(value);
}

function metricSampleLines(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_") && line.includes("{"));
}

function canonicalStageSampleLines(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_events_total{")
      || line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_"));
}

function canonicalStageSeriesKeys(output: string): readonly string[] {
  return canonicalStageSampleLines(output).map((line) => line.slice(0, line.lastIndexOf(" ")));
}

function approvalStageOutcomesFromMetrics(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_events_total{"))
    .map((line) => /outcome="([^"]+)"/u.exec(line)?.[1] ?? "");
}

function histogramBoundaries(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_bucket{"))
    .map((line) => /le="([^"]+)"/u.exec(line)?.[1] ?? "");
}

function expectHealthOneHot(
  output: string,
  probe: "liveness" | "startup" | "readiness",
  expected: "ok" | "degraded" | "unhealthy"
): void {
  for (const outcome of [
    "ok",
    "degraded",
    "unhealthy"
  ] as const) {
    expect(sampleValue(output, "nutsnews_worker_health_probe", {
      probe,
      outcome
    })).toBe(outcome === expected ? 1 : 0);
  }
}

function metricLabelNames(line: string): readonly string[] {
  const start = line.indexOf("{");
  const end = line.indexOf("}", start);

  return line
    .slice(start + 1, end)
    .split(",")
    .map((label) => label.slice(0, label.indexOf("=")));
}

function expectedMetricLabelNames(line: string): readonly string[] {
  if (line.startsWith("nutsnews_worker_build_info{")) {
    return [
      "environment",
      "service",
      "version",
      "revision"
    ];
  }

  if (line.startsWith("nutsnews_worker_deployment_info{")) {
    return [
      "environment",
      "service",
      "deployment",
      "adapter"
    ];
  }

  if (line.startsWith("nutsnews_worker_expected_active{")) {
    return [
      "environment",
      "service"
    ];
  }

  if (line.startsWith("nutsnews_worker_uplift_stage_events_total{")) {
    return [
      "environment",
      "service",
      "outcome"
    ];
  }

  if (line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_bucket{")) {
    return [
      "environment",
      "service",
      "le"
    ];
  }

  if (line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_")) {
    return [
      "environment",
      "service"
    ];
  }

  if (line.startsWith("nutsnews_worker_health_probe{")) {
    return [
      "environment",
      "service",
      "probe",
      "outcome"
    ];
  }

  return [
    ...RUNTIME_ALLOWED_METRIC_LABELS
  ];
}
