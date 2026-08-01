import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeServiceIdentity,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export const APPROVAL_STAGE_LATENCY_BUCKETS_SECONDS = [
  0.01,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  30,
  60,
  120,
  300
] as const;

export type ApprovalStageOutcome = "success" | "duplicate" | "invalid" | "retry" | "dlq" | "failure";
export type ApprovalHealthProbe = "liveness" | "startup" | "readiness";
export type ApprovalHealthOutcome = "ok" | "degraded" | "unhealthy";

export interface ApprovalMetricIdentity extends RuntimeServiceIdentity {
  readonly revision?: string;
  readonly deployment?: "local" | "test" | "shadow" | "production" | "unknown";
  readonly adapter?: "in_memory" | "mixed" | "production" | "unknown";
}

export interface ApprovalPrometheusTelemetrySinkOptions extends Omit<PrometheusRuntimeTelemetrySinkOptions, "identity"> {
  readonly identity: ApprovalMetricIdentity;
}

export interface ApprovalRuntimeMetricsSink extends RuntimeTelemetrySink {
  readonly allowedLabels: readonly string[];
  collect(): string;
  setInFlight(queue: string, value: number): void;
  setShutdownDraining(draining: boolean): void;
}

export interface ApprovalPrometheusTelemetrySink extends ApprovalRuntimeMetricsSink {
  setHealthProbe(probe: ApprovalHealthProbe, outcome: ApprovalHealthOutcome): void;
}

const APPROVAL_STAGE_SERVICE = "approval";
const APPROVAL_MAIN_QUEUE = "nutsnews.worker.approval.v1";
const APPROVAL_STAGE_OUTCOMES = [
  "success",
  "duplicate",
  "invalid",
  "retry",
  "dlq",
  "failure"
] as const satisfies readonly ApprovalStageOutcome[];
const HEALTH_PROBES = [
  "liveness",
  "startup",
  "readiness"
] as const satisfies readonly ApprovalHealthProbe[];
const HEALTH_OUTCOMES = [
  "ok",
  "degraded",
  "unhealthy"
] as const satisfies readonly ApprovalHealthOutcome[];
const MAX_LABEL_LENGTH = 96;

export function createApprovalPrometheusTelemetrySink(
  options: ApprovalPrometheusTelemetrySinkOptions
): ApprovalPrometheusTelemetrySink {
  const runtime = createPrometheusRuntimeTelemetrySink(options);
  const environment = metricLabelValue(options.identity.environment);
  const counters = new Map<ApprovalStageOutcome, number>(
    APPROVAL_STAGE_OUTCOMES.map((outcome) => [
      outcome,
      0
    ] as const)
  );
  const bucketCounts = APPROVAL_STAGE_LATENCY_BUCKETS_SECONDS.map(() => 0);
  const health = new Map<ApprovalHealthProbe, ApprovalHealthOutcome>([
    [
      "liveness",
      "ok"
    ],
    [
      "startup",
      "unhealthy"
    ],
    [
      "readiness",
      "unhealthy"
    ]
  ]);
  let latencyCount = 0;
  let latencySum = 0;

  return {
    allowedLabels: runtime.allowedLabels,
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      if (shouldForwardToRuntime(event)) {
        await runtime.emit(event);
      }
      recordHealthEvent(health, event);
      recordConsumerReadinessEvent(health, event);
      const outcome = approvalStageOutcome(event);

      if (outcome === undefined) {
        return;
      }

      counters.set(outcome, (counters.get(outcome) ?? 0) + 1);
      const durationSeconds = durationSecondsFrom(event.durationMs);

      if (durationSeconds === undefined) {
        return;
      }

      latencyCount += 1;
      latencySum += durationSeconds;

      for (const [index, boundary] of APPROVAL_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
        if (durationSeconds <= boundary) {
          bucketCounts[index] = (bucketCounts[index] ?? 0) + 1;
        }
      }
    },
    collect(): string {
      const runtimeOutput = runtime.collect().trimEnd();
      const identityOutput = collectCompatibilityIdentityMetrics(options, runtimeOutput);
      const approvalOutput = collectApprovalStageMetrics(environment, counters, bucketCounts, latencyCount, latencySum);
      const ownershipOutput = collectExpectedActiveMetric(environment);
      const healthOutput = collectHealthProbeMetrics(environment, health);

      return `${[
        runtimeOutput,
        identityOutput,
        ownershipOutput,
        healthOutput,
        approvalOutput
      ].filter((output) => output.length > 0).join("\n")}\n`;
    },
    setInFlight(queue, value): void {
      runtime.setInFlight(queue, value);
    },
    setShutdownDraining(draining): void {
      runtime.setShutdownDraining(draining);
    },
    setHealthProbe(probe, outcome): void {
      health.set(probe, outcome);
    }
  };
}

function collectCompatibilityIdentityMetrics(
  options: ApprovalPrometheusTelemetrySinkOptions,
  runtimeOutput: string
): string {
  const identity = options.identity;
  const environment = metricLabelValue(identity.environment);
  const service = metricLabelValue(identity.service);
  const lines: string[] = [];

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_build_info")) {
    lines.push(
      "# HELP nutsnews_worker_build_info Immutable worker build identity.",
      "# TYPE nutsnews_worker_build_info gauge",
      `nutsnews_worker_build_info${labels({
        environment,
        service,
        version: metricLabelValue(identity.version),
        revision: metricLabelValue(identity.revision ?? "unknown")
      })} 1`
    );
  }

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_deployment_info")) {
    lines.push(
      "# HELP nutsnews_worker_deployment_info Worker deployment ownership and dependency adapter identity.",
      "# TYPE nutsnews_worker_deployment_info gauge",
      `nutsnews_worker_deployment_info${labels({
        environment,
        service,
        deployment: metricLabelValue(identity.deployment ?? "unknown"),
        adapter: metricLabelValue(identity.adapter ?? "unknown")
      })} 1`
    );
  }

  return lines.join("\n");
}

function hasMetricFamily(output: string, metric: string): boolean {
  return output.split("\n").some((line) => line.startsWith(`# HELP ${metric} `)
    || line.startsWith(`${metric}{`)
    || line.startsWith(`${metric} `));
}

function shouldForwardToRuntime(event: RuntimeTelemetryEvent): boolean {
  if (event.name === "runtime.health.evaluated") {
    return false;
  }

  if (event.name !== "runtime.dependency.observed") {
    return true;
  }

  const attributeDuration = event.attributes?.durationMs;

  return (event.durationMs !== undefined && Number.isFinite(event.durationMs))
    || (typeof attributeDuration === "number" && Number.isFinite(attributeDuration));
}

function recordHealthEvent(
  health: Map<ApprovalHealthProbe, ApprovalHealthOutcome>,
  event: RuntimeTelemetryEvent
): void {
  if (event.name !== "runtime.health.evaluated") {
    return;
  }

  const probe = event.attributes?.probe;
  const outcome = event.outcome;

  if (isHealthProbe(probe) && isHealthOutcome(outcome)) {
    health.set(probe, outcome);
  }
}

function recordConsumerReadinessEvent(
  health: Map<ApprovalHealthProbe, ApprovalHealthOutcome>,
  event: RuntimeTelemetryEvent
): void {
  if (event.name === "runtime.broker.consumer_state_changed"
    && event.stage === "approval"
    && event.queue === APPROVAL_MAIN_QUEUE
    && event.outcome !== "active") {
    health.set("readiness", "unhealthy");
  }
}

function collectExpectedActiveMetric(environment: string): string {
  return [
    "# HELP nutsnews_worker_expected_active Whether this worker deployment is expected to own active production work.",
    "# TYPE nutsnews_worker_expected_active gauge",
    `nutsnews_worker_expected_active${labels({
      environment,
      service: APPROVAL_STAGE_SERVICE
    })} 0`
  ].join("\n");
}

function collectHealthProbeMetrics(
  environment: string,
  health: ReadonlyMap<ApprovalHealthProbe, ApprovalHealthOutcome>
): string {
  const lines = [
    "# HELP nutsnews_worker_health_probe Worker health status by distinct bounded probe and outcome.",
    "# TYPE nutsnews_worker_health_probe gauge"
  ];

  for (const probe of HEALTH_PROBES) {
    const current = health.get(probe);

    if (current === undefined) {
      continue;
    }

    for (const outcome of HEALTH_OUTCOMES) {
      lines.push(`nutsnews_worker_health_probe${labels({
        environment,
        service: APPROVAL_STAGE_SERVICE,
        probe,
        outcome
      })} ${outcome === current ? "1" : "0"}`);
    }
  }

  return lines.join("\n");
}

function approvalStageOutcome(event: RuntimeTelemetryEvent): ApprovalStageOutcome | undefined {
  if (event.stage !== "approval" || event.queue !== APPROVAL_MAIN_QUEUE) {
    return undefined;
  }

  switch (event.name) {
    case "runtime.message.accepted":
      return "success";
    case "runtime.message.duplicate":
      return "duplicate";
    case "runtime.message.invalid":
      return "invalid";
    case "runtime.message.retry":
      return "retry";
    case "runtime.message.dlq":
      return "dlq";
    case "runtime.message.started":
    case "runtime.broker.state_changed":
    case "runtime.broker.topology_asserted":
    case "runtime.broker.consumer_state_changed":
    case "runtime.dependency.observed":
    case "runtime.health.evaluated":
    case "runtime.shutdown.started":
    case "runtime.shutdown.completed":
    case "runtime.shutdown.failed":
      return undefined;
  }
}

function collectApprovalStageMetrics(
  environment: string,
  counters: ReadonlyMap<ApprovalStageOutcome, number>,
  bucketCounts: readonly number[],
  latencyCount: number,
  latencySum: number
): string {
  const eventLabels = (outcome: ApprovalStageOutcome): string => labels({
    environment,
    service: APPROVAL_STAGE_SERVICE,
    outcome
  });
  const histogramLabels = labels({
    environment,
    service: APPROVAL_STAGE_SERVICE
  });
  const lines = [
    "# HELP nutsnews_worker_uplift_stage_events_total Completed worker-uplift stage deliveries by bounded service and outcome.",
    "# TYPE nutsnews_worker_uplift_stage_events_total counter"
  ];

  for (const outcome of APPROVAL_STAGE_OUTCOMES) {
    lines.push(`nutsnews_worker_uplift_stage_events_total${eventLabels(outcome)} ${formatMetricNumber(counters.get(outcome) ?? 0)}`);
  }

  lines.push(
    "# HELP nutsnews_worker_uplift_stage_latency_seconds Worker-uplift stage completion latency in seconds.",
    "# TYPE nutsnews_worker_uplift_stage_latency_seconds histogram"
  );

  for (const [index, boundary] of APPROVAL_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
    lines.push(`nutsnews_worker_uplift_stage_latency_seconds_bucket${labels({
      environment,
      service: APPROVAL_STAGE_SERVICE,
      le: String(boundary)
    })} ${formatMetricNumber(bucketCounts[index] ?? 0)}`);
  }

  lines.push(
    `nutsnews_worker_uplift_stage_latency_seconds_bucket${labels({
      environment,
      service: APPROVAL_STAGE_SERVICE,
      le: "+Inf"
    })} ${formatMetricNumber(latencyCount)}`,
    `nutsnews_worker_uplift_stage_latency_seconds_sum${histogramLabels} ${formatMetricNumber(latencySum)}`,
    `nutsnews_worker_uplift_stage_latency_seconds_count${histogramLabels} ${formatMetricNumber(latencyCount)}`
  );

  return lines.join("\n");
}

function labels(values: Readonly<Record<string, string>>): string {
  return `{${Object.entries(values).map(([name, value]) => `${name}="${escapeLabelValue(value)}"`).join(",")}}`;
}

function metricLabelValue(value: string): string {
  const cleaned = value
    .trim()
    .replace(/[^A-Za-z0-9_.:-]+/gu, "_")
    .slice(0, MAX_LABEL_LENGTH);

  return cleaned.length > 0 ? cleaned : "unknown";
}

function escapeLabelValue(value: string): string {
  return value
    .replace(/\\/gu, "\\\\")
    .replace(/"/gu, "\\\"");
}

function formatMetricNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(6)));
}

function durationSecondsFrom(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) ? Math.max(0, value) / 1_000 : undefined;
}

function isHealthProbe(value: unknown): value is ApprovalHealthProbe {
  return typeof value === "string" && HEALTH_PROBES.some((probe) => probe === value);
}

function isHealthOutcome(value: unknown): value is ApprovalHealthOutcome {
  return typeof value === "string" && HEALTH_OUTCOMES.some((outcome) => outcome === value);
}
