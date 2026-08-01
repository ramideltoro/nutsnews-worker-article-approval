import {
  getWorkerRoute
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createBrokerConsumerReadinessCheck,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  createRuntimeMessageProcessor,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type RuntimeHealthCheck,
  type RuntimeHealthReport,
  type RuntimeHealthProbeSet,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetrySink
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
  const processor = createRuntimeMessageProcessor({
    stage: "approval",
    idempotencyStore: options.dependencies.stateStore,
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    handler: async (context) => {
      const ownership = options.dependencies.stateStore.ownership(context.envelope.idempotencyKey);

      try {
        return await drain.track(async () => {
          setInFlight(options.metrics, approvalRoute.mainQueue.name, drain.inFlight);
          ownership.assertOwned();
          const result = await options.dependencies.workHandler.handle(context, {
            signal: ownership.signal,
            assertOwnership: () => ownership.assertOwned(),
            publish: async (command) => {
              ownership.assertOwned();
              const receipt = await options.dependencies.brokerTransport.publishOwned(command, ownership.signal);
              ownership.assertOwned();
              return receipt;
            },
            recordOutbox: async (command, receipt) => {
              ownership.assertOwned();
              await options.dependencies.brokerOutbox.record(command, receipt, ownership.signal);
              ownership.assertOwned();
            },
            withTransaction: async (operation) => {
              ownership.assertOwned();
              const value = await options.dependencies.transactionRunner.withTransaction(operation, ownership.signal);
              ownership.assertOwned();
              return value;
            }
          });
          ownership.assertOwned();

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
          productionDependencyModeCheck(options.config),
          dependencyAdapterModeCheck(options.config, options.dependencies.adapterMode),
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

      assertSafeDependencyComposition(options.config, options.dependencies.adapterMode);
      await broker.start();
      const brokerConsumer = await broker.consume("approval", processor);
      consumer = {
        stage: brokerConsumer.stage,
        cancel: async () => {
          await brokerConsumer.cancel();
          await refreshHealthProbeBestEffort(
            "readiness",
            () => service.health.readiness(),
            options.metrics
          );
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
      await refreshHealthMetricsBestEffort(service.health, options.metrics);
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      const cancelConsumer = consumer?.cancel();
      drain.stopAcceptingWork();
      setShutdownDraining(options.metrics, true);
      let drainFailure: Error | undefined;

      try {
        await cancelConsumer;
        try {
          await drain.waitForDrain(options.config.shutdownTimeoutMs);
        } catch (error: unknown) {
          drainFailure = error instanceof Error
            ? error
            : new Error("Approval service failed while draining in-flight work.");
          await options.dependencies.stateStore.abortActiveClaims(
            "Approval service shutdown exceeded the in-flight drain deadline."
          );
        }

        await broker.stop("shutdown");
      } finally {
        setShutdownDraining(options.metrics, false);
        setInFlight(options.metrics, approvalRoute.mainQueue.name, drain.inFlight);
        setHealthProbe(options.metrics, "startup", "unhealthy");
        setHealthProbe(options.metrics, "readiness", "unhealthy");
        consumer = undefined;
        started = false;
        await refreshHealthMetricsBestEffort(service.health, options.metrics);
      }

      if (drainFailure !== undefined) {
        throw drainFailure;
      }
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

async function refreshHealthMetricsBestEffort(
  probes: RuntimeHealthProbeSet,
  metrics: ApprovalRuntimeMetricsSink | undefined
): Promise<void> {
  await Promise.all([
    refreshHealthProbeBestEffort("liveness", () => probes.liveness(), metrics),
    refreshHealthProbeBestEffort("startup", () => probes.startup(), metrics),
    refreshHealthProbeBestEffort("readiness", () => probes.readiness(), metrics)
  ]);
}

async function refreshHealthProbeBestEffort(
  probe: "liveness" | "startup" | "readiness",
  operation: () => Promise<RuntimeHealthReport>,
  metrics: ApprovalRuntimeMetricsSink | undefined
): Promise<void> {
  try {
    await operation();
  } catch {
    setHealthProbe(metrics, probe, "unhealthy");
  }
}

function isApprovalMetrics(
  metrics: ApprovalRuntimeMetricsSink | undefined
): metrics is ApprovalPrometheusTelemetrySink {
  return metrics !== undefined
    && "setHealthProbe" in metrics
    && typeof metrics.setHealthProbe === "function";
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

function productionDependencyModeCheck(config: ApprovalConfig): RuntimeHealthCheck {
  return {
    name: "production-dependency-mode",
    critical: true,
    check: () => normalizedMode(config.environment) !== "production"
      || normalizedMode(config.dependencyMode) === "production"
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            environment: config.environment,
            dependencyMode: config.dependencyMode
          }
        }
  };
}

function dependencyAdapterModeCheck(
  config: ApprovalConfig,
  adapterMode: ApprovalDependencies["adapterMode"]
): RuntimeHealthCheck {
  return {
    name: "dependency-adapter-mode",
    critical: true,
    check: () => normalizedMode(config.dependencyMode) !== "production"
      || normalizedMode(adapterMode) === "production"
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            dependencyMode: config.dependencyMode,
            adapterMode
          }
        }
  };
}

function assertSafeDependencyComposition(
  config: ApprovalConfig,
  adapterMode: ApprovalDependencies["adapterMode"]
): void {
  const environment = normalizedMode(config.environment);
  const dependencyMode = normalizedMode(config.dependencyMode);
  const actualAdapterMode = normalizedMode(adapterMode);

  if (!config.shadowMode) {
    throw new Error("Approval startup refused: this worker must remain in shadow mode.");
  }

  if (environment === "production" && dependencyMode !== "production") {
    throw new Error("Approval startup refused: production environment requires production dependency mode.");
  }

  if (dependencyMode === "production" && actualAdapterMode !== "production") {
    throw new Error("Approval startup refused: production dependency mode requires production adapters.");
  }
}

function normalizedMode(value: string): string {
  return value.trim().toLowerCase();
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
