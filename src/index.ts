import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadApprovalConfig,
  type ApprovalConfig
} from "./config.js";
import type { ApprovalDependencies } from "./dependencies.js";
import { createArticleApprovalWorkHandler } from "./approval.js";
import { createApprovalHttpServer } from "./http.js";
import { createApprovalPrometheusTelemetrySink } from "./metrics.js";
import { createProductionApprovalDependencies } from "./production.js";
import type { ApprovalReconciler } from "./reconciliation.js";
import { createApprovalService } from "./service.js";
import {
  bestEffortTelemetryFlusher,
  combineBestEffortTelemetrySinks
} from "./telemetry.js";
import { createLocalApprovalDependencies } from "./test-doubles.js";

export {
  InMemoryApprovalQwenCapacityLimiter,
  type ApprovalQwenCapacityLimiter,
  type ApprovalQwenCapacityLimiterOptions,
  type ApprovalQwenCapacityPermit
} from "./backpressure.js";
export {
  APPROVAL_CONFIG_SCHEMA,
  APPROVAL_SERVICE_NAME,
  APPROVAL_SERVICE_VERSION,
  ApprovalConfigError,
  loadApprovalConfig,
  type ApprovalConfig
} from "./config.js";
export type {
  ApprovalBrokerOutbox,
  ApprovalDatabaseTransaction,
  ApprovalDatabaseTransactionRunner,
  ApprovalDependencies,
  ApprovalDependencyProbe,
  ApprovalDecisionKey,
  ApprovalEnrichmentRecord,
  ApprovalEnrichmentRecordInput,
  ApprovalMetadataReference,
  ApprovalPrompt,
  ApprovalPromptRegistry,
  ApprovalQwenClient,
  ApprovalQwenRequest,
  ApprovalStateStore,
  ApprovalStoredDecision,
  ApprovalTranslationPublication,
  ApprovalWorkHandler,
  ApprovalWorkTools
} from "./dependencies.js";
export {
  ApprovalQwenError
} from "./dependencies.js";
export {
  createArticleApprovalWorkHandler,
  type ArticleApprovalWorkHandlerOptions
} from "./approval.js";
export {
  createApprovalHttpServer,
  type ApprovalHttpServer
} from "./http.js";
export {
  APPROVAL_RECONCILIATION_CONFIRMATION,
  APPROVAL_RECONCILIATION_PATH,
  type ApprovalReconciliationCandidate,
  type ApprovalReconciliationReport,
  type ApprovalReconciliationRequest,
  type ApprovalReconciler
} from "./reconciliation.js";
export {
  LocalAiApprovalQwenClient,
  PayloadRabbitMqTransport,
  PostgresApprovalBrokerOutbox,
  PostgresApprovalOutboxReconciler,
  PostgresApprovalStateStore,
  PostgresApprovalTransactionRunner,
  StaticApprovalPromptRegistry,
  createProductionApprovalDependencies,
  type ProductionApprovalDependencies
} from "./production.js";
export {
  APPROVAL_STAGE_LATENCY_BUCKETS_SECONDS,
  createApprovalPrometheusTelemetrySink,
  type ApprovalHealthOutcome,
  type ApprovalHealthProbe,
  type ApprovalPrometheusTelemetrySink,
  type ApprovalRuntimeMetricsSink,
  type ApprovalStageOutcome
} from "./metrics.js";
export {
  createApprovalService,
  type ApprovalService
} from "./service.js";
export {
  InMemoryApprovalStateStore,
  LocalApprovalBrokerOutbox,
  LocalApprovalPromptRegistry,
  LocalApprovalQwenClient,
  LocalApprovalTransactionRunner,
  LocalApprovalWorkHandler,
  LocalBrokerTransport,
  ManualApprovalClock,
  createLocalApprovalDependencies,
  createMinimalApprovalDelivery,
  createMinimalApprovalEnvelope,
  createMinimalApprovalPayload
} from "./test-doubles.js";

export interface ApprovalApplication {
  readonly config: ApprovalConfig;
  start(): Promise<void>;
  stop(): Promise<void>;
  url(path?: string): string;
}

export interface ApprovalApplicationOptions {
  readonly dependencies?: ApprovalDependencies;
}

export function createApprovalApplication(
  config = loadApprovalConfig(),
  options: ApprovalApplicationOptions = {}
): ApprovalApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host,
    revision: config.buildRevision,
    deployment: config.dependencyMode === "production"
      ? "shadow"
      : config.environment === "test" ? "test" : "local",
    adapter: config.dependencyMode === "production" ? "production" : "in_memory"
  } as const;
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createApprovalPrometheusTelemetrySink({
        identity
      })
    : undefined;
  const telemetry = combineBestEffortTelemetrySinks(logSink, metrics);
  const telemetryFlusher = bestEffortTelemetryFlusher(logSink);
  const baseDependencies = options.dependencies ?? (config.dependencyMode === "production"
    ? createProductionApprovalDependencies({
        config,
        clock: SYSTEM_RUNTIME_CLOCK,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      })
    : createLocalApprovalDependencies({
        clock: SYSTEM_RUNTIME_CLOCK
      }));
  const dependencies = options.dependencies ?? {
    ...baseDependencies,
    workHandler: createArticleApprovalWorkHandler({
      config,
      dependencies: baseDependencies,
      ...(telemetry === undefined ? {} : {
        telemetry
      })
    })
  };
  const service = createApprovalService({
    config,
    dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const httpServer = createApprovalHttpServer({
    config,
    service,
    ...(hasReconciler(baseDependencies) ? {
      reconciler: baseDependencies.reconciler
    } : {}),
    ...(hasReconciliationToken(baseDependencies) ? {
      reconciliationToken: baseDependencies.reconciliationToken
    } : {}),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  let startPromise: Promise<void> | undefined;
  let listenerBound = false;
  let started = false;
  let stopped = false;
  let stopRequested = false;
  let dependenciesClosed = false;
  const isStopRequested = (): boolean => stopRequested;
  const closeListener = async (): Promise<void> => {
    if (!listenerBound) {
      return;
    }

    try {
      await httpServer.close();
    } finally {
      listenerBound = false;
    }
  };
  const closeDependencies = async (): Promise<void> => {
    if (dependenciesClosed || !hasDependencyCloser(baseDependencies)) {
      return;
    }

    dependenciesClosed = true;
    await baseDependencies.close();
  };
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        stopRequested = true;
        await closeListener();
      },
      async () => {
        await service.stop();
      },
      closeDependencies
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(telemetryFlusher === undefined ? {} : {
      telemetryFlusher
    })
  });

  return {
    config,
    async start(): Promise<void> {
      if (started) {
        return;
      }

      if (stopped) {
        throw new Error("Approval application cannot be restarted after shutdown.");
      }

      if (startPromise !== undefined) {
        await startPromise;
        return;
      }

      const operation = (async () => {
        assertPackageCompatibility();

        try {
          await httpServer.listen();
          listenerBound = true;
          shutdown.start();

          if (isStopRequested()) {
            await shutdown.trigger("manual");
            throw new Error("Approval application startup was interrupted by shutdown.");
          }

          await service.start();

          if (isStopRequested()) {
            await cleanupBestEffort(() => service.stop());
            throw new Error("Approval application startup was interrupted by shutdown.");
          }

          started = true;
        } catch (error: unknown) {
          shutdown.stop();
          await cleanupBestEffort(closeListener);
          await cleanupBestEffort(() => service.stop());
          await cleanupBestEffort(closeDependencies);
          stopped = true;

          throw error;
        }
      })();

      startPromise = operation;

      try {
        await operation;
      } finally {
        startPromise = undefined;
      }
    },
    async stop(): Promise<void> {
      if (stopped || (!started && startPromise === undefined)) {
        return;
      }

      stopRequested = true;

      try {
        if (shutdown.isStarted) {
          await shutdown.trigger("manual");
        }
      } finally {
        started = false;
        stopped = true;
        startPromise = undefined;
      }
    },
    url: (path) => httpServer.url(path)
  };
}

async function cleanupBestEffort(operation: () => Promise<void>): Promise<void> {
  try {
    await operation();
  } catch {
    // Startup cleanup cannot replace the original startup failure.
  }
}

function hasDependencyCloser(
  dependencies: ApprovalDependencies
): dependencies is ApprovalDependencies & { readonly close: () => Promise<void> } {
  const candidate = dependencies as Partial<{ readonly close: unknown }>;

  return typeof candidate.close === "function";
}

function hasReconciler(
  dependencies: ApprovalDependencies
): dependencies is ApprovalDependencies & { readonly reconciler: ApprovalReconciler } {
  const candidate = dependencies as Partial<{ readonly reconciler: unknown }>;

  return typeof candidate.reconciler === "object" && candidate.reconciler !== null;
}

function hasReconciliationToken(
  dependencies: ApprovalDependencies
): dependencies is ApprovalDependencies & { readonly reconciliationToken: string } {
  const candidate = dependencies as Partial<{ readonly reconciliationToken: unknown }>;

  return typeof candidate.reconciliationToken === "string" && candidate.reconciliationToken.length > 0;
}

export const SUPPORTED_RUNTIME_PACKAGE_VERSION = "0.5.0";

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.4.0") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== SUPPORTED_RUNTIME_PACKAGE_VERSION) {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createApprovalApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start approval");
    process.exitCode = 1;
  });
}
