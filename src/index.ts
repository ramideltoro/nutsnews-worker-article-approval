import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadApprovalConfig,
  type ApprovalConfig
} from "./config.js";
import type { ApprovalDependencies } from "./dependencies.js";
import { createArticleApprovalWorkHandler } from "./approval.js";
import { createApprovalHttpServer } from "./http.js";
import { createProductionApprovalDependencies } from "./production.js";
import type { ApprovalReconciler } from "./reconciliation.js";
import { createApprovalService } from "./service.js";
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
}

export function createApprovalApplication(config = loadApprovalConfig()): ApprovalApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host
  };
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createPrometheusRuntimeTelemetrySink({
        identity
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const baseDependencies = config.dependencyMode === "production"
    ? createProductionApprovalDependencies({
        config,
        clock: SYSTEM_RUNTIME_CLOCK,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      })
    : createLocalApprovalDependencies({
        clock: SYSTEM_RUNTIME_CLOCK
      });
  const dependencies = {
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
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        await httpServer.close();
      },
      async () => {
        await service.stop();
      },
      async () => {
        if (hasDependencyCloser(baseDependencies)) {
          await baseDependencies.close();
        }
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: logSink
    })
  });

  return {
    config,
    async start(): Promise<void> {
      assertPackageCompatibility();
      await service.start();
      await httpServer.listen();
      shutdown.start();
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
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

function combineTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        await sink.emit(event);
      }
    }
  };
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
