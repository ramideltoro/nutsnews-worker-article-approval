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
import { createApprovalHttpServer } from "./http.js";
import { createApprovalService } from "./service.js";
import { createLocalApprovalDependencies } from "./test-doubles.js";

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
  ApprovalPrompt,
  ApprovalPromptRegistry,
  ApprovalQwenClient,
  ApprovalStateStore,
  ApprovalWorkHandler,
  ApprovalWorkTools
} from "./dependencies.js";
export {
  createApprovalHttpServer,
  type ApprovalHttpServer
} from "./http.js";
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
  const dependencies = createLocalApprovalDependencies({
    clock: SYSTEM_RUNTIME_CLOCK
  });
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

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.4.0") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== "0.4.0") {
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
