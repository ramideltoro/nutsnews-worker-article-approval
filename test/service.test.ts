import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadApprovalConfig } from "../src/config.js";
import { createApprovalPrometheusTelemetrySink } from "../src/metrics.js";
import { createApprovalService } from "../src/service.js";
import {
  InMemoryApprovalStateStore,
  LocalApprovalBrokerOutbox,
  LocalApprovalPromptRegistry,
  LocalApprovalQwenClient,
  LocalApprovalTransactionRunner,
  LocalApprovalWorkHandler,
  LocalBrokerTransport,
  createLocalApprovalDependencies,
  createMinimalApprovalEnvelope,
  createMinimalApprovalDelivery
} from "../src/test-doubles.js";

describe("createApprovalService", () => {
  it("starts, becomes ready, registers approval and translation routes, and drains cleanly", async () => {
    const context = createServiceContext();

    await context.service.start();

    expect(context.service.isStarted).toBe(true);
    expect(context.service.consumer?.stage).toBe("approval");
    expect(context.broker.assertedRoutes.map((route) => route.stage)).toEqual([
      "approval",
      "translation"
    ]);
    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.startup()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("ok");
    expect(context.metrics.collect()).not.toContain("nutsnews_worker_dependency_duration_ms");

    await context.service.stop();

    expect(context.service.isStarted).toBe(false);
    expect(context.service.broker.state).toBe("closed");
    expect(context.telemetry.events.some((event) => event.name === "runtime.broker.state_changed")).toBe(true);
  });

  it("delegates a valid approval delivery and acks duplicate replays without business logic", async () => {
    const context = createServiceContext();
    const delivery = createMinimalApprovalDelivery();

    await context.service.start();

    await expect(context.broker.deliverApproval(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverApproval(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.workHandler.handled[0]?.payload).toMatchObject({
      candidateId: "candidate-world-001",
      imageStatus: "hydrated",
      articleMetadataRef: {
        canonicalArticleId: "article-001"
      }
    });

    await context.service.stop();
  });

  it("rejects payloads that are not consumed by the approval service", async () => {
    const context = createServiceContext();

    await context.service.start();

    await expect(context.broker.deliverApproval({
      envelope: createMinimalApprovalDelivery().envelope,
      payload: translationTaskPayload(),
      receivedAt: "2026-07-23T00:00:01.000Z"
    })).resolves.toMatchObject({
      action: "dlq",
      reason: "payload-consumer-mismatch"
    });

    expect(context.workHandler.handled).toHaveLength(0);

    await context.service.stop();
  });

  it("waits for an in-flight delivery during shutdown without wall-clock sleeps", async () => {
    const context = createServiceContext();
    const gate = deferred<undefined>();
    const started = deferred<undefined>();

    context.workHandler.handleGate = gate.promise;
    context.workHandler.onHandleStart = () => {
      started.resolve(undefined);
    };

    await context.service.start();
    const delivery = context.broker.deliverApproval();
    await started.promise;
    const stop = context.service.stop();

    expect(context.service.isDraining).toBe(true);
    expect(context.workHandler.handled).toHaveLength(0);

    gate.resolve(undefined);
    await expect(delivery).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await stop;

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.service.isStarted).toBe(false);
  });

  it("aborts an over-deadline delivery, closes the broker, and reports the drain timeout", async () => {
    vi.useFakeTimers();

    try {
      const context = createServiceContext({
        NUTSNEWS_APPROVAL_SHUTDOWN_TIMEOUT_MS: "1000"
      });
      const gate = deferred<undefined>();
      const started = deferred<undefined>();

      context.workHandler.handleGate = gate.promise;
      context.workHandler.onHandleStart = () => {
        started.resolve(undefined);
      };

      await context.service.start();
      const delivery = context.broker.deliverApproval();
      await started.promise;
      const stopping = context.service.stop();
      const stopped = expect(stopping).rejects.toThrow("Shutdown exceeded 1000ms");

      expect(context.service.isDraining).toBe(true);
      await vi.advanceTimersByTimeAsync(1_000);

      await expect(delivery).resolves.toMatchObject({
        action: "retry",
        reason: "handler-error"
      });
      await stopped;
      expect(context.workHandler.handled).toHaveLength(0);
      expect(context.service.isStarted).toBe(false);
      expect(context.service.broker.state).toBe("closed");
      expect(() => context.stateStore.ownership(createMinimalApprovalEnvelope().idempotencyKey)).toThrow();

      gate.resolve(undefined);
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps liveness independent from AI endpoint readiness", async () => {
    const context = createServiceContext();

    context.qwenClient.status = "unhealthy";
    await context.service.start();

    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("unhealthy");

    await context.service.stop();
  });
  it("reports readiness unhealthy when the main queue consumer is cancelled", async () => {
    const context = createServiceContext();

    await context.service.start();
    await context.service.consumer?.cancel();

    const readiness = await context.service.health.readiness();
    expect(readiness.status).toBe("unhealthy");
    const consumerCheck = readiness.checks.find((check) => check.name === "rabbitmq-consumer");
    expect(consumerCheck?.status).toBe("unhealthy");
    expect(consumerCheck?.details).toMatchObject({
      queue: "nutsnews.worker.approval.v1",
      activeConsumers: 0
    });

    await context.service.stop();
  });

  it("fails readiness for defensively injected production config using test dependencies", async () => {
    const localConfig = loadApprovalConfig({
      NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
    });
    const config = {
      ...localConfig,
      environment: "production"
    } as const;
    const dependencies = createLocalApprovalDependencies();
    const service = createApprovalService({
      config,
      dependencies
    });

    await expect(service.start()).rejects.toThrow(
      "Approval startup refused: production environment requires production dependency mode."
    );
    const readiness = await service.health.readiness();

    expect(readiness.status).toBe("unhealthy");
    expect(readiness.checks.find((check) => check.name === "production-dependency-mode")).toMatchObject({
      status: "unhealthy",
      details: {
        environment: "production",
        dependencyMode: "test"
      }
    });

    expect((dependencies.brokerTransport as LocalBrokerTransport).assertedRoutes).toHaveLength(0);
  });

  it("refuses a defensively injected shadow cutover before broker topology or consumption", async () => {
    const config = {
      ...loadApprovalConfig({
        NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
      }),
      shadowMode: false
    } as const;
    const dependencies = createLocalApprovalDependencies();
    const service = createApprovalService({
      config,
      dependencies
    });

    await expect(service.start()).rejects.toThrow(
      "Approval startup refused: this worker must remain in shadow mode."
    );
    const readiness = await service.health.readiness();

    expect(readiness.status).toBe("unhealthy");
    expect(readiness.checks.find((check) => check.name === "shadow-mode")).toMatchObject({
      status: "unhealthy",
      details: {
        reason: "shadow-mode-disabled"
      }
    });
    expect((dependencies.brokerTransport as LocalBrokerTransport).assertedRoutes).toHaveLength(0);
    expect(service.consumer).toBeUndefined();
  });

  it("fails readiness when production mode receives non-production adapters", async () => {
    const config = loadApprovalConfig({
      NUTSNEWS_ENVIRONMENT: "production",
      NUTSNEWS_APPROVAL_DEPENDENCY_MODE: "production",
      NUTSNEWS_APPROVAL_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_APPROVAL_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_APPROVAL_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_APPROVAL_QWEN_BASE_URL: "https://qwen.internal.invalid/v1",
      NUTSNEWS_APPROVAL_QWEN_API_KEY: "test-key",
      NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalApprovalDependencies();
    const service = createApprovalService({
      config,
      dependencies
    });

    await expect(service.start()).rejects.toThrow(
      "Approval startup refused: production dependency mode requires production adapters."
    );
    const readiness = await service.health.readiness();

    expect(readiness.status).toBe("unhealthy");
    expect(readiness.checks.find((check) => check.name === "dependency-adapter-mode")).toMatchObject({
      status: "unhealthy",
      details: {
        dependencyMode: "production",
        adapterMode: "in_memory"
      }
    });

    expect((dependencies.brokerTransport as LocalBrokerTransport).assertedRoutes).toHaveLength(0);
  });
});

function createServiceContext(env: NodeJS.ProcessEnv = {}) {
  const config = loadApprovalConfig({
    NUTSNEWS_APPROVAL_HTTP_PORT: "0",
    NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent",
    ...env
  });
  const dependencies = createLocalApprovalDependencies();
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
    telemetry,
    metrics
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    metrics,
    outbox: dependencies.brokerOutbox as LocalApprovalBrokerOutbox,
    promptRegistry: dependencies.promptRegistry as LocalApprovalPromptRegistry,
    qwenClient: dependencies.qwenClient as LocalApprovalQwenClient,
    service,
    stateStore: dependencies.stateStore as InMemoryApprovalStateStore,
    telemetry,
    transactionRunner: dependencies.transactionRunner as LocalApprovalTransactionRunner,
    workHandler: dependencies.workHandler as LocalApprovalWorkHandler
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject
  };
}

function translationTaskPayload(): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4701",
    idempotencyKey: "approval:translation:article-001",
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
