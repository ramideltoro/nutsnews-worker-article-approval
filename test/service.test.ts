import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadApprovalConfig } from "../src/config.js";
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
    expect(context.metrics.collect()).toContain("nutsnews_worker_dependency_duration_ms");

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
});

function createServiceContext() {
  const config = loadApprovalConfig({
    NUTSNEWS_APPROVAL_HTTP_PORT: "0",
    NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalApprovalDependencies();
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createPrometheusRuntimeTelemetrySink({
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
