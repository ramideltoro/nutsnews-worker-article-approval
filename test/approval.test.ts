import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  WORKER_DELIVERY_BEHAVIOR
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { createArticleApprovalWorkHandler } from "../src/approval.js";
import { loadApprovalConfig } from "../src/config.js";
import { ApprovalQwenError } from "../src/dependencies.js";
import { createApprovalService } from "../src/service.js";
import {
  InMemoryApprovalStateStore,
  LocalApprovalBrokerOutbox,
  LocalApprovalQwenClient,
  LocalBrokerTransport,
  ManualApprovalClock,
  createLocalApprovalDependencies,
  createMinimalApprovalEnvelope,
  createMinimalApprovalPayload
} from "../src/test-doubles.js";

describe("createArticleApprovalWorkHandler", () => {
  it("accepts valid Qwen decisions, records traceable metadata, and publishes one translation task", async () => {
    const context = createApprovalContext();
    const summary = "Local reporting describes a constructive public-interest update with enough detail for translation.";

    context.qwenClient.response = {
      decision: "accepted",
      reasonCode: "newsworthy",
      category: "Community | Uplifting",
      confidenceScore: 94,
      qualityScore: 91,
      positivityScore: 82,
      summary,
      latencyMs: 37,
      usage: {
        inputTokens: 110,
        outputTokens: 42,
        totalTokens: 152
      }
    };

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.qwenClient.requests).toHaveLength(1);
    expect(context.qwenClient.requests[0]).toMatchObject({
      model: "qwen2.5:3b",
      timeoutMs: 30_000,
      deterministic: {
        temperature: 0,
        topP: 1
      },
      responseSchema: {
        name: "approval_decision_v1"
      }
    });
    expect(context.qwenClient.requests[0]?.prompt).toMatchObject({
      id: "editorial-approval-v1",
      version: "0.1.0"
    });
    expect(context.qwenClient.requests[0]?.inputBytes).toBeGreaterThan(0);
    expect(context.qwenClient.requests[0]?.inputBytes).toBeLessThanOrEqual(context.config.qwen.maxInputBytes);
    expect(context.stateStore.decisions).toHaveLength(1);
    expect(context.stateStore.decisions[0]).toMatchObject({
      candidateId: "candidate-world-001",
      canonicalArticleId: "article-001",
      articleVersion: 1,
      decision: "accepted",
      provider: "local_ai",
      model: "qwen2.5:3b",
      promptId: "editorial-approval-v1",
      promptVersion: "0.1.0",
      canonicalUrl: "https://articles.example.test/world/story-001",
      title: "Synthetic world story",
      description: "A durable metadata description retained by reference for local approval tests.",
      imageUrl: "https://images.example.test/world/story-001.jpg",
      category: "Community | Uplifting",
      sourceSummary: summary,
      sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4801",
      correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4601",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    });
    expect(context.stateStore.decisions[0]?.reviewRef).toMatchObject({
      canonicalArticleId: "article-001",
      articleVersion: 1,
      promptId: "editorial-approval-v1",
      promptVersion: "0.1.0",
      model: "qwen2.5:3b",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4801"
    });
    expect(context.stateStore.decisions[0]?.aiUsageRef).toMatchObject({
      inputTokens: 110,
      outputTokens: 42,
      totalTokens: 152
    });
    const decisionId = context.stateStore.decisions[0]?.decisionId ?? "missing-decision-id";

    expect(context.stateStore.decisions[0]?.translationPublication).toMatchObject({
      messageId: context.broker.published[0]?.envelope.messageId,
      idempotencyKey: `approval:translation:${decisionId}`
    });
    expect(context.broker.published).toHaveLength(1);
    expect(context.broker.published[0]?.envelope.route).toBe("translation");
    expect(context.broker.published[0]?.payload).toMatchObject({
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
      articleId: "article-001",
      sourceLanguage: "en",
      targetLanguages: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ],
      reason: "new_article",
      existingLanguageCodes: []
    });
    expect(context.outbox.records).toHaveLength(1);

    const telemetryJson = JSON.stringify(context.telemetry.events);
    const decisionTelemetry = context.telemetry.events.find(
      (event) => event.name === "runtime.dependency.observed"
        && event.attributes?.event === "approval.article.reviewed"
    );

    expect(telemetryJson).not.toContain(summary);
    expect(telemetryJson).not.toContain(context.qwenClient.requests[0]?.prompt.instructions);
    expect(decisionTelemetry).toMatchObject({
      durationMs: 37,
      attributes: {
        latencyMs: 37
      }
    });
  });

  it("records valid rejected Qwen decisions without publishing translation work", async () => {
    const context = createApprovalContext();

    context.qwenClient.response = {
      decision: "rejected",
      reasonCode: "low_quality",
      confidenceScore: 88,
      qualityScore: 31,
      positivityScore: 14,
      latencyMs: 21
    };

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.stateStore.decisions).toHaveLength(1);
    expect(context.stateStore.decisions[0]).toMatchObject({
      decision: "rejected",
      rejectionReason: "low_quality",
      provider: "local_ai",
      qualityScore: 31
    });
    expect(context.broker.published).toHaveLength(0);
  });

  it.each([
    "throw",
    "reject"
  ] as const)("keeps a decision telemetry %s from changing persistence, publication, or duplicate acknowledgement", async (failureMode) => {
    const clock = new ManualApprovalClock();
    const config = loadApprovalConfig({
      NUTSNEWS_APPROVAL_HTTP_PORT: "0",
      NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
    });
    const baseDependencies = createLocalApprovalDependencies({
      clock
    });
    const telemetry = {
      emit: (): void | Promise<void> => {
        if (failureMode === "throw") {
          throw new Error("decision telemetry unavailable");
        }

        return Promise.reject(new Error("decision telemetry unavailable"));
      }
    };
    const dependencies = {
      ...baseDependencies,
      workHandler: createArticleApprovalWorkHandler({
        config,
        dependencies: baseDependencies,
        telemetry
      })
    };
    const service = createApprovalService({
      config,
      dependencies
    });
    const broker = dependencies.brokerTransport as LocalBrokerTransport;
    const stateStore = dependencies.stateStore as InMemoryApprovalStateStore;
    const qwenClient = dependencies.qwenClient as LocalApprovalQwenClient;
    const delivery = {
      envelope: createMinimalApprovalEnvelope(),
      payload: createMinimalApprovalPayload(),
      receivedAt: "2026-07-23T00:00:01.000Z"
    };

    await service.start();
    await expect(broker.deliverApproval(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(broker.deliverApproval(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });
    await service.stop();

    expect(stateStore.decisions).toHaveLength(1);
    expect(qwenClient.requests).toHaveLength(1);
    expect(broker.published).toHaveLength(1);
  });

  it("rejects no-thumbnail enrichment results before calling Qwen", async () => {
    const context = createApprovalContext();
    const noThumbnailPayload = withoutImageUrl(createMinimalApprovalPayload({
      imageStatus: "no_thumbnail"
    }));

    await context.service.start();

    await expect(context.broker.deliverApproval({
      envelope: createMinimalApprovalEnvelope(),
      payload: noThumbnailPayload,
      receivedAt: "2026-07-23T00:00:01.000Z"
    })).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.qwenClient.requests).toHaveLength(0);
    expect(context.stateStore.decisions[0]).toMatchObject({
      decision: "rejected",
      rejectionReason: "no_thumbnail",
      provider: "prefilter"
    });
    expect(context.broker.published).toHaveLength(0);
    expect(context.telemetry.events.find(
      (event) => event.name === "runtime.dependency.observed"
        && event.attributes?.event === "approval.article.reviewed"
    )?.durationMs).toBeUndefined();
  });

  it("stores invalid Qwen schemas as permanent failures without publishing translation work", async () => {
    const context = createApprovalContext();

    context.qwenClient.response = {
      decision: "maybe",
      reasonCode: "unknown",
      confidenceScore: 50,
      qualityScore: 50,
      positivityScore: 50,
      latencyMs: 10
    };

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.stateStore.decisions[0]).toMatchObject({
      decision: "permanent_failure",
      rejectionReason: "invalid_ai_decision_schema"
    });
    expect(context.broker.published).toHaveLength(0);
  });

  it("stores accepted Qwen decisions with empty summaries as permanent failures", async () => {
    const context = createApprovalContext();

    context.qwenClient.response = {
      decision: "accepted",
      reasonCode: "newsworthy",
      confidenceScore: 91,
      qualityScore: 86,
      positivityScore: 75,
      summary: "  ",
      latencyMs: 12
    };

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.stateStore.decisions[0]).toMatchObject({
      decision: "permanent_failure",
      rejectionReason: "empty_summary"
    });
    expect(context.broker.published).toHaveLength(0);
  });

  it("retries Qwen timeouts without recording a decision", async () => {
    const context = createApprovalContext();

    context.qwenClient.error = new ApprovalQwenError("qwen-timeout", {
      retryable: true,
      retryAfterMs: 5_000
    });

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "retry",
      reason: "qwen-timeout",
      retryAfterMs: 5_000
    });

    await context.service.stop();

    expect(context.stateStore.decisions).toHaveLength(0);
    expect(context.broker.published).toHaveLength(0);
  });

  it("stores Qwen unauthorized errors as permanent failures", async () => {
    const context = createApprovalContext();

    context.qwenClient.error = new ApprovalQwenError("qwen-unauthorized", {
      retryable: false
    });

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.stateStore.decisions[0]).toMatchObject({
      decision: "permanent_failure",
      rejectionReason: "qwen-unauthorized"
    });
    expect(context.broker.published).toHaveLength(0);
  });

  it("retries retryable Qwen model errors without publishing translation work", async () => {
    const context = createApprovalContext();

    context.qwenClient.error = new ApprovalQwenError("qwen-model-error", {
      retryable: true
    });

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "retry",
      reason: "qwen-model-error"
    });

    await context.service.stop();

    expect(context.stateStore.decisions).toHaveLength(0);
    expect(context.broker.published).toHaveLength(0);
  });

  it("reuses recorded article-version decisions and does not duplicate Qwen calls or translation publishes", async () => {
    const context = createApprovalContext();
    const secondMessageId = "018f1598-2dd5-7c4f-9f92-8f7a7f8b4802";

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverApproval({
      envelope: createMinimalApprovalEnvelope({
        messageId: secondMessageId,
        idempotencyKey: "enrichment:approval:enrichment-req-002:fingerprint001"
      }),
      payload: createMinimalApprovalPayload({
        sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4702",
        idempotencyKey: "enrichment:approval:enrichment-req-002:fingerprint001"
      }),
      receivedAt: "2026-07-23T00:00:02.000Z"
    })).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.qwenClient.requests).toHaveLength(1);
    expect(context.stateStore.decisions).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);
    expect(context.outbox.records).toHaveLength(1);
    const reusedDecisionTelemetry = context.telemetry.events.find(
      (event) => event.attributes?.reusedDecision === true
    );
    expect(reusedDecisionTelemetry).toBeDefined();
    expect(reusedDecisionTelemetry?.durationMs).toBeUndefined();
  });

  it("limits parallel Qwen calls while queued deliveries wait for capacity", async () => {
    const context = createApprovalContext({
      NUTSNEWS_APPROVAL_CONCURRENCY: "2",
      NUTSNEWS_APPROVAL_PREFETCH: "2",
      NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS: "1",
      NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS: "1"
    });
    const gate = deferred<undefined>();
    const firstReviewStarted = deferred<undefined>();
    let reviewStarts = 0;

    context.qwenClient.reviewGate = gate.promise;
    context.qwenClient.onReviewStart = () => {
      reviewStarts += 1;

      if (reviewStarts === 1) {
        firstReviewStarted.resolve(undefined);
      }
    };

    await context.service.start();
    const first = context.broker.deliverApproval(createArticleDelivery(101, 1));

    await firstReviewStarted.promise;

    const second = context.broker.deliverApproval(createArticleDelivery(102, 2));

    await Promise.resolve();
    await Promise.resolve();

    expect(context.qwenClient.activeRequests).toBe(1);
    expect(context.qwenClient.requests).toHaveLength(1);

    gate.resolve(undefined);

    await expect(first).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(second).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await context.service.stop();

    expect(context.qwenClient.requests).toHaveLength(2);
    expect(context.qwenClient.maxActiveRequests).toBe(1);
    expect(context.broker.published).toHaveLength(2);
  });

  it("returns bounded retries when Qwen capacity is saturated", async () => {
    const context = createApprovalContext({
      NUTSNEWS_APPROVAL_CONCURRENCY: "2",
      NUTSNEWS_APPROVAL_PREFETCH: "2",
      NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS: "1",
      NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS: "0",
      NUTSNEWS_APPROVAL_QWEN_BACKPRESSURE_RETRY_AFTER_MS: "9000"
    });
    const gate = deferred<undefined>();
    const firstReviewStarted = deferred<undefined>();

    context.qwenClient.reviewGate = gate.promise;
    context.qwenClient.onReviewStart = () => {
      firstReviewStarted.resolve(undefined);
    };

    await context.service.start();
    const first = context.broker.deliverApproval(createArticleDelivery(201, 1));

    await firstReviewStarted.promise;

    await expect(context.broker.deliverApproval(createArticleDelivery(202, 2))).resolves.toMatchObject({
      action: "retry",
      reason: "qwen-backpressure",
      retryAfterMs: 9_000
    });

    expect(context.qwenClient.requests).toHaveLength(1);

    gate.resolve(undefined);

    await expect(first).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await context.service.stop();
  });

  it("retries Qwen rate limits as an approved transient condition", async () => {
    const context = createApprovalContext();

    context.qwenClient.error = new ApprovalQwenError("qwen-rate-limited", {
      retryable: true,
      retryAfterMs: 60_000
    });

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "retry",
      reason: "qwen-rate-limited",
      retryAfterMs: 60_000
    });

    await context.service.stop();

    expect(context.stateStore.decisions).toHaveLength(0);
    expect(context.broker.published).toHaveLength(0);
  });

  it("does not retry unauthorized Qwen errors even when misclassified as retryable", async () => {
    const context = createApprovalContext();

    context.qwenClient.error = new ApprovalQwenError("qwen-unauthorized", {
      retryable: true
    });

    await context.service.start();

    await expect(context.broker.deliverApproval()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.stateStore.decisions[0]).toMatchObject({
      decision: "permanent_failure",
      rejectionReason: "qwen-unauthorized"
    });
    expect(context.broker.published).toHaveLength(0);
  });

  it("DLQs repeated transient Qwen failures after retry attempts are exhausted", async () => {
    const context = createApprovalContext();

    context.qwenClient.error = new ApprovalQwenError("qwen-timeout", {
      retryable: true,
      retryAfterMs: 5_000
    });

    await context.service.start();

    await expect(context.broker.deliverApproval({
      ...createArticleDelivery(301, 1),
      envelope: createMinimalApprovalEnvelope({
        messageId: messageIdFor(1),
        idempotencyKey: "enrichment:approval:article-301:fingerprint-301:1",
        aggregate: {
          type: "article",
          id: "article-301",
          version: 1
        },
        attempt: {
          count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
          max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
          firstAttemptAt: "2026-07-23T00:00:00.000Z"
        }
      })
    })).resolves.toMatchObject({
      action: "dlq",
      reason: "qwen-timeout"
    });

    await context.service.stop();

    expect(context.stateStore.decisions).toHaveLength(0);
    expect(context.broker.published).toHaveLength(0);
  });

  it("recovers a recorded decision after publish failure without another Qwen call", async () => {
    const context = createApprovalContext();

    context.broker.publishError = new Error("publish unavailable");

    await context.service.start();

    await expect(context.broker.deliverApproval(createArticleDelivery(401, 1))).resolves.toMatchObject({
      action: "retry",
      reason: "handler-error"
    });

    expect(context.qwenClient.requests).toHaveLength(1);
    expect(context.stateStore.decisions).toHaveLength(1);
    expect(context.stateStore.decisions[0]?.translationPublication).toBeUndefined();
    expect(context.broker.published).toHaveLength(0);

    await context.service.stop();

    context.broker.publishError = undefined;
    context.qwenClient.error = new Error("Qwen should not be called after decision persistence.");
    const restartedService = createApprovalService({
      config: context.config,
      dependencies: context.dependencies,
      telemetry: context.telemetry
    });

    await restartedService.start();

    await expect(context.broker.deliverApproval(createArticleDelivery(401, 2))).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await restartedService.stop();

    expect(context.qwenClient.requests).toHaveLength(1);
    expect(context.broker.published).toHaveLength(1);
    expect(context.stateStore.decisions[0]?.translationPublication).toBeDefined();
  });
});

function createApprovalContext(env: NodeJS.ProcessEnv = {}) {
  const clock = new ManualApprovalClock();
  const config = loadApprovalConfig({
    NUTSNEWS_APPROVAL_HTTP_PORT: "0",
    NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent",
    ...env
  });
  const baseDependencies = createLocalApprovalDependencies({
    clock
  });
  const telemetry = createBufferedRuntimeTelemetrySink(200);
  const dependencies = {
    ...baseDependencies,
    workHandler: createArticleApprovalWorkHandler({
      config,
      dependencies: baseDependencies,
      telemetry
    })
  };
  const service = createApprovalService({
    config,
    dependencies,
    telemetry
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    config,
    dependencies,
    outbox: dependencies.brokerOutbox as LocalApprovalBrokerOutbox,
    qwenClient: dependencies.qwenClient as LocalApprovalQwenClient,
    service,
    stateStore: dependencies.stateStore as InMemoryApprovalStateStore,
    telemetry
  };
}

function withoutImageUrl(payload: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> {
  const entries = Object.entries(payload).filter(([key]) => key !== "imageUrl");

  return Object.fromEntries(entries);
}

function createArticleDelivery(articleNumber: number, deliveryNumber: number) {
  const articleId = `article-${String(articleNumber)}`;
  const candidateId = `candidate-${String(articleNumber)}`;
  const fingerprint = `fingerprint-${String(articleNumber)}`;
  const idempotencyKey = `enrichment:approval:${articleId}:${fingerprint}:${String(deliveryNumber)}`;
  const sourceMessageId = messageIdFor(deliveryNumber + 100);

  return {
    envelope: createMinimalApprovalEnvelope({
      messageId: messageIdFor(deliveryNumber),
      causationId: sourceMessageId,
      idempotencyKey,
      aggregate: {
        type: "article",
        id: articleId,
        version: 1
      }
    }),
    payload: createMinimalApprovalPayload({
      sourceMessageId,
      idempotencyKey,
      candidateId,
      canonicalUrl: `https://articles.example.test/world/story-${String(articleNumber)}`,
      articleMetadataRef: {
        kind: "backend-record",
        uri: `backend://worker-uplift/enrichment/${articleId}/${fingerprint}`,
        mediaType: "application/json",
        contentFingerprint: fingerprint,
        canonicalArticleId: articleId,
        articleVersion: 1,
        title: `Synthetic approval story ${String(articleNumber)}`,
        description: "A sanitized parity-style metadata description for approval recovery tests.",
        language: "en"
      }
    }),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}

function messageIdFor(index: number): string {
  return `018f1598-2dd5-7c4f-9f92-8f7a7f8b${String(5_000 + index).slice(-4)}`;
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
