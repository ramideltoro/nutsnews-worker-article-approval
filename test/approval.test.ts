import {
  STAGE_PAYLOAD_SCHEMA_IDS
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

    expect(telemetryJson).not.toContain(summary);
    expect(telemetryJson).not.toContain(context.qwenClient.requests[0]?.prompt.instructions);
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
    expect(context.telemetry.events.some((event) => event.attributes?.reusedDecision === true)).toBe(true);
  });
});

function createApprovalContext() {
  const clock = new ManualApprovalClock();
  const config = loadApprovalConfig({
    NUTSNEWS_APPROVAL_HTTP_PORT: "0",
    NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
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
