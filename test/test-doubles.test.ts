import {
  getWorkerRoute,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  LocalApprovalBrokerOutbox,
  LocalApprovalPromptRegistry,
  LocalApprovalQwenClient,
  LocalApprovalTransactionRunner,
  LocalBrokerTransport,
  createMinimalApprovalDelivery
} from "../src/test-doubles.js";

describe("approval test doubles", () => {
  it("requires a registered local broker consumer before delivery", async () => {
    const broker = new LocalBrokerTransport();

    await broker.connect();

    await expect(broker.deliverApproval(createMinimalApprovalDelivery())).rejects.toThrow("No local consumer is registered for approval.");
  });

  it("records local transaction and outbox boundaries without external dependencies", async () => {
    const runner = new LocalApprovalTransactionRunner();
    const outbox = new LocalApprovalBrokerOutbox();
    const route = getWorkerRoute("translation");
    const command = {
      envelope: {
        schemaId: route.schemaId,
        schemaVersion: 1,
        route: "translation",
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4901",
        causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4801",
        correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4601",
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        idempotencyKey: "approval:translation:article-001",
        aggregate: {
          type: "article",
          id: "article-001",
          version: 1
        },
        occurredAt: "2026-07-23T00:00:00.000Z",
        attempt: {
          count: 1,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z"
        },
        producer: {
          name: "approval",
          version: "0.1.0"
        },
        payloadRef: {
          kind: "backend-record",
          uri: "backend://worker-uplift/approval/article-001/translation-task",
          mediaType: "application/json",
          sizeBytes: 512
        }
      } satisfies WorkerMessageEnvelope,
      payload: {}
    };

    await expect(runner.withTransaction((transaction) => Promise.resolve(transaction.transactionId))).resolves.toBe("local-transaction-1");
    await outbox.record(command, {
      messageId: command.envelope.messageId,
      stage: "translation",
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });

    expect(runner.transactions).toHaveLength(1);
    expect(outbox.records).toHaveLength(1);
  });

  it("provides injectable Qwen and prompt-registry readiness doubles", async () => {
    const qwenClient = new LocalApprovalQwenClient();
    const promptRegistry = new LocalApprovalPromptRegistry();

    expect(qwenClient.probe()).toEqual({
      status: "ok",
      summary: "local Qwen endpoint ready"
    });
    await expect(promptRegistry.getPrompt("editorial-approval-v1")).resolves.toEqual({
      id: "editorial-approval-v1",
      version: "0.1.0",
      purpose: "editorial-approval",
      instructions: "Return a structured editorial approval decision without copying secrets or raw article bodies."
    });
    await expect(qwenClient.review({
      model: "qwen2.5:3b",
      prompt: promptRegistry.prompt,
      timeoutMs: 30_000,
      maxInputBytes: 32_768,
      deterministic: {
        temperature: 0,
        topP: 1
      },
      responseSchema: {
        name: "approval_decision_v1",
        requiredFields: [
          "decision"
        ]
      },
      input: {
        candidateId: "candidate-world-001",
        canonicalArticleId: "article-001",
        articleVersion: 1,
        canonicalUrl: "https://articles.example.test/world/story-001",
        title: "Synthetic world story",
        sourceLanguage: "en",
        contentFingerprint: "fingerprint001"
      },
      inputBytes: 256
    })).resolves.toMatchObject({
      decision: "accepted"
    });
    expect(qwenClient.requests).toHaveLength(1);
  });
});
