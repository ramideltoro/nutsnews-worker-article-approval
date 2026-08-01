import { createHash } from "node:crypto";

import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import type {
  BrokerConsumerHandle,
  BrokerDeliveryHandler,
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport
} from "@ramideltoro/nutsnews-worker-runtime";
import type { QueryResultRow } from "pg";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  PostgresApprovalBrokerOutbox,
  PostgresApprovalOutboxReconciler
} from "../src/production.js";
import { loadApprovalConfig } from "../src/config.js";
import { stableUuid } from "../src/ids.js";
import { APPROVAL_RECONCILIATION_CONFIRMATION } from "../src/reconciliation.js";

const now = "2026-07-23T00:00:00.000Z";
const clock = {
  now: () => new Date(now)
};
const config = loadApprovalConfig({
  NUTSNEWS_APPROVAL_TARGET_LANGUAGES: "fr"
});

describe("approval outbox reconciliation", () => {
  it("records full envelope and payload so service-owned replay can hydrate payload_ref", async () => {
    const pool = new FakePool([]);
    const outbox = new PostgresApprovalBrokerOutbox(pool.asPool());
    const command = translationCommand();

    await outbox.record(command, {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: getWorkerRoute("translation").exchange,
      routingKey: getWorkerRoute("translation").routingKey,
      confirmed: true,
      confirmedAt: now
    });

    const diagnostic = JSON.parse(String(firstQuery(pool).values[14])) as Readonly<Record<string, unknown>>;

    expect(diagnostic).toMatchObject({
      envelope: {
        messageId: command.envelope.messageId,
        correlationId: command.envelope.correlationId,
        causationId: command.envelope.causationId,
        idempotencyKey: command.envelope.idempotencyKey
      },
      payload: {
        schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
        idempotencyKey: command.envelope.idempotencyKey
      },
      payloadSchemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask
    });
  });

  it("dry-runs deterministic candidates without publishing", async () => {
    const command = translationCommand();
    const pool = new FakePool([
      outboxRow(command)
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresApprovalOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      config,
      env: {}
    });

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      status: "dry_run",
      selectedCount: 1,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.candidates[0]).toMatchObject({
      idempotencyKey: command.envelope.idempotencyKey,
      destinationStage: "translation",
      status: "selected"
    });
    expect(transport.published).toHaveLength(0);
  });

  it("applies replay with a new message ID while preserving routing metadata and audit history", async () => {
    const command = translationCommand();
    const pool = new FakePool([
      outboxRow(command)
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresApprovalOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      config,
      env: {
        NUTSNEWS_APPROVAL_RECONCILIATION_APPLY_ENABLED: "true"
      }
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723",
      protectedConfirmation: APPROVAL_RECONCILIATION_CONFIRMATION
    });

    expect(report).toMatchObject({
      status: "applied",
      replayedCount: 1,
      writesPerformed: true,
      productionVisibilityEnabled: false
    });
    expect(transport.published).toHaveLength(1);
    const replay = transport.published[0];
    expect(replay?.envelope.messageId).not.toBe(command.envelope.messageId);
    expect(replay?.envelope.idempotencyKey).toBe(command.envelope.idempotencyKey);
    expect(replay?.envelope.correlationId).toBe(command.envelope.correlationId);
    expect(replay?.envelope.causationId).toBe(command.envelope.causationId);
    expect(replay?.envelope.aggregate).toEqual(command.envelope.aggregate);
    expect(pool.queries.some((query) => query.sql.includes("reconciliationAuditHistory"))).toBe(true);
  });

  it("fails closed without publishing when the authoritative envelope is missing", async () => {
    const command = translationCommand();
    const pool = new FakePool([
      {
        ...outboxRow(command),
        diagnostic_metadata: {
          payload: command.payload,
          payloadSchemaId: command.payload.schemaId
        }
      }
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresApprovalOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      config,
      env: {
        NUTSNEWS_APPROVAL_RECONCILIATION_APPLY_ENABLED: "true"
      }
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723",
      protectedConfirmation: APPROVAL_RECONCILIATION_CONFIRMATION
    });

    expect(report.status).toBe("failed_closed");
    expect(report.errors).toContain("1:missing-stored-envelope");
    expect(report.writesPerformed).toBe(false);
    expect(transport.published).toHaveLength(0);
  });

  it("hydrates legacy rows from approval decision snapshots when payload_ref and digest match", async () => {
    const decision = approvalDecisionSnapshot();
    const payload = translationPayloadFromDecision(decision);
    const row = legacyOutboxRow(decision, payload);
    const pool = new FakePool([
      row
    ], [
      approvalDecisionRow(decision)
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresApprovalOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      config,
      env: {
        NUTSNEWS_APPROVAL_RECONCILIATION_APPLY_ENABLED: "true"
      }
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723",
      protectedConfirmation: APPROVAL_RECONCILIATION_CONFIRMATION
    });

    expect(report.status).toBe("applied");
    expect(report.replayedCount).toBe(1);
    expect(report.failedClosedCount).toBe(0);
    expect(transport.published).toHaveLength(1);
    const replay = transport.published[0];
    expect(replay?.payload).toEqual(payload);
    expect(replay?.envelope.messageId).not.toBe(row.outbox_message_id);
    expect(replay?.envelope.idempotencyKey).toBe(row.idempotency_key);
    expect(replay?.envelope.correlationId).toBe(decision.correlationId);
    expect(replay?.envelope.causationId).toBe(decision.sourceMessageId);
    expect(replay?.envelope.payloadRef.uri).toBe(row.payload_ref);
    expect(pool.queries.some((query) => query.sql.includes("approval_decisions"))).toBe(true);
  });

  it("reconstructs the authoritative payload after PostgreSQL JSONB reorders a complete carrier", async () => {
    const decision = approvalDecisionSnapshot();
    const payload = translationPayloadFromDecision(decision);
    const row = legacyOutboxRow(decision, payload);
    const pool = new FakePool([
      {
        ...row,
        diagnostic_metadata: {
          envelope: translationCommand().envelope,
          payload: reversePayloadInsertionOrder(payload),
          payloadSchemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask
        }
      }
    ], [
      approvalDecisionRow(decision)
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresApprovalOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      config,
      env: {}
    });

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-jsonb-order"
    });

    expect(report).toMatchObject({
      status: "dry_run",
      selectedCount: 1,
      failedClosedCount: 0,
      writesPerformed: false
    });
    expect(transport.published).toHaveLength(0);
  });

  it("still fails closed when the authoritative approval payload digest is tampered", async () => {
    const decision = approvalDecisionSnapshot();
    const payload = translationPayloadFromDecision(decision);
    const row = legacyOutboxRow(decision, payload);
    const diagnosticCommand = translationCommand();
    const pool = new FakePool([
      {
        ...row,
        payload_digest: `sha256:${"0".repeat(64)}`,
        diagnostic_metadata: {
          envelope: {
            ...diagnosticCommand.envelope,
            payloadRef: {
              ...diagnosticCommand.envelope.payloadRef,
              uri: row.payload_ref
            }
          },
          payload: reversePayloadInsertionOrder(payload),
          payloadSchemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask
        }
      }
    ], [
      approvalDecisionRow(decision)
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresApprovalOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      config,
      env: {}
    });

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-tampered-digest"
    });

    expect(report.status).toBe("failed_closed");
    expect(report.errors).toContain("legacy-1:payload-digest-mismatch");
    expect(report.writesPerformed).toBe(false);
    expect(transport.published).toHaveLength(0);
  });
});

function translationCommand(): BrokerPublishCommand {
  const route = getWorkerRoute("translation");
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3401",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3402",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3403",
    idempotencyKey: "approval:translation:article-001:1",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: now,
    articleId: "article-001",
    sourceLanguage: "en",
    targetLanguages: [
      "fr"
    ],
    reason: "new_article",
    existingLanguageCodes: []
  };
  const envelope = assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "translation",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3410",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3403",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3400",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: payload.idempotencyKey,
    aggregate: {
      type: "article",
      id: "article-001",
      version: 1
    },
    occurredAt: now,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: now
    },
    producer: {
      name: "approval",
      version: "0.1.0",
      instanceId: "test-host"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/approval/article-001/translation-task",
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(payload),
      digest: sha256Json(payload)
    }
  });

  return {
    envelope,
    payload
  };
}

function outboxRow(command: BrokerPublishCommand): QueryResultRow {
  return {
    id: "1",
    outbox_message_id: command.envelope.messageId,
    pipeline_run_id: command.payload.pipelineRunId,
    stage_execution_id: command.payload.stageExecutionId,
    destination_stage: command.envelope.route,
    routing_key: getWorkerRoute(command.envelope.route).routingKey,
    entity_kind: command.envelope.aggregate.type,
    entity_id: command.envelope.aggregate.id,
    schema_version: command.envelope.schemaVersion,
    operation_version: command.envelope.aggregate.version,
    idempotency_key: command.envelope.idempotencyKey,
    payload_ref: command.envelope.payloadRef.uri,
    payload_digest: sha256Json(command.payload),
    created_at: new Date("2026-07-22T23:00:00.000Z"),
    published_at: new Date("2026-07-22T23:00:01.000Z"),
    confirmed_at: new Date("2026-07-22T23:00:02.000Z"),
    status: "confirmed",
    diagnostic_metadata: {
      envelope: command.envelope,
      payload: command.payload,
      payloadSchemaId: command.payload.schemaId
    }
  };
}

function approvalDecisionSnapshot() {
  return {
    decisionId: "96ad1cc7-6c44-562d-9206-16e29dd88744",
    candidateId: "candidate-001",
    canonicalArticleId: "article-001",
    articleVersion: 1,
    canonicalUrl: "https://example.com/article-001",
    decision: "accepted",
    provider: "local_ai",
    model: "qwen2.5:3b",
    promptId: "editorial-approval-v1",
    promptVersion: "0.1.0",
    positivityScore: 91,
    confidenceScore: 87,
    qualityScore: 83,
    sourceLanguage: "en",
    contentFingerprint: "fingerprint-001",
    reviewRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/approval/article-001/reviews/96ad1cc7-6c44-562d-9206-16e29dd88744",
      mediaType: "application/json",
      decisionId: "96ad1cc7-6c44-562d-9206-16e29dd88744",
      canonicalArticleId: "article-001",
      articleVersion: 1,
      promptId: "editorial-approval-v1",
      promptVersion: "0.1.0",
      model: "qwen2.5:3b",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3403"
    },
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3403",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3400",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    latencyMs: 125,
    decidedAt: now
  } as const;
}

function translationPayloadFromDecision(decision: ReturnType<typeof approvalDecisionSnapshot>): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3401",
    stageExecutionId: stableUuid([
      "translation-stage-execution",
      decision.decisionId
    ]),
    sourceMessageId: decision.sourceMessageId,
    idempotencyKey: `approval:translation:${decision.decisionId}`,
    traceparent: decision.traceparent,
    producedAt: decision.decidedAt,
    articleId: decision.canonicalArticleId,
    sourceLanguage: decision.sourceLanguage,
    targetLanguages: [
      "fr"
    ],
    reason: "new_article",
    existingLanguageCodes: []
  };
}

function legacyOutboxRow(
  decision: ReturnType<typeof approvalDecisionSnapshot>,
  payload: Readonly<Record<string, unknown>>
): QueryResultRow & { readonly payload_ref: string } {
  return {
    id: "legacy-1",
    outbox_message_id: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3410",
    pipeline_run_id: payload.pipelineRunId,
    stage_execution_id: payload.stageExecutionId,
    destination_stage: "translation",
    routing_key: getWorkerRoute("translation").routingKey,
    entity_kind: "article",
    entity_id: decision.canonicalArticleId,
    schema_version: 1,
    operation_version: decision.articleVersion,
    idempotency_key: payload.idempotencyKey,
    payload_ref: `backend://worker-uplift/approval/${encodeURIComponent(decision.canonicalArticleId)}/${decision.decisionId}/translation-task`,
    payload_digest: sha256Json(payload),
    created_at: new Date("2026-07-22T23:00:00.000Z"),
    published_at: new Date("2026-07-22T23:00:01.000Z"),
    confirmed_at: new Date("2026-07-22T23:00:02.000Z"),
    status: "confirmed",
    diagnostic_metadata: {
      payload: reversePayloadInsertionOrder(payload),
      payloadSchemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask
    }
  };
}

function reversePayloadInsertionOrder(payload: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> {
  return Object.fromEntries(Object.entries(payload).reverse());
}

function approvalDecisionRow(decision: ReturnType<typeof approvalDecisionSnapshot>): QueryResultRow {
  return {
    article_identity_hash: decision.canonicalArticleId,
    approval_version: decision.articleVersion,
    decision: "approved",
    positivity_score: decision.positivityScore,
    ai_provider: decision.provider,
    ai_model: decision.model,
    prompt_version: `${decision.promptId}:${decision.promptVersion}`,
    model_version: decision.model,
    model_metadata: {},
    diagnostic_metadata: {
      decisionId: decision.decisionId,
      decisionSnapshot: decision
    },
    reviewed_at: new Date(decision.decidedAt)
  };
}

function sha256Json(value: unknown): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

function firstQuery(pool: FakePool): { readonly sql: string; readonly values: readonly unknown[] } {
  const query = pool.queries[0];

  if (query === undefined) {
    throw new Error("expected a captured query");
  }

  return query;
}

class FakePool {
  readonly queries: { readonly sql: string; readonly values: readonly unknown[] }[] = [];

  constructor(
    private readonly rows: readonly QueryResultRow[],
    private readonly decisionRows: readonly QueryResultRow[] = []
  ) {}

  asPool() {
    return this as never;
  }

  query(sql: string, values: readonly unknown[] = []): Promise<{ readonly rows: QueryResultRow[]; readonly rowCount: number }> {
    this.queries.push({
      sql,
      values
    });

    if (sql.includes("approval_decisions")) {
      return Promise.resolve({
        rows: [...this.decisionRows],
        rowCount: this.decisionRows.length
      });
    }

    if (sql.trimStart().startsWith("SELECT")) {
      return Promise.resolve({
        rows: [...this.rows],
        rowCount: this.rows.length
      });
    }

    return Promise.resolve({
      rows: [],
      rowCount: 1
    });
  }
}

class FakeBrokerTransport implements RuntimeBrokerTransport {
  readonly name = "fake-broker";
  readonly published: BrokerPublishCommand[] = [];

  connect(): Promise<void> {
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    void routes;
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: now
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    void stage;
    void handler;
    throw new Error("consume is not supported in fake transport");
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}
