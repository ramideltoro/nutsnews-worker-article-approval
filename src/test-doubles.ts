import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createInMemoryIdempotencyStore,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeClock,
  type RuntimeHandlerResult,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  ApprovalBrokerOutbox,
  ApprovalBrokerTransport,
  ApprovalClaimOwnership,
  ApprovalDatabaseTransaction,
  ApprovalDatabaseTransactionRunner,
  ApprovalDependencies,
  ApprovalDependencyProbe,
  ApprovalDecisionKey,
  ApprovalEnrichmentRecord,
  ApprovalEnrichmentRecordInput,
  ApprovalPrompt,
  ApprovalPromptRegistry,
  ApprovalQwenRequest,
  ApprovalQwenClient,
  ApprovalStateStore,
  ApprovalStoredDecision,
  ApprovalTranslationPublication,
  ApprovalWorkHandler,
  ApprovalWorkTools
} from "./dependencies.js";
import { ApprovalClaimOwnershipError } from "./dependencies.js";

export class ManualApprovalClock implements RuntimeClock {
  private current: Date;

  constructor(initial = "2026-07-23T00:00:00.000Z") {
    this.current = new Date(initial);
  }

  now(): Date {
    return new Date(this.current.getTime());
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export class InMemoryApprovalStateStore implements ApprovalStateStore {
  readonly name: string = "local-approval-state";
  status: ApprovalDependencyProbe["status"] = "ok";
  readonly decisions: ApprovalStoredDecision[] = [];
  readonly enrichmentRecords = new Map<string, ApprovalEnrichmentRecord>();
  private readonly store;
  private readonly activeClaims = new Map<string, {
    readonly claimToken: string;
    readonly controller: AbortController;
  }>();
  private acceptingClaims = true;

  constructor(clock: RuntimeClock = new ManualApprovalClock()) {
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): ApprovalDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local approval state ready" : "local approval state degraded"
    };
  }

  async claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    const result = await this.store.claim(idempotencyKey, context);

    if (result.status === "claimed") {
      this.activeClaims.get(idempotencyKey)?.controller.abort(
        new ApprovalClaimOwnershipError("Approval idempotency claim was replaced.")
      );
      this.activeClaims.set(idempotencyKey, {
        claimToken: result.claimToken,
        controller: new AbortController()
      });
      if (!this.acceptingClaims) {
        this.activeClaims.get(idempotencyKey)?.controller.abort(
          new ApprovalClaimOwnershipError("Approval state store is not accepting new claims.")
        );
      }
    }

    return result;
  }

  async markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    try {
      await this.store.markCompleted(idempotencyKey, completion);
    } finally {
      this.finishClaim(idempotencyKey, completion.claimToken);
    }
  }

  async markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    try {
      await this.store.markFailed(idempotencyKey, failure);
    } finally {
      this.finishClaim(idempotencyKey, failure.claimToken);
    }
  }

  async releaseClaim(idempotencyKey: string, failure: RuntimeIdempotencyFailure) {
    try {
      return await this.store.releaseClaim(idempotencyKey, failure);
    } finally {
      this.finishClaim(idempotencyKey, failure.claimToken);
    }
  }

  ownership(idempotencyKey: string): ApprovalClaimOwnership {
    const active = this.activeClaims.get(idempotencyKey);

    if (active === undefined) {
      throw new ApprovalClaimOwnershipError("No active approval idempotency claim exists.");
    }

    return {
      signal: active.controller.signal,
      assertOwned: () => {
        if (this.activeClaims.get(idempotencyKey) !== active || active.controller.signal.aborted) {
          throw ownershipAbortReason(active.controller.signal);
        }
      }
    };
  }

  abortActiveClaims(reason: string): Promise<void> {
    this.acceptingClaims = false;
    for (const active of this.activeClaims.values()) {
      active.controller.abort(new ApprovalClaimOwnershipError(reason));
    }

    this.activeClaims.clear();
    return Promise.resolve();
  }

  loadEnrichmentRecord(input: ApprovalEnrichmentRecordInput, transaction: ApprovalDatabaseTransaction): Promise<ApprovalEnrichmentRecord> {
    void transaction;
    const key = enrichmentRecordKey(input.articleMetadataRef.canonicalArticleId, input.articleMetadataRef.articleVersion);
    const existing = this.enrichmentRecords.get(key);

    if (existing !== undefined) {
      return Promise.resolve(existing);
    }

    const record: ApprovalEnrichmentRecord = {
      candidateId: input.candidateId,
      canonicalArticleId: input.articleMetadataRef.canonicalArticleId,
      articleVersion: input.articleMetadataRef.articleVersion,
      canonicalUrl: input.canonicalUrl,
      imageStatus: input.imageStatus,
      ...(input.imageUrl === undefined ? {} : {
        imageUrl: input.imageUrl
      }),
      contentFingerprint: input.articleMetadataRef.contentFingerprint,
      title: input.articleMetadataRef.title ?? "Synthetic world story",
      ...(input.articleMetadataRef.description === undefined ? {} : {
        description: input.articleMetadataRef.description
      }),
      ...(input.articleMetadataRef.publishedAt === undefined ? {} : {
        publishedAt: input.articleMetadataRef.publishedAt
      }),
      sourceLanguage: input.articleMetadataRef.language ?? "en",
      metadataRef: input.articleMetadataRef
    };

    this.enrichmentRecords.set(key, record);

    return Promise.resolve(record);
  }

  findDecision(key: ApprovalDecisionKey, transaction: ApprovalDatabaseTransaction): Promise<ApprovalStoredDecision | undefined> {
    void transaction;

    return Promise.resolve(this.decisions.find((decision) => decision.canonicalArticleId === key.canonicalArticleId
      && decision.articleVersion === key.articleVersion
      && decision.promptId === key.promptId
      && decision.promptVersion === key.promptVersion
      && decision.model === key.model));
  }

  recordDecision(decision: ApprovalStoredDecision, transaction: ApprovalDatabaseTransaction): Promise<ApprovalStoredDecision> {
    void transaction;
    const existingIndex = this.decisions.findIndex((stored) => stored.decisionId === decision.decisionId);

    if (existingIndex === -1) {
      this.decisions.push(decision);
    } else {
      this.decisions[existingIndex] = decision;
    }

    return Promise.resolve(decision);
  }

  markTranslationPublished(
    decisionId: string,
    publication: ApprovalTranslationPublication,
    transaction: ApprovalDatabaseTransaction
  ): Promise<ApprovalStoredDecision> {
    void transaction;
    const existingIndex = this.decisions.findIndex((decision) => decision.decisionId === decisionId);

    if (existingIndex === -1) {
      return Promise.reject(new Error(`Approval decision ${decisionId} is not recorded.`));
    }

    const existing = this.decisions[existingIndex];

    if (existing === undefined) {
      return Promise.reject(new Error(`Approval decision ${decisionId} is not recorded.`));
    }

    const updated = {
      ...existing,
      translationPublication: publication
    } satisfies ApprovalStoredDecision;

    this.decisions[existingIndex] = updated;

    return Promise.resolve(updated);
  }

  seedEnrichmentRecord(record: ApprovalEnrichmentRecord): void {
    this.enrichmentRecords.set(enrichmentRecordKey(record.canonicalArticleId, record.articleVersion), record);
  }

  private finishClaim(idempotencyKey: string, claimToken: string): void {
    const active = this.activeClaims.get(idempotencyKey);

    if (active?.claimToken === claimToken) {
      this.activeClaims.delete(idempotencyKey);
    }
  }
}

export class LocalApprovalTransactionRunner implements ApprovalDatabaseTransactionRunner {
  readonly name: string = "local-database-transactions";
  status: ApprovalDependencyProbe["status"] = "ok";
  readonly transactions: ApprovalDatabaseTransaction[] = [];

  probe(): ApprovalDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local transaction runner ready" : "local transaction runner degraded"
    };
  }

  async withTransaction<T>(
    operation: (transaction: ApprovalDatabaseTransaction) => Promise<T>,
    signal?: AbortSignal
  ): Promise<T> {
    throwIfOperationAborted(signal);
    const transaction = {
      transactionId: `local-transaction-${String(this.transactions.length + 1)}`
    };

    this.transactions.push(transaction);

    const value = await waitForOperation(operation(transaction), signal);
    throwIfOperationAborted(signal);

    return value as T;
  }
}

export class LocalApprovalBrokerOutbox implements ApprovalBrokerOutbox {
  readonly name: string = "local-broker-outbox";
  status: ApprovalDependencyProbe["status"] = "ok";
  readonly records: { readonly command: BrokerPublishCommand; readonly receipt: BrokerPublishReceipt }[] = [];

  probe(): ApprovalDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local broker outbox ready" : "local broker outbox degraded"
    };
  }

  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt, signal?: AbortSignal): Promise<void> {
    throwIfOperationAborted(signal);
    this.records.push({
      command,
      receipt
    });
    return Promise.resolve();
  }
}

export class LocalApprovalQwenClient implements ApprovalQwenClient {
  readonly name: string = "local-qwen-client";
  status: ApprovalDependencyProbe["status"] = "ok";
  readonly requests: ApprovalQwenRequest[] = [];
  activeRequests = 0;
  maxActiveRequests = 0;
  response: unknown = {
    decision: "accepted",
    reasonCode: "newsworthy",
    confidenceScore: 92,
    qualityScore: 88,
    positivityScore: 73,
    summary: "A concise source-language summary that is long enough for approval fixtures.",
    latencyMs: 42,
    usage: {
      inputTokens: 120,
      outputTokens: 36,
      totalTokens: 156
    }
  };
  error: unknown;
  reviewGate: Promise<unknown> | undefined;
  onReviewStart: (() => void) | undefined;

  probe(): ApprovalDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local Qwen endpoint ready" : "local Qwen endpoint degraded"
    };
  }

  async review(request: ApprovalQwenRequest, signal?: AbortSignal): Promise<unknown> {
    throwIfOperationAborted(signal);
    this.requests.push(request);
    this.activeRequests += 1;
    this.maxActiveRequests = Math.max(this.maxActiveRequests, this.activeRequests);
    this.onReviewStart?.();

    try {
      await waitForOperation(this.reviewGate, signal);
      throwIfOperationAborted(signal);

      if (this.error !== undefined) {
        const reason = typeof this.error === "string" ? this.error : "local Qwen fixture error";

        throw this.error instanceof Error ? this.error : new Error(reason);
      }

      return this.response;
    } finally {
      this.activeRequests = Math.max(0, this.activeRequests - 1);
    }
  }
}

export class LocalApprovalPromptRegistry implements ApprovalPromptRegistry {
  readonly name: string = "local-prompt-registry";
  status: ApprovalDependencyProbe["status"] = "ok";
  prompt: ApprovalPrompt = {
    id: "editorial-approval-v1",
    version: "0.1.0",
    purpose: "editorial-approval",
    instructions: "Return a structured editorial approval decision without copying secrets or raw article bodies."
  };

  probe(): ApprovalDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local prompt registry ready" : "local prompt registry degraded"
    };
  }

  getPrompt(id: string): Promise<ApprovalPrompt> {
    if (id !== this.prompt.id) {
      return Promise.reject(new Error("Prompt fixture is not registered."));
    }

    return Promise.resolve(this.prompt);
  }
}

export class LocalApprovalWorkHandler implements ApprovalWorkHandler {
  readonly name: string = "local-approval-work-handler";
  readonly handled: RuntimeMessageContext[] = [];
  result: RuntimeHandlerResult = {
    status: "ok"
  };
  handleGate: Promise<unknown> | undefined;
  onHandleStart: (() => void) | undefined;

  async handle(context: RuntimeMessageContext, tools: ApprovalWorkTools): Promise<RuntimeHandlerResult> {
    tools.assertOwnership();
    this.onHandleStart?.();
    await waitForOperation(this.handleGate, tools.signal);
    tools.assertOwnership();
    this.handled.push(context);

    return this.result;
  }
}

function throwIfOperationAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted === true) {
    throw ownershipAbortReason(signal);
  }
}

function ownershipAbortReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new ApprovalClaimOwnershipError();
}

function waitForOperation<T>(operation: Promise<T> | undefined, signal: AbortSignal | undefined): Promise<T | undefined> {
  if (operation === undefined) {
    throwIfOperationAborted(signal);
    return Promise.resolve(undefined);
  }

  if (signal === undefined) {
    return operation;
  }

  throwIfOperationAborted(signal);

  return new Promise((resolve, reject) => {
    const onAbort = (): void => {
      reject(ownershipAbortReason(signal));
    };

    signal.addEventListener("abort", onAbort, {
      once: true
    });
    void operation.then(resolve, reject).finally(() => {
      signal.removeEventListener("abort", onAbort);
    });
  });
}

export class LocalBrokerTransport implements ApprovalBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly inFlightDeliveryCount = 0;
  readonly assertedRoutes: WorkerRoute[] = [];
  readonly published: BrokerPublishCommand[] = [];
  publishError: Error | undefined;
  private connected = false;
  private readonly consumers = new Map<WorkerStage, BrokerDeliveryHandler>();

  connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.assertConnected();
    this.assertedRoutes.push(...routes);
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    this.assertConnected();

    if (this.publishError !== undefined) {
      return Promise.reject(this.publishError);
    }

    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });
  }

  publishOwned(command: BrokerPublishCommand, signal: AbortSignal): Promise<BrokerPublishReceipt> {
    throwIfOperationAborted(signal);
    return this.publish(command);
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    this.assertConnected();
    this.consumers.set(stage, handler);

    return Promise.resolve({
      stage,
      cancel: () => {
        this.consumers.delete(stage);
        return Promise.resolve();
      }
    });
  }

  deliverApproval(delivery: RuntimeMessageDelivery = createMinimalApprovalDelivery()): Promise<RuntimeMessageProcessingResult> {
    const handler = this.consumers.get("approval");

    if (handler === undefined) {
      return Promise.reject(new Error("No local consumer is registered for approval."));
    }

    return handler(delivery);
  }

  drain(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.connected = false;
    this.consumers.clear();
    return Promise.resolve();
  }

  private assertConnected(): void {
    if (!this.connected) {
      throw new Error("Local broker transport is not connected.");
    }
  }
}

export function createLocalApprovalDependencies(options: {
  readonly clock?: RuntimeClock;
  readonly workHandler?: ApprovalWorkHandler;
} = {}): ApprovalDependencies {
  const clock = options.clock ?? new ManualApprovalClock();

  return {
    adapterMode: "in_memory",
    clock,
    stateStore: new InMemoryApprovalStateStore(clock),
    transactionRunner: new LocalApprovalTransactionRunner(),
    brokerOutbox: new LocalApprovalBrokerOutbox(),
    brokerTransport: new LocalBrokerTransport(),
    qwenClient: new LocalApprovalQwenClient(),
    promptRegistry: new LocalApprovalPromptRegistry(),
    workHandler: options.workHandler ?? new LocalApprovalWorkHandler()
  };
}

export function createMinimalApprovalEnvelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  const route = getWorkerRoute("approval");
  const occurredAt = "2026-07-23T00:00:00.000Z";
  const envelope = {
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "approval",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4801",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4701",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4601",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "enrichment:approval:enrichment-req-001:fingerprint001",
    aggregate: {
      type: "article",
      id: "article-001",
      version: 1
    },
    occurredAt,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: occurredAt
    },
    producer: {
      name: "enrichment",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/enrichment/article-001/fingerprint001",
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(createMinimalApprovalPayload())
    },
    ...overrides
  };

  return assertWorkerEnvelope(envelope);
}

export function createMinimalApprovalPayload(
  overrides: Readonly<Record<string, unknown>> = {}
): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.enrichmentResult,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4701",
    idempotencyKey: "enrichment:approval:enrichment-req-001:fingerprint001",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: "2026-07-23T00:00:00.000Z",
    candidateId: "candidate-world-001",
    canonicalUrl: "https://articles.example.test/world/story-001",
    imageStatus: "hydrated",
    imageUrl: "https://images.example.test/world/story-001.jpg",
    articleMetadataRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/enrichment/article-001/fingerprint001",
      mediaType: "application/json",
      contentFingerprint: "fingerprint001",
      canonicalArticleId: "article-001",
      articleVersion: 1,
      title: "Synthetic world story",
      description: "A durable metadata description retained by reference for local approval tests.",
      language: "en"
    },
    ...overrides
  };
}

export function createMinimalApprovalDelivery(): RuntimeMessageDelivery {
  return {
    envelope: createMinimalApprovalEnvelope(),
    payload: createMinimalApprovalPayload(),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}

function enrichmentRecordKey(canonicalArticleId: string, articleVersion: number): string {
  return `${canonicalArticleId}:${String(articleVersion)}`;
}
