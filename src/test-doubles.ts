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
  type RuntimeBrokerTransport,
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
  private readonly store;

  constructor(clock: RuntimeClock = new ManualApprovalClock()) {
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): ApprovalDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local approval state ready" : "local approval state degraded"
    };
  }

  claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    return this.store.claim(idempotencyKey, context);
  }

  markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    return this.store.markCompleted(idempotencyKey, completion);
  }

  markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    return this.store.markFailed(idempotencyKey, failure);
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

  async withTransaction<T>(operation: (transaction: ApprovalDatabaseTransaction) => Promise<T>): Promise<T> {
    const transaction = {
      transactionId: `local-transaction-${String(this.transactions.length + 1)}`
    };

    this.transactions.push(transaction);

    return operation(transaction);
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

  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
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

  probe(): ApprovalDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local Qwen endpoint ready" : "local Qwen endpoint degraded"
    };
  }
}

export class LocalApprovalPromptRegistry implements ApprovalPromptRegistry {
  readonly name: string = "local-prompt-registry";
  status: ApprovalDependencyProbe["status"] = "ok";
  prompt: ApprovalPrompt = {
    id: "editorial-approval-v1",
    version: "0.1.0",
    purpose: "editorial-approval"
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
    void tools;
    this.onHandleStart?.();
    await this.handleGate;
    this.handled.push(context);

    return this.result;
  }
}

export class LocalBrokerTransport implements RuntimeBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly inFlightDeliveryCount = 0;
  readonly assertedRoutes: WorkerRoute[] = [];
  readonly published: BrokerPublishCommand[] = [];
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
