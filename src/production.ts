import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";

import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  validateStagePayload,
  validateWorkerEnvelope,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  computeRetryJitterMs,
  createBrokerConsumerMonitor,
  createRetryEnvelope,
  randomUuidMessageIdFactory,
  runtimeNow,
  runtimeTraceHeadersFromEnvelope,
  type BrokerConsumerHandle,
  type BrokerConsumerMonitor,
  type BrokerConsumerStatus,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport,
  type RuntimeClock,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  connect as amqpConnect,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
  type Options
} from "amqplib";
import {
  Pool,
  type PoolClient,
  type QueryResultRow
} from "pg";

import type { ApprovalConfig } from "./config.js";
import {
  ApprovalQwenError,
  type ApprovalBrokerOutbox,
  type ApprovalDatabaseTransaction,
  type ApprovalDatabaseTransactionRunner,
  type ApprovalDependencies,
  type ApprovalDependencyProbe,
  type ApprovalDecisionKey,
  type ApprovalEnrichmentRecord,
  type ApprovalEnrichmentRecordInput,
  type ApprovalPrompt,
  type ApprovalPromptRegistry,
  type ApprovalQwenClient,
  type ApprovalQwenRequest,
  type ApprovalStateStore,
  type ApprovalStoredDecision,
  type ApprovalTranslationPublication,
  type ApprovalWorkHandler
} from "./dependencies.js";
import { stableUuid } from "./ids.js";
import {
  APPROVAL_RECONCILIATION_CONFIRMATION,
  type ApprovalReconciliationCandidate,
  type ApprovalReconciliationReport,
  type ApprovalReconciliationRequest,
  type ApprovalReconciler
} from "./reconciliation.js";
import { LocalApprovalWorkHandler } from "./test-doubles.js";

const APPROVAL_SCHEMA = "worker_uplift_approval";
const DEFAULT_PROMPT_VERSION = "0.1.0";
const DEFAULT_CONFIRM_TIMEOUT_MS = WORKER_DELIVERY_BEHAVIOR.confirmTimeoutMs;
const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

export type ProductionApprovalDependencies = ApprovalDependencies & {
  readonly reconciler: ApprovalReconciler;
  readonly reconciliationToken?: string;
  close(): Promise<void>;
};

interface ProductionApprovalDependencyOptions {
  readonly config: ApprovalConfig;
  readonly clock: RuntimeClock;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly env?: NodeJS.ProcessEnv;
  readonly workHandler?: ApprovalWorkHandler;
}

interface PayloadCarrier {
  readonly envelope: WorkerMessageEnvelope;
  readonly payload: Readonly<Record<string, unknown>>;
}

interface PayloadConsumerRegistration {
  readonly handler: BrokerDeliveryHandler;
  readonly monitor: BrokerConsumerMonitor;
  consumerTag: string | undefined;
}

interface PgApprovalTransaction extends ApprovalDatabaseTransaction {
  readonly client: PoolClient;
}

interface LocalAiUsage {
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly totalTokens: number;
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;
type RabbitMqConnect = (url: string) => Promise<ChannelModel>;

export function createProductionApprovalDependencies(
  options: ProductionApprovalDependencyOptions
): ProductionApprovalDependencies {
  const env = options.env ?? process.env;
  const pool = new Pool({
    connectionString: requiredEnv(env, "NUTSNEWS_APPROVAL_DATABASE_URL"),
    max: Math.max(2, options.config.concurrency + 1),
    application_name: options.config.serviceName
  });
  const brokerTransport = new PayloadRabbitMqTransport({
    url: requiredEnv(env, "NUTSNEWS_APPROVAL_RABBITMQ_URL"),
    prefetch: options.config.prefetch,
    clock: options.clock,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    })
  });
  const stateStore = new PostgresApprovalStateStore(pool);
  const transactionRunner = new PostgresApprovalTransactionRunner(pool);
  const brokerOutbox = new PostgresApprovalBrokerOutbox(pool);
  const reconciler = new PostgresApprovalOutboxReconciler({
    pool,
    brokerTransport,
    clock: options.clock,
    env,
    config: options.config
  });
  const reconciliationToken = reconciliationTokenFromEnv(env);
  const qwenClient = new LocalAiApprovalQwenClient({
    baseUrl: requiredEnv(env, "NUTSNEWS_APPROVAL_QWEN_BASE_URL"),
    apiKey: requiredEnv(env, "NUTSNEWS_APPROVAL_QWEN_API_KEY"),
    clock: options.clock
  });
  const promptRegistry = new StaticApprovalPromptRegistry(options.config.qwen.promptId);

  return {
    clock: options.clock,
    stateStore,
    transactionRunner,
    brokerOutbox,
    reconciler,
    ...(reconciliationToken === undefined ? {} : {
      reconciliationToken
    }),
    brokerTransport,
    qwenClient,
    promptRegistry,
    workHandler: options.workHandler ?? new LocalApprovalWorkHandler(),
    async close(): Promise<void> {
      await brokerTransport.close();
      await pool.end();
    }
  };
}

export class PayloadRabbitMqTransport implements RuntimeBrokerTransport {
  readonly name = "rabbitmq-payload-transport";

  private readonly url: string;
  private readonly prefetchCount: number;
  private readonly clock: RuntimeClock;
  private readonly telemetry: RuntimeTelemetrySink | undefined;
  private readonly connectToBroker: RabbitMqConnect;
  private readonly consumers = new Map<WorkerStage, PayloadConsumerRegistration>();
  private readonly inFlight = new Set<Promise<void>>();
  private connection: ChannelModel | undefined;
  private channel: ConfirmChannel | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnecting = false;
  private routes: readonly WorkerRoute[] = [];
  private closing = false;

  constructor(options: {
    readonly url: string;
    readonly prefetch: number;
    readonly clock: RuntimeClock;
    readonly telemetry?: RuntimeTelemetrySink;
    readonly connect?: RabbitMqConnect;
  }) {
    this.url = options.url;
    this.prefetchCount = options.prefetch;
    this.clock = options.clock;
    this.telemetry = options.telemetry;
    this.connectToBroker = options.connect ?? amqpConnect;
  }

  get inFlightDeliveryCount(): number {
    return this.inFlight.size;
  }

  consumerStatus(stage: WorkerStage): BrokerConsumerStatus | undefined {
    return this.consumers.get(stage)?.monitor.status;
  }

  async connect(): Promise<void> {
    this.closing = false;
    await this.ensureChannel();
  }

  async assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.routes = routes;
    await this.ensureChannel();
  }

  async publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    const route = getWorkerRoute(command.envelope.route);
    const channel = await this.ensureChannel();

    await publishCarrierWithConfirm(channel, {
      carrier: {
        envelope: command.envelope,
        payload: command.payload
      },
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS
    });

    return {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: runtimeNow(this.clock)
    };
  }

  async consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    const channel = await this.ensureChannel();
    const existing = this.consumers.get(stage);

    if (existing?.consumerTag !== undefined) {
      existing.monitor.markClosed("consumer-replaced");
      await channel.cancel(existing.consumerTag).catch(() => undefined);
    }

    const registration: PayloadConsumerRegistration = {
      handler,
      consumerTag: undefined,
      monitor: createBrokerConsumerMonitor({
        stage,
        clock: this.clock,
        ...(this.telemetry === undefined ? {} : {
          telemetry: this.telemetry
        })
      })
    };
    this.consumers.set(stage, registration);
    await this.activateConsumer(stage, registration, channel);

    return {
      stage,
      cancel: async (): Promise<void> => {
        const registered = this.consumers.get(stage);
        this.consumers.delete(stage);
        registered?.monitor.markClosed("consumer-handle-cancelled");

        if (registered?.consumerTag !== undefined && this.channel !== undefined) {
          await this.channel.cancel(registered.consumerTag).catch(() => undefined);
        }
      }
    };
  }

  async drain(timeoutMs = DEFAULT_DRAIN_TIMEOUT_MS): Promise<void> {
    if (this.inFlight.size === 0) {
      return;
    }

    await Promise.race([
      Promise.all([...this.inFlight]).then(() => undefined),
      new Promise<void>((_, reject) => {
        setTimeout(() => {
          reject(new Error("Timed out waiting for RabbitMQ payload deliveries to drain."));
        }, timeoutMs);
      })
    ]);
  }

  async close(): Promise<void> {
    this.closing = true;
    this.clearReconnectTimer();
    const channel = this.channel;

    if (channel !== undefined) {
      for (const registration of this.consumers.values()) {
        if (registration.consumerTag !== undefined) {
          await channel.cancel(registration.consumerTag).catch(() => undefined);
        }
        registration.monitor.markClosed("transport-closing");
      }
    }

    this.consumers.clear();
    await this.drain().catch(() => undefined);

    if (this.channel !== undefined) {
      await this.channel.close().catch(() => undefined);
      this.channel = undefined;
    }

    if (this.connection !== undefined) {
      await this.connection.close().catch(() => undefined);
      this.connection = undefined;
    }
  }

  private async ensureChannel(): Promise<ConfirmChannel> {
    if (this.channel !== undefined) {
      return this.channel;
    }

    if (this.closing) {
      throw new Error("RabbitMQ payload transport is closing.");
    }

    const connection = await this.connectToBroker(this.url);
    const channel = await connection.createConfirmChannel();
    this.connection = connection;
    this.channel = channel;

    connection.on("close", () => {
      if (this.connection === connection) {
        this.connection = undefined;
      }

      this.markChannelClosed(channel, "connection-closed");
      this.scheduleConsumerReconnect();
    });
    connection.on("error", () => {
      if (this.connection === connection) {
        this.connection = undefined;
      }

      this.markChannelClosed(channel, "connection-error");
      this.scheduleConsumerReconnect();
    });
    channel.on("close", () => {
      this.markChannelClosed(channel, "channel-closed");
      this.scheduleConsumerReconnect();
    });
    channel.on("error", () => {
      this.markChannelClosed(channel, "channel-error");
      this.scheduleConsumerReconnect();
    });

    await this.restoreConsumers(channel);

    return channel;
  }

  private async restoreConsumers(channel: ConfirmChannel): Promise<void> {
    for (const [stage, registration] of this.consumers) {
      await this.activateConsumer(stage, registration, channel);
    }
  }

  private async activateConsumer(
    stage: WorkerStage,
    registration: PayloadConsumerRegistration,
    channel: ConfirmChannel
  ): Promise<void> {
    if (registration.consumerTag !== undefined) {
      return;
    }

    const route = getWorkerRoute(stage);
    registration.monitor.markRecovering("consumer-activation");
    await channel.prefetch(this.prefetchCount);
    const reply = await channel.consume(route.mainQueue.name, (message) => {
      if (message === null) {
        registration.consumerTag = undefined;
        registration.monitor.markCancelled("broker-cancelled-consumer");
        this.recoverCancelledConsumer(stage, registration, channel);
        return;
      }

      const tracked = this.handleDelivery(stage, registration.handler, message);
      this.inFlight.add(tracked);
      void tracked.finally(() => {
        this.inFlight.delete(tracked);
      });
    }, {
      noAck: false
    });

    registration.consumerTag = reply.consumerTag;
    registration.monitor.markActive("consumer-registered");
  }

  private markChannelClosed(channel: ConfirmChannel, reason: string): void {
    if (this.channel !== channel) {
      return;
    }

    this.channel = undefined;
    for (const registration of this.consumers.values()) {
      registration.consumerTag = undefined;
      registration.monitor.markChannelDropped(reason);
    }
  }

  private recoverCancelledConsumer(
    stage: WorkerStage,
    registration: PayloadConsumerRegistration,
    channel: ConfirmChannel
  ): void {
    if (this.closing || this.channel !== channel || this.consumers.get(stage) !== registration) {
      return;
    }

    void this.activateConsumer(stage, registration, channel).catch(() => {
      this.markChannelClosed(channel, "consumer-reactivation-failed");
      this.scheduleConsumerReconnect();
    });
  }

  private scheduleConsumerReconnect(): void {
    if (this.closing || this.consumers.size === 0 || this.reconnecting || this.reconnectTimer !== undefined) {
      return;
    }

    this.reconnectTimer = setTimeout(() => {
      void this.reconnectConsumers();
    }, 1_000);
    this.reconnectTimer.unref();
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
  }

  private async reconnectConsumers(): Promise<void> {
    this.reconnectTimer = undefined;

    if (this.closing || this.consumers.size === 0 || this.channel !== undefined) {
      return;
    }

    this.reconnecting = true;
    let retry = false;
    try {
      await this.ensureChannel();
    } catch {
      retry = true;
    } finally {
      this.reconnecting = false;
    }

    if (retry) {
      this.scheduleConsumerReconnect();
    }
  }

  private async handleDelivery(
    stage: WorkerStage,
    handler: BrokerDeliveryHandler,
    message: ConsumeMessage
  ): Promise<void> {
    const channel = this.channel;

    if (channel === undefined) {
      return;
    }

    try {
      const carrier = decodeCarrier(message);
      const result = await handler({
        envelope: carrier.envelope,
        payload: carrier.payload,
        receivedAt: runtimeNow(this.clock)
      });
      await this.settleDelivery(channel, message, carrier, result);
    } catch {
      channel.nack(message, false, false);
    }
  }

  private async settleDelivery(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    carrier: PayloadCarrier,
    result: RuntimeMessageProcessingResult
  ): Promise<void> {
    if (result.action === "ack") {
      channel.ack(message);
      return;
    }

    if (result.action === "retry") {
      const retryEnvelope = createRetryEnvelope(result.envelope, {
        now: runtimeNow(this.clock),
        messageIdFactory: randomUuidMessageIdFactory
      });
      const retryJitterMs = computeRetryJitterMs(result.destination.ttlMs, 0.1);
      await publishCarrierWithConfirm(channel, {
        carrier: {
          envelope: retryEnvelope,
          payload: carrier.payload
        },
        exchange: getWorkerRoute(result.envelope.route).retryExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS,
        retryJitterMs
      });
      channel.ack(message);
      return;
    }

    if (result.envelope !== undefined && result.destination !== undefined) {
      await publishCarrierWithConfirm(channel, {
        carrier: {
          envelope: result.envelope,
          payload: carrier.payload
        },
        exchange: getWorkerRoute(result.envelope.route).dlqExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS
      });
      channel.ack(message);
      return;
    }

    channel.nack(message, false, false);
  }
}

export class PostgresApprovalTransactionRunner implements ApprovalDatabaseTransactionRunner {
  readonly name = "postgres-approval-transactions";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<ApprovalDependencyProbe> {
    return probePool(this.pool, "approval transaction database ready");
  }

  async withTransaction<T>(operation: (transaction: ApprovalDatabaseTransaction) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    const transaction: PgApprovalTransaction = {
      transactionId: randomUUID(),
      client
    };

    try {
      await client.query("BEGIN");
      const value = await operation(transaction);
      await client.query("COMMIT");
      return value;
    } catch (error: unknown) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}

export class PostgresApprovalStateStore implements ApprovalStateStore {
  readonly name = "postgres-approval-state";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<ApprovalDependencyProbe> {
    return probePool(this.pool, "approval state database ready");
  }

  async claim(
    idempotencyKey: string,
    context: RuntimeIdempotencyClaimContext
  ): Promise<RuntimeIdempotencyClaimResult> {
    const inserted = await this.pool.query<{ readonly received_at: Date }>(
      `INSERT INTO ${APPROVAL_SCHEMA}.inbox (
        message_id, pipeline_run_id, stage_execution_id, source_stage, source_message_id,
        entity_kind, entity_id, schema_version, operation_version, idempotency_key,
        payload_ref, payload_digest, received_at, status, diagnostic_metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, 'processing', $14::jsonb)
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING received_at`,
      [
        context.envelope.messageId,
        context.envelope.correlationId,
        context.envelope.messageId,
        context.envelope.producer.name,
        context.envelope.causationId,
        context.envelope.aggregate.type,
        context.envelope.aggregate.id,
        context.envelope.schemaVersion,
        Math.max(1, context.envelope.aggregate.version),
        idempotencyKey,
        context.envelope.payloadRef.uri,
        context.envelope.payloadRef.digest ?? sha256Json(context.envelope.payloadRef),
        context.receivedAt,
        JSON.stringify({
          route: context.envelope.route,
          attempt: context.envelope.attempt
        })
      ]
    );

    if ((inserted.rowCount ?? 0) > 0) {
      return {
        status: "claimed",
        firstSeenAt: context.receivedAt,
        replay: false
      };
    }

    const existing = await this.pool.query<{
      readonly status: string;
      readonly received_at: Date;
      readonly processed_at: Date | null;
    }>(
      `SELECT status, received_at, processed_at
       FROM ${APPROVAL_SCHEMA}.inbox
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );
    const row = existing.rows[0];

    if (row === undefined) {
      return {
        status: "in-progress",
        firstSeenAt: context.receivedAt
      };
    }

    const firstSeenAt = row.received_at.toISOString();

    if (row.status === "processed" || row.status === "duplicate") {
      return {
        status: "already-completed",
        firstSeenAt,
        completedAt: (row.processed_at ?? row.received_at).toISOString()
      };
    }

    if (row.status === "failed" || row.status === "parked") {
      await this.pool.query(
        `UPDATE ${APPROVAL_SCHEMA}.inbox
         SET status = 'processing',
             sanitized_error_code = NULL,
             sanitized_error_message = NULL,
             diagnostic_metadata = diagnostic_metadata || $2::jsonb
         WHERE idempotency_key = $1`,
        [
          idempotencyKey,
          JSON.stringify({
            replayedAt: context.receivedAt,
            replayMessageId: context.envelope.messageId
          })
        ]
      );

      return {
        status: "claimed",
        firstSeenAt,
        replay: true
      };
    }

    return {
      status: "in-progress",
      firstSeenAt
    };
  }

  async markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    await this.pool.query(
      `UPDATE ${APPROVAL_SCHEMA}.inbox
       SET status = 'processed',
           processed_at = $2::timestamptz,
           diagnostic_metadata = diagnostic_metadata || $3::jsonb
       WHERE idempotency_key = $1`,
      [
        idempotencyKey,
        completion.completedAt,
        JSON.stringify({
          completedMessageId: completion.messageId,
          completedStage: completion.stage
        })
      ]
    );
  }

  async markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    await this.pool.query(
      `UPDATE ${APPROVAL_SCHEMA}.inbox
       SET status = 'failed',
           sanitized_error_code = $2,
           sanitized_error_message = $3,
           diagnostic_metadata = diagnostic_metadata || $4::jsonb
       WHERE idempotency_key = $1`,
      [
        idempotencyKey,
        sanitizeCode(failure.reason),
        sanitizeMessage(failure.reason),
        JSON.stringify({
          failedAt: failure.failedAt,
          failedMessageId: failure.messageId,
          retryable: failure.retryable
        })
      ]
    );
  }

  loadEnrichmentRecord(
    input: ApprovalEnrichmentRecordInput,
    transaction: ApprovalDatabaseTransaction
  ): Promise<ApprovalEnrichmentRecord> {
    void transaction;

    return Promise.resolve({
      candidateId: input.candidateId,
      canonicalArticleId: input.articleMetadataRef.canonicalArticleId,
      articleVersion: input.articleMetadataRef.articleVersion,
      canonicalUrl: input.canonicalUrl,
      imageStatus: input.imageStatus,
      ...(input.imageUrl === undefined ? {} : {
        imageUrl: input.imageUrl
      }),
      contentFingerprint: input.articleMetadataRef.contentFingerprint,
      title: input.articleMetadataRef.title ?? "NutsNews approved article",
      ...(input.articleMetadataRef.description === undefined ? {} : {
        description: input.articleMetadataRef.description
      }),
      ...(input.articleMetadataRef.publishedAt === undefined ? {} : {
        publishedAt: input.articleMetadataRef.publishedAt
      }),
      sourceLanguage: input.articleMetadataRef.language ?? "en",
      metadataRef: input.articleMetadataRef
    });
  }

  async findDecision(
    key: ApprovalDecisionKey,
    transaction: ApprovalDatabaseTransaction
  ): Promise<ApprovalStoredDecision | undefined> {
    const result = await transactionClient(transaction).query<ApprovalDecisionRow>(
      `SELECT article_identity_hash, approval_version, decision, positivity_score, ai_provider,
              ai_model, prompt_version, model_version, model_metadata, diagnostic_metadata, reviewed_at
       FROM ${APPROVAL_SCHEMA}.approval_decisions
       WHERE article_identity_hash = $1
         AND approval_version = $2
         AND prompt_version = $3
         AND ai_model = $4
       LIMIT 1`,
      [
        key.canonicalArticleId,
        key.articleVersion,
        promptVersionKey(key.promptId, key.promptVersion),
        key.model
      ]
    );

    return decisionFromRow(result.rows[0]);
  }

  async recordDecision(
    decision: ApprovalStoredDecision,
    transaction: ApprovalDatabaseTransaction
  ): Promise<ApprovalStoredDecision> {
    await transactionClient(transaction).query(
      `INSERT INTO ${APPROVAL_SCHEMA}.approval_decisions (
        article_identity_hash, approval_version, decision, positivity_score, ai_provider,
        ai_model, prompt_version, model_version, model_metadata, diagnostic_metadata, reviewed_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11::timestamptz)
      ON CONFLICT (article_identity_hash, approval_version)
      DO UPDATE SET decision = EXCLUDED.decision,
                    positivity_score = EXCLUDED.positivity_score,
                    ai_provider = EXCLUDED.ai_provider,
                    ai_model = EXCLUDED.ai_model,
                    prompt_version = EXCLUDED.prompt_version,
                    model_version = EXCLUDED.model_version,
                    model_metadata = EXCLUDED.model_metadata,
                    diagnostic_metadata = EXCLUDED.diagnostic_metadata,
                    reviewed_at = EXCLUDED.reviewed_at`,
      [
        decision.canonicalArticleId,
        decision.articleVersion,
        approvalDbDecision(decision.decision),
        decision.positivityScore,
        decision.provider,
        decision.model,
        promptVersionKey(decision.promptId, decision.promptVersion),
        decision.model,
        JSON.stringify({
          promptId: decision.promptId,
          promptVersion: decision.promptVersion,
          confidenceScore: decision.confidenceScore,
          qualityScore: decision.qualityScore,
          sourceLanguage: decision.sourceLanguage,
          latencyMs: decision.latencyMs,
          summaryRef: decision.summaryRef,
          aiUsageRef: decision.aiUsageRef
        }),
        JSON.stringify({
          decisionId: decision.decisionId,
          candidateId: decision.candidateId,
          canonicalUrl: decision.canonicalUrl,
          contentFingerprint: decision.contentFingerprint,
          rejectionReason: decision.rejectionReason,
          reviewRef: decision.reviewRef,
          sourceMessageId: decision.sourceMessageId,
          correlationId: decision.correlationId,
          traceparent: decision.traceparent,
          translationPublication: decision.translationPublication,
          decisionSnapshot: decision
        }),
        decision.decidedAt
      ]
    );

    return decision;
  }

  async markTranslationPublished(
    decisionId: string,
    publication: ApprovalTranslationPublication,
    transaction: ApprovalDatabaseTransaction
  ): Promise<ApprovalStoredDecision> {
    const result = await transactionClient(transaction).query<ApprovalDecisionRow>(
      `SELECT article_identity_hash, approval_version, decision, positivity_score, ai_provider,
              ai_model, prompt_version, model_version, model_metadata, diagnostic_metadata, reviewed_at
       FROM ${APPROVAL_SCHEMA}.approval_decisions
       WHERE diagnostic_metadata->>'decisionId' = $1
       LIMIT 1`,
      [decisionId]
    );
    const existing = decisionFromRow(result.rows[0]);

    if (existing === undefined) {
      throw new Error(`Approval decision ${decisionId} is not recorded.`);
    }

    const updated = {
      ...existing,
      translationPublication: publication
    } satisfies ApprovalStoredDecision;

    return this.recordDecision(updated, transaction);
  }
}

export class PostgresApprovalBrokerOutbox implements ApprovalBrokerOutbox {
  readonly name = "postgres-approval-outbox";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<ApprovalDependencyProbe> {
    return probePool(this.pool, "approval outbox database ready");
  }

  async record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    const payload = command.payload;

    await this.pool.query(
      `INSERT INTO ${APPROVAL_SCHEMA}.outbox (
        outbox_message_id, pipeline_run_id, stage_execution_id, destination_stage, routing_key,
        entity_kind, entity_id, schema_version, operation_version, idempotency_key,
        payload_ref, payload_digest, published_at, confirmed_at, status, diagnostic_metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, $14::timestamptz, 'confirmed', $15::jsonb)
      ON CONFLICT (idempotency_key)
      DO UPDATE SET confirmed_at = EXCLUDED.confirmed_at,
                    status = 'confirmed',
                    diagnostic_metadata = ${APPROVAL_SCHEMA}.outbox.diagnostic_metadata || EXCLUDED.diagnostic_metadata`,
      [
        receipt.messageId,
        stringFrom(payload.pipelineRunId, command.envelope.correlationId),
        stringFrom(payload.stageExecutionId, command.envelope.messageId),
        command.envelope.route,
        receipt.routingKey,
        command.envelope.aggregate.type,
        command.envelope.aggregate.id,
        command.envelope.schemaVersion,
        Math.max(1, command.envelope.aggregate.version),
        command.envelope.idempotencyKey,
        command.envelope.payloadRef.uri,
        command.envelope.payloadRef.digest ?? sha256Json(payload),
        receipt.confirmedAt,
        receipt.confirmedAt,
        JSON.stringify({
          envelope: command.envelope,
          exchange: receipt.exchange,
          payload,
          payloadSchemaId: payload.schemaId
        })
      ]
    );
  }
}

interface ApprovalOutboxReconcilerOptions {
  readonly pool: Pool;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly clock: RuntimeClock;
  readonly env: NodeJS.ProcessEnv;
  readonly config: ApprovalConfig;
}

interface ApprovalOutboxRow extends QueryResultRow {
  readonly id: string | number;
  readonly outbox_message_id: string;
  readonly pipeline_run_id: string;
  readonly stage_execution_id: string;
  readonly destination_stage: string;
  readonly routing_key: string;
  readonly entity_kind: string;
  readonly entity_id: string;
  readonly schema_version: number;
  readonly operation_version: number;
  readonly idempotency_key: string;
  readonly payload_ref: string;
  readonly payload_digest: string;
  readonly created_at: Date;
  readonly published_at: Date | null;
  readonly confirmed_at: Date | null;
  readonly status: string;
  readonly diagnostic_metadata: unknown;
}

interface HydratedReplay {
  readonly row: ApprovalOutboxRow;
  readonly candidate: ApprovalReconciliationCandidate;
  readonly command: BrokerPublishCommand;
}

export class PostgresApprovalOutboxReconciler implements ApprovalReconciler {
  readonly name = "postgres-approval-outbox-reconciler";

  constructor(private readonly options: ApprovalOutboxReconcilerOptions) {}

  async reconcile(request: ApprovalReconciliationRequest): Promise<ApprovalReconciliationReport> {
    const requestedAt = runtimeNow(this.options.clock);
    const mode = request.mode === "apply" ? "apply" : "dry-run";
    const maxItems = boundedInteger(request.maxItems, 100, 1, 100);
    const minAgeSeconds = boundedInteger(request.minAgeSeconds, 900, 0, 86_400);
    const reason = safeReason(request.reason);
    const runId = safeRunId(request.runId);

    if (this.killSwitchActive()) {
      return report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "kill_switch_active",
        errors: [
          "approval reconciliation stop switch is active"
        ],
        candidates: []
      });
    }

    if (mode === "apply") {
      const applyError = this.applyGateError(request, runId);

      if (applyError !== undefined) {
        return report({
          mode,
          requestedAt,
          runId,
          reason,
          maxItems,
          minAgeSeconds,
          status: "failed_closed",
          errors: [
            applyError
          ],
          candidates: []
        });
      }
    }

    const rows = await this.selectCandidates(maxItems, minAgeSeconds, runId);
    const hydrated = await Promise.all(rows.map((row) => this.hydrate(row, requestedAt)));
    const failed = hydrated.filter((candidate): candidate is ApprovalReconciliationCandidate => "status" in candidate);

    if (failed.length > 0) {
      return report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "failed_closed",
        errors: failed.map((candidate) => `${candidate.outboxId}:${candidate.failedClosedReason ?? "unrecoverable-payload"}`),
        candidates: failed
      });
    }

    const replayable = hydrated as HydratedReplay[];

    if (mode === "dry-run") {
      return report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "dry_run",
        candidates: replayable.map((item) => item.candidate),
        errors: []
      });
    }

    const replayed: ApprovalReconciliationCandidate[] = [];

    for (const item of replayable) {
      if (this.killSwitchActive()) {
        return report({
          mode,
          requestedAt,
          runId,
          reason,
          maxItems,
          minAgeSeconds,
          status: "kill_switch_active",
          candidates: replayed,
          errors: [
            "approval reconciliation stop switch became active"
          ]
        });
      }

      const receipt = await this.options.brokerTransport.publish(item.command);
      await this.recordReplay(item.row, item.command, receipt, requestedAt, runId ?? "untracked", reason);
      replayed.push({
        ...item.candidate,
        status: "replayed",
        replayMessageId: receipt.messageId
      });
    }

    return report({
      mode,
      requestedAt,
      runId,
      reason,
      maxItems,
      minAgeSeconds,
      status: "applied",
      candidates: replayed,
      errors: []
    });
  }

  private async selectCandidates(maxItems: number, minAgeSeconds: number, runId: string | undefined): Promise<readonly ApprovalOutboxRow[]> {
    const result = await this.options.pool.query<ApprovalOutboxRow>(
      `SELECT id, outbox_message_id, pipeline_run_id, stage_execution_id, destination_stage, routing_key,
              entity_kind, entity_id, schema_version, operation_version, idempotency_key,
              payload_ref, payload_digest, created_at, published_at, confirmed_at, status, diagnostic_metadata
       FROM ${APPROVAL_SCHEMA}.outbox
       WHERE status = 'confirmed'
         AND confirmed_at IS NOT NULL
         AND created_at <= now() - ($2::integer * interval '1 second')
         AND ($3::text IS NULL OR diagnostic_metadata->>'lastReconciliationRunId' IS DISTINCT FROM $3::text)
       ORDER BY created_at ASC, id ASC
       LIMIT $1`,
      [
        maxItems,
        minAgeSeconds,
        runId ?? null
      ]
    );

    return result.rows;
  }

  private async hydrate(row: ApprovalOutboxRow, requestedAt: string): Promise<HydratedReplay | ApprovalReconciliationCandidate> {
    const baseCandidate = candidateFromRow(row, "confirmed-outbox-replay");
    const diagnostic = objectValue(row.diagnostic_metadata);
    const reconstructed = await this.reconstructMissingCarrier(row, requestedAt);
    const payload = reconstructed.payload ?? diagnostic.payload;
    const envelope = reconstructed.envelope ?? diagnostic.envelope;

    if (!isRecord(payload)) {
      return failedCandidate(baseCandidate, "missing-stored-payload");
    }

    if (!isRecord(envelope)) {
      return failedCandidate(baseCandidate, "missing-stored-envelope");
    }

    if (row.payload_ref !== stringFrom(objectValue(envelope.payloadRef).uri, "")) {
      return failedCandidate(baseCandidate, "payload-ref-mismatch");
    }

    if (row.payload_digest !== sha256Json(payload)) {
      return failedCandidate(baseCandidate, "payload-digest-mismatch");
    }

    const payloadValidation = validateStagePayload(payload);

    if (!payloadValidation.ok) {
      return failedCandidate(baseCandidate, `invalid-stored-payload:${payloadValidation.issues[0]?.code ?? "unknown"}`);
    }

    const replayMessageId = randomUUID();
    const attempt = objectValue(envelope.attempt);
    const replayEnvelope = {
      ...envelope,
      messageId: replayMessageId,
      occurredAt: requestedAt,
      attempt: {
        ...attempt,
        lastAttemptAt: requestedAt
      }
    };
    const envelopeValidation = validateWorkerEnvelope(replayEnvelope);

    if (!envelopeValidation.ok) {
      return failedCandidate(baseCandidate, `invalid-stored-envelope:${envelopeValidation.issues[0]?.code ?? "unknown"}`);
    }

    if (envelopeValidation.value.route !== row.destination_stage) {
      return failedCandidate(baseCandidate, "destination-stage-mismatch");
    }

    return {
      row,
      candidate: {
        ...baseCandidate,
        replayMessageId
      },
      command: {
        envelope: envelopeValidation.value,
        payload
      }
    };
  }

  private async reconstructMissingCarrier(
    row: ApprovalOutboxRow,
    requestedAt: string
  ): Promise<{ readonly payload?: Readonly<Record<string, unknown>>; readonly envelope?: WorkerMessageEnvelope }> {
    const diagnostic = objectValue(row.diagnostic_metadata);

    if (isRecord(diagnostic.payload)
      && isRecord(diagnostic.envelope)
      && row.payload_digest === sha256Json(diagnostic.payload)) {
      return {};
    }

    const decisionId = approvalDecisionIdFromOutboxRow(row);

    if (decisionId === undefined) {
      return {};
    }

    const decision = await this.findReplayDecision(decisionId);

    if (decision === undefined
      || row.destination_stage !== "translation"
      || row.entity_kind !== "article"
      || row.entity_id !== decision.canonicalArticleId
      || row.operation_version !== decision.articleVersion
      || row.idempotency_key !== `approval:translation:${decision.decisionId}`) {
      return {};
    }

    const expectedPayloadRef = approvalTranslationPayloadRef(decision);

    if (row.payload_ref !== expectedPayloadRef) {
      return {};
    }

    const stageExecutionId = stableUuid([
      "translation-stage-execution",
      decision.decisionId
    ]);
    const payload = {
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
      schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
      pipelineRunId: row.pipeline_run_id,
      stageExecutionId,
      sourceMessageId: decision.sourceMessageId,
      idempotencyKey: row.idempotency_key,
      traceparent: decision.traceparent,
      producedAt: decision.decidedAt,
      articleId: decision.canonicalArticleId,
      sourceLanguage: decision.sourceLanguage,
      targetLanguages: this.options.config.targetLanguages,
      reason: "new_article",
      existingLanguageCodes: []
    };

    if (row.stage_execution_id !== stageExecutionId || row.payload_digest !== sha256Json(payload)) {
      return {};
    }

    const payloadValidation = validateStagePayload(payload);

    if (!payloadValidation.ok) {
      return {};
    }

    const route = getWorkerRoute("translation");
    const envelope = assertReplayEnvelope({
      schemaId: route.schemaId,
      schemaVersion: row.schema_version,
      route: "translation",
      messageId: row.outbox_message_id,
      causationId: decision.sourceMessageId,
      correlationId: decision.correlationId,
      traceparent: decision.traceparent,
      idempotencyKey: row.idempotency_key,
      aggregate: {
        type: "article",
        id: decision.canonicalArticleId,
        version: decision.articleVersion
      },
      occurredAt: decision.decidedAt,
      attempt: {
        count: 1,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: decision.decidedAt,
        lastAttemptAt: requestedAt
      },
      producer: {
        name: this.options.config.serviceName,
        version: this.options.config.serviceVersion
      },
      payloadRef: {
        kind: "backend-record",
        uri: expectedPayloadRef,
        mediaType: "application/json",
        sizeBytes: getStagePayloadSizeBytes(payload)
      }
    });

    if (envelope === undefined) {
      return {};
    }

    return {
      payload,
      envelope
    };
  }

  private async findReplayDecision(decisionId: string): Promise<ApprovalStoredDecision | undefined> {
    const result = await this.options.pool.query<ApprovalDecisionRow>(
      `SELECT article_identity_hash, approval_version, decision, positivity_score, ai_provider,
              ai_model, prompt_version, model_version, model_metadata, diagnostic_metadata, reviewed_at
       FROM ${APPROVAL_SCHEMA}.approval_decisions
       WHERE diagnostic_metadata->>'decisionId' = $1
       LIMIT 1`,
      [
        decisionId
      ]
    );

    const snapshot = objectValue(result.rows[0]?.diagnostic_metadata).decisionSnapshot;

    return isReplayableApprovalDecisionSnapshot(snapshot) ? snapshot : undefined;
  }

  private async recordReplay(
    row: ApprovalOutboxRow,
    command: BrokerPublishCommand,
    receipt: BrokerPublishReceipt,
    requestedAt: string,
    runId: string,
    reason: string | undefined
  ): Promise<void> {
    const audit = {
      event: "approval.outbox.replayed",
      runId,
      reason: reason ?? "unspecified",
      originalMessageId: row.outbox_message_id,
      replayMessageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      correlationId: command.envelope.correlationId,
      causationId: command.envelope.causationId,
      articleId: command.envelope.aggregate.id,
      articleVersion: command.envelope.aggregate.version,
      replayedAt: requestedAt,
      exchange: receipt.exchange,
      routingKey: receipt.routingKey
    };

    await this.options.pool.query(
      `UPDATE ${APPROVAL_SCHEMA}.outbox
       SET diagnostic_metadata =
         jsonb_set(
           diagnostic_metadata || $2::jsonb,
           '{reconciliationAuditHistory}',
           coalesce(diagnostic_metadata->'reconciliationAuditHistory', '[]'::jsonb) || $3::jsonb,
           true
         )
       WHERE id = $1`,
      [
        row.id,
        JSON.stringify({
          lastReconciliationRunId: runId,
          reconciliationLastReplayAt: requestedAt,
          reconciliationLastReplayMessageId: command.envelope.messageId
        }),
        JSON.stringify([
          audit
        ])
      ]
    );
  }

  private applyGateError(request: ApprovalReconciliationRequest, runId: string | undefined): string | undefined {
    if (!this.applyEnabled()) {
      return "approval reconciliation apply is disabled by configuration";
    }

    if (request.protectedConfirmation !== APPROVAL_RECONCILIATION_CONFIRMATION) {
      return `protectedConfirmation must be ${APPROVAL_RECONCILIATION_CONFIRMATION}`;
    }

    if (runId === undefined) {
      return "runId is required for apply";
    }

    return undefined;
  }

  private applyEnabled(): boolean {
    return flagEnabled(this.options.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_APPLY_ENABLED)
      || flagEnabled(this.options.env.NUTSNEWS_APPROVAL_RECONCILIATION_APPLY_ENABLED);
  }

  private killSwitchActive(): boolean {
    return flagEnabled(this.options.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_STOP)
      || flagEnabled(this.options.env.NUTSNEWS_APPROVAL_RECONCILIATION_STOP);
  }
}

export class LocalAiApprovalQwenClient implements ApprovalQwenClient {
  readonly name = "local-ai-approval-client";

  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly clock: RuntimeClock;
  private readonly fetcher: FetchLike;

  constructor(options: {
    readonly baseUrl: string;
    readonly apiKey: string;
    readonly clock: RuntimeClock;
    readonly fetcher?: FetchLike;
  }) {
    this.baseUrl = stripTrailingSlashes(options.baseUrl);
    this.apiKey = options.apiKey;
    this.clock = options.clock;
    this.fetcher = options.fetcher ?? fetch;
  }

  async probe(): Promise<ApprovalDependencyProbe> {
    try {
      const response = await this.fetcher(`${this.baseUrl}/health`, {
        method: "GET",
        signal: AbortSignal.timeout(5_000)
      });

      return response.ok
        ? {
            status: "ok",
            summary: "local AI approval endpoint ready"
          }
        : {
            status: "unhealthy",
            summary: `local AI health returned ${String(response.status)}`
          };
    } catch (error: unknown) {
      return {
        status: "unhealthy",
        summary: error instanceof Error ? error.message : "local AI health failed"
      };
    }
  }

  async review(request: ApprovalQwenRequest): Promise<unknown> {
    const apiKey = safeHeaderValue(this.apiKey);

    if (apiKey === undefined) {
      throw new ApprovalQwenError("qwen-unauthorized", {
        retryable: false
      });
    }

    const startedAtMs = this.clock.now().getTime();
    let response: Response;

    try {
      response = await this.fetcher(`${this.baseUrl}/review`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-nutsnews-ai-key": apiKey
        },
        body: JSON.stringify({
          model: request.model,
          source: "NutsNews worker uplift",
          title: request.input.title,
          excerpt: request.input.description ?? request.input.title,
          url: request.input.canonicalUrl
        }),
        signal: AbortSignal.timeout(request.timeoutMs)
      });
    } catch (error: unknown) {
      if (error instanceof Error && error.name === "TimeoutError") {
        throw new ApprovalQwenError("qwen-timeout", {
          retryable: true
        });
      }

      throw new ApprovalQwenError("qwen-model-error", {
        retryable: true
      });
    }

    if (!response.ok) {
      throw approvalErrorFromStatus(response);
    }

    const raw = await response.json();

    return mapLocalAiReview(raw, request, Math.max(0, this.clock.now().getTime() - startedAtMs));
  }
}

export class StaticApprovalPromptRegistry implements ApprovalPromptRegistry {
  readonly name = "static-approval-prompt-registry";

  constructor(private readonly promptId: string) {}

  probe(): ApprovalDependencyProbe {
    return {
      status: "ok",
      summary: "static approval prompt registry ready"
    };
  }

  getPrompt(id: string): Promise<ApprovalPrompt> {
    if (id !== this.promptId) {
      return Promise.reject(new Error(`Unknown approval prompt ${id}.`));
    }

    return Promise.resolve({
      id,
      version: DEFAULT_PROMPT_VERSION,
      purpose: "editorial-approval",
      instructions: "Return a structured NutsNews approval decision using local AI without logging prompt or article bodies."
    });
  }
}

interface ApprovalDecisionRow extends QueryResultRow {
  readonly article_identity_hash: string;
  readonly approval_version: number;
  readonly decision: string;
  readonly positivity_score: string | number | null;
  readonly ai_provider: string | null;
  readonly ai_model: string | null;
  readonly prompt_version: string | null;
  readonly model_version: string | null;
  readonly model_metadata: unknown;
  readonly diagnostic_metadata: unknown;
  readonly reviewed_at: Date;
}

async function probePool(pool: Pool, summary: string): Promise<ApprovalDependencyProbe> {
  try {
    await pool.query("SELECT 1");

    return {
      status: "ok",
      summary
    };
  } catch (error: unknown) {
    return {
      status: "unhealthy",
      summary: error instanceof Error ? error.message : "database probe failed"
    };
  }
}

function transactionClient(transaction: ApprovalDatabaseTransaction): PoolClient {
  const client = (transaction as Partial<PgApprovalTransaction>).client;

  if (client === undefined) {
    throw new Error("Approval operation requires a Postgres transaction.");
  }

  return client;
}

async function publishCarrierWithConfirm(
  channel: ConfirmChannel,
  options: {
    readonly carrier: PayloadCarrier;
    readonly exchange: string;
    readonly routingKey: string;
    readonly confirmTimeoutMs: number;
    readonly retryJitterMs?: number;
  }
): Promise<void> {
  const content = Buffer.from(JSON.stringify(options.carrier), "utf8");
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let settled = false;

  await new Promise<void>((resolve, reject) => {
    const cleanup = (): void => {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }

      channel.off("return", onReturn);
      channel.off("close", onClose);
      channel.off("error", onChannelError);
      settled = true;
    };
    const fail = (error: Error): void => {
      if (settled) {
        return;
      }

      cleanup();
      reject(error);
    };
    const onReturn = (returned: unknown): void => {
      if (returnedMessageId(returned) === options.carrier.envelope.messageId) {
        fail(new Error(`RabbitMQ publish was returned for ${options.exchange}:${options.routingKey}.`));
      }
    };
    const onClose = (): void => {
      fail(new Error("RabbitMQ channel closed during publish."));
    };
    const onChannelError = (): void => {
      fail(new Error("RabbitMQ channel errored during publish."));
    };

    timeout = setTimeout(() => {
      fail(new Error("RabbitMQ publish confirm timed out."));
    }, options.confirmTimeoutMs);

    channel.on("return", onReturn);
    channel.on("close", onClose);
    channel.on("error", onChannelError);
    channel.publish(
      options.exchange,
      options.routingKey,
      content,
      publishOptions(options.carrier.envelope, options.retryJitterMs),
      (error: unknown) => {
        if (error !== null && error !== undefined) {
          fail(error instanceof Error ? error : new Error("RabbitMQ publish confirm failed."));
          return;
        }

        if (!settled) {
          cleanup();
          resolve();
        }
      }
    );
  });
}

function decodeCarrier(message: ConsumeMessage): PayloadCarrier {
  const parsed = JSON.parse(message.content.toString("utf8")) as unknown;

  if (isRecord(parsed) && isRecord(parsed.envelope)) {
    return {
      envelope: parsed.envelope as unknown as WorkerMessageEnvelope,
      payload: isRecord(parsed.payload) ? parsed.payload : {}
    };
  }

  if (!isRecord(parsed)) {
    throw new Error("RabbitMQ message body must be a JSON object.");
  }

  return {
    envelope: parsed as unknown as WorkerMessageEnvelope,
    payload: {}
  };
}

function publishOptions(envelope: WorkerMessageEnvelope, retryJitterMs: number | undefined): Options.Publish {
  return {
    persistent: true,
    mandatory: true,
    contentType: WORKER_DELIVERY_BEHAVIOR.contentType,
    contentEncoding: WORKER_DELIVERY_BEHAVIOR.contentEncoding,
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    timestamp: Math.floor(Date.parse(envelope.occurredAt) / 1_000),
    headers: {
      schemaId: envelope.schemaId,
      schemaVersion: envelope.schemaVersion,
      route: envelope.route,
      attemptCount: envelope.attempt.count,
      idempotencyKey: envelope.idempotencyKey,
      payloadCarrier: "envelope-plus-payload",
      ...runtimeTraceHeadersFromEnvelope(envelope),
      ...(retryJitterMs === undefined ? {} : {
        retryJitterMs
      })
    }
  };
}

function returnedMessageId(returned: unknown): string | undefined {
  if (!isRecord(returned) || !isRecord(returned.properties)) {
    return undefined;
  }

  const messageId = returned.properties.messageId;

  return typeof messageId === "string" ? messageId : undefined;
}

function decisionFromRow(row: ApprovalDecisionRow | undefined): ApprovalStoredDecision | undefined {
  if (row === undefined) {
    return undefined;
  }

  const diagnostic = objectValue(row.diagnostic_metadata);
  const snapshot = diagnostic.decisionSnapshot;

  if (isApprovalDecisionSnapshot(snapshot)) {
    return snapshot;
  }

  const modelMetadata = objectValue(row.model_metadata);
  const promptParts = (row.prompt_version ?? "unknown:0.0.0").split(":");
  const promptId = stringFrom(modelMetadata.promptId, promptParts[0] ?? "unknown");
  const promptVersion = stringFrom(modelMetadata.promptVersion, promptParts[1] ?? DEFAULT_PROMPT_VERSION);
  const model = row.ai_model ?? row.model_version ?? "unknown";
  const decisionId = stringFrom(diagnostic.decisionId, stableUuid([
    row.article_identity_hash,
    String(row.approval_version),
    promptId,
    promptVersion,
    model
  ]));
  const traceparent = stringFrom(diagnostic.traceparent, "00-00000000000000000000000000000000-0000000000000000-00");
  const sourceMessageId = stringFrom(diagnostic.sourceMessageId, decisionId);

  return {
    decisionId,
    candidateId: stringFrom(diagnostic.candidateId, row.article_identity_hash),
    canonicalArticleId: row.article_identity_hash,
    articleVersion: row.approval_version,
    canonicalUrl: stringFrom(diagnostic.canonicalUrl, "https://example.invalid/approval-shadow"),
    decision: approvalRuntimeDecision(row.decision),
    ...(typeof diagnostic.rejectionReason === "string" ? {
      rejectionReason: diagnostic.rejectionReason
    } : {}),
    provider: approvalProvider(row.ai_provider),
    model,
    promptId,
    promptVersion,
    positivityScore: Number(row.positivity_score ?? 0),
    confidenceScore: numberFrom(modelMetadata.confidenceScore, 0),
    qualityScore: numberFrom(modelMetadata.qualityScore, 0),
    sourceLanguage: stringFrom(modelMetadata.sourceLanguage, "en"),
    contentFingerprint: stringFrom(diagnostic.contentFingerprint, row.article_identity_hash),
    reviewRef: reviewRefFromDiagnostic(diagnostic, decisionId, row.article_identity_hash, row.approval_version, promptId, promptVersion, model, traceparent, sourceMessageId),
    ...(isApprovalSummaryRef(modelMetadata.summaryRef) ? {
      summaryRef: modelMetadata.summaryRef
    } : {}),
    ...(isApprovalAiUsageRef(modelMetadata.aiUsageRef) ? {
      aiUsageRef: modelMetadata.aiUsageRef
    } : {}),
    sourceMessageId,
    correlationId: stringFrom(diagnostic.correlationId, sourceMessageId),
    traceparent,
    latencyMs: numberFrom(modelMetadata.latencyMs, 0),
    decidedAt: row.reviewed_at.toISOString(),
    ...(isTranslationPublication(diagnostic.translationPublication) ? {
      translationPublication: diagnostic.translationPublication
    } : {})
  };
}

function reviewRefFromDiagnostic(
  diagnostic: Readonly<Record<string, unknown>>,
  decisionId: string,
  canonicalArticleId: string,
  articleVersion: number,
  promptId: string,
  promptVersion: string,
  model: string,
  traceparent: string,
  sourceMessageId: string
): ApprovalStoredDecision["reviewRef"] {
  if (isRecord(diagnostic.reviewRef)) {
    return diagnostic.reviewRef as ApprovalStoredDecision["reviewRef"];
  }

  return {
    kind: "backend-record",
    uri: `backend://worker-uplift/approval/${encodeURIComponent(canonicalArticleId)}/${decisionId}/review`,
    mediaType: "application/json",
    decisionId,
    canonicalArticleId,
    articleVersion,
    promptId,
    promptVersion,
    model,
    traceparent,
    sourceMessageId
  };
}

function mapLocalAiReview(raw: unknown, request: ApprovalQwenRequest, latencyMs: number): unknown {
  if (!isRecord(raw)) {
    return raw;
  }

  const localDecision = stringFrom(raw.decision, "reject").toLowerCase();
  const accepted = localDecision === "accept" || localDecision === "accepted";
  const usage = usageFromLocalAi(raw);

  return {
    decision: accepted ? "accepted" : "rejected",
    reasonCode: normalizeReasonCode(raw.category ?? raw.reason, accepted ? "newsworthy" : "not-newsworthy"),
    confidenceScore: accepted ? 92 : 88,
    qualityScore: accepted ? 90 : 85,
    positivityScore: localPositivityScore(raw.positivity_score ?? raw.positivityScore),
    ...(accepted ? {
      summary: stringFrom(raw.summary, `${request.input.title} is an uplifting NutsNews story with useful context for readers.`)
    } : {}),
    latencyMs: numberFrom(raw.duration_ms, latencyMs),
    ...(usage === undefined ? {} : {
      usage
    })
  };
}

function approvalErrorFromStatus(response: Response): ApprovalQwenError {
  if (response.status === 401 || response.status === 403) {
    return new ApprovalQwenError("qwen-unauthorized", {
      retryable: false
    });
  }

  if (response.status === 408) {
    return new ApprovalQwenError("qwen-timeout", {
      retryable: true
    });
  }

  if (response.status === 429) {
    const retryAfter = retryAfterMs(response);

    return new ApprovalQwenError("qwen-rate-limited", retryAfter === undefined
      ? {
          retryable: true
        }
      : {
          retryable: true,
          retryAfterMs: retryAfter
        });
  }

  return new ApprovalQwenError("qwen-model-error", {
    retryable: response.status >= 500
  });
}

function retryAfterMs(response: Response): number | undefined {
  const value = response.headers.get("retry-after");

  if (value === null) {
    return undefined;
  }

  const seconds = Number(value);

  return Number.isFinite(seconds) && seconds > 0 ? Math.round(seconds * 1_000) : undefined;
}

function usageFromLocalAi(raw: Readonly<Record<string, unknown>>): LocalAiUsage | undefined {
  const inputTokens = nonNegativeInteger(raw.prompt_tokens) ? raw.prompt_tokens : 0;
  const outputTokens = nonNegativeInteger(raw.completion_tokens) ? raw.completion_tokens : 0;
  const totalTokens = nonNegativeInteger(raw.total_tokens) ? raw.total_tokens : inputTokens + outputTokens;

  if (inputTokens === 0 && outputTokens === 0 && totalTokens === 0) {
    return undefined;
  }

  return {
    inputTokens,
    outputTokens,
    totalTokens
  };
}

function approvalDbDecision(decision: ApprovalStoredDecision["decision"]): "approved" | "rejected" | "needs_review" {
  if (decision === "accepted") {
    return "approved";
  }

  if (decision === "rejected") {
    return "rejected";
  }

  return "needs_review";
}

function approvalRuntimeDecision(value: string): ApprovalStoredDecision["decision"] {
  if (value === "approved") {
    return "accepted";
  }

  if (value === "rejected") {
    return "rejected";
  }

  return "permanent_failure";
}

function approvalProvider(value: string | null): ApprovalStoredDecision["provider"] {
  if (value === "prefilter" || value === "legacy_openai_fallback") {
    return value;
  }

  return "local_ai";
}

function promptVersionKey(promptId: string, promptVersion: string): string {
  return `${promptId}:${promptVersion}`;
}

function sha256Json(value: unknown): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

function requiredEnv(env: NodeJS.ProcessEnv, key: string): string {
  const value = env[key]?.trim();

  if (value === undefined || value.length === 0) {
    throw new Error(`${key} is required for production approval dependencies.`);
  }

  return value;
}

function reconciliationTokenFromEnv(env: NodeJS.ProcessEnv): string | undefined {
  const directToken = optionalEnv(env, "NUTSNEWS_APPROVAL_RECONCILIATION_TOKEN")
    ?? optionalEnv(env, "NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN");

  if (directToken !== undefined) {
    return directToken;
  }

  const tokenFile = optionalEnv(env, "NUTSNEWS_APPROVAL_RECONCILIATION_TOKEN_FILE")
    ?? optionalEnv(env, "NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN_FILE");

  if (tokenFile === undefined) {
    return undefined;
  }

  const value = readFileSync(tokenFile, "utf8").trim();

  return value.length > 0 ? value : undefined;
}

function optionalEnv(env: NodeJS.ProcessEnv, key: string): string | undefined {
  const value = env[key]?.trim();

  return value === undefined || value.length === 0 ? undefined : value;
}

function report(input: {
  readonly mode: ApprovalReconciliationRequest["mode"];
  readonly requestedAt: string;
  readonly runId?: string | undefined;
  readonly reason?: string | undefined;
  readonly maxItems: number;
  readonly minAgeSeconds: number;
  readonly status: ApprovalReconciliationReport["status"];
  readonly candidates: readonly ApprovalReconciliationCandidate[];
  readonly errors: readonly string[];
}): ApprovalReconciliationReport {
  const replayedCount = input.candidates.filter((candidate) => candidate.status === "replayed").length;
  const failedClosedCount = input.candidates.filter((candidate) => candidate.status === "failed_closed").length;
  const skippedCount = input.status === "failed_closed"
    ? Math.max(0, input.candidates.length - failedClosedCount)
    : 0;
  const base = {
    service: "approval",
    mode: input.mode,
    status: input.status,
    requestedAt: input.requestedAt,
    maxItems: input.maxItems,
    minAgeSeconds: input.minAgeSeconds,
    selectedCount: input.candidates.length,
    replayedCount,
    failedClosedCount,
    skippedCount,
    writesPerformed: replayedCount > 0,
    dryRun: input.mode === "dry-run",
    productionVisibilityEnabled: false,
    legacyRuntimeRequired: false,
    protectedApplyRequired: true,
    candidates: input.candidates,
    errors: input.errors,
    metrics: {
      candidateCount: input.candidates.length,
      replayedCount,
      failedClosedCount,
      skippedCount
    }
  } satisfies Omit<ApprovalReconciliationReport, "runId" | "reason">;

  return {
    ...base,
    ...(input.runId === undefined ? {} : {
      runId: input.runId
    }),
    ...(input.reason === undefined ? {} : {
      reason: input.reason
    })
  };
}

function candidateFromRow(row: ApprovalOutboxRow, selectedReason: string): ApprovalReconciliationCandidate {
  return {
    outboxId: String(row.id),
    idempotencyKey: row.idempotency_key,
    destinationStage: row.destination_stage,
    routingKey: row.routing_key,
    entityKind: row.entity_kind,
    entityId: row.entity_id,
    payloadRef: row.payload_ref,
    payloadDigest: row.payload_digest,
    selectedReason,
    status: "selected"
  };
}

function approvalDecisionIdFromOutboxRow(row: ApprovalOutboxRow): string | undefined {
  const keyPrefix = "approval:translation:";

  if (row.idempotency_key.startsWith(keyPrefix) && row.idempotency_key.length > keyPrefix.length) {
    return row.idempotency_key.slice(keyPrefix.length);
  }

  const pathParts = row.payload_ref.split("/");
  const decisionId = pathParts.length >= 2 ? pathParts[pathParts.length - 2] : undefined;

  if (decisionId === undefined || decisionId.length === 0 || decisionId === "approval") {
    return undefined;
  }

  try {
    return decodeURIComponent(decisionId);
  } catch {
    return undefined;
  }
}

function approvalTranslationPayloadRef(decision: ApprovalStoredDecision): string {
  return `backend://worker-uplift/approval/${encodeURIComponent(decision.canonicalArticleId)}/${decision.decisionId}/translation-task`;
}

function assertReplayEnvelope(value: unknown): WorkerMessageEnvelope | undefined {
  const validation = validateWorkerEnvelope(value);

  return validation.ok ? validation.value : undefined;
}

function failedCandidate(candidate: ApprovalReconciliationCandidate, failedClosedReason: string): ApprovalReconciliationCandidate {
  return {
    ...candidate,
    status: "failed_closed",
    failedClosedReason
  };
}

function boundedInteger(value: unknown, defaultValue: number, min: number, max: number): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    return defaultValue;
  }

  return Math.max(min, Math.min(max, value));
}

function safeRunId(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();

  return /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/u.test(trimmed) ? trimmed : undefined;
}

function safeReason(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.replace(/[\r\n\t]+/gu, " ").trim();

  return trimmed.length === 0 ? undefined : trimmed.slice(0, 160);
}

function flagEnabled(value: string | undefined): boolean {
  return value?.trim().toLowerCase() === "true";
}

function safeHeaderValue(value: string): string | undefined {
  const trimmed = value.trim();

  if (trimmed.length === 0 || /[\r\n]/u.test(trimmed)) {
    return undefined;
  }

  return trimmed;
}

function sanitizeCode(value: string): string {
  return normalizeReasonCode(value, "approval-error");
}

function sanitizeMessage(value: string): string {
  return value.replace(/[\r\n\t]+/gu, " ").slice(0, 512);
}

function normalizeReasonCode(value: unknown, fallback: string): string {
  const normalized = boundedReasonCode(stringFrom(value, fallback), 80);

  if (normalized.length >= 2 && isLowerAsciiLetter(normalized.charCodeAt(0))) {
    return normalized;
  }

  return fallback;
}

function stripTrailingSlashes(value: string): string {
  let end = value.length;

  while (end > 0 && value.charCodeAt(end - 1) === 47) {
    end -= 1;
  }

  return value.slice(0, end);
}

function boundedReasonCode(value: string, maxLength: number): string {
  let output = "";

  for (const character of value.trim().toLowerCase()) {
    if (output.length >= maxLength) {
      break;
    }

    const codePoint = character.codePointAt(0);

    if (codePoint === undefined) {
      continue;
    }

    if (isLowerAsciiLetter(codePoint) || isAsciiDigit(codePoint) || character === "_" || character === "-") {
      output += character;
      continue;
    }

    if (output.length > 0 && !output.endsWith("-")) {
      output += "-";
    }
  }

  while (output.startsWith("-")) {
    output = output.slice(1);
  }

  while (output.endsWith("-")) {
    output = output.slice(0, -1);
  }

  return output;
}

function isLowerAsciiLetter(codePoint: number): boolean {
  return codePoint >= 97 && codePoint <= 122;
}

function isAsciiDigit(codePoint: number): boolean {
  return codePoint >= 48 && codePoint <= 57;
}

function localPositivityScore(value: unknown): number {
  const score = numberFrom(value, 0);

  return Math.max(0, Math.min(100, Math.round(score <= 10 ? score * 10 : score)));
}

function numberFrom(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function stringFrom(value: unknown, fallback: string): string {
  return typeof value === "string" && value.trim().length > 0 ? value : fallback;
}

function objectValue(value: unknown): Readonly<Record<string, unknown>> {
  return isRecord(value) ? value : {};
}

function nonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function isApprovalDecisionSnapshot(value: unknown): value is ApprovalStoredDecision {
  return isRecord(value)
    && typeof value.decisionId === "string"
    && typeof value.canonicalArticleId === "string"
    && typeof value.articleVersion === "number"
    && typeof value.decision === "string"
    && typeof value.provider === "string"
    && typeof value.model === "string"
    && typeof value.promptId === "string"
    && typeof value.promptVersion === "string";
}

function isReplayableApprovalDecisionSnapshot(value: unknown): value is ApprovalStoredDecision {
  return isApprovalDecisionSnapshot(value)
    && value.decisionId.trim().length > 0
    && value.canonicalArticleId.trim().length > 0
    && Number.isInteger(value.articleVersion)
    && value.articleVersion > 0
    && typeof value.sourceMessageId === "string"
    && value.sourceMessageId.trim().length > 0
    && typeof value.correlationId === "string"
    && value.correlationId.trim().length > 0
    && typeof value.traceparent === "string"
    && value.traceparent.trim().length > 0
    && typeof value.decidedAt === "string"
    && value.decidedAt.trim().length > 0
    && typeof value.sourceLanguage === "string"
    && value.sourceLanguage.trim().length > 0;
}

function isTranslationPublication(value: unknown): value is ApprovalTranslationPublication {
  return isRecord(value)
    && typeof value.messageId === "string"
    && typeof value.idempotencyKey === "string"
    && typeof value.publishedAt === "string";
}

function isApprovalSummaryRef(value: unknown): value is NonNullable<ApprovalStoredDecision["summaryRef"]> {
  return isRecord(value)
    && value.kind === "backend-record"
    && typeof value.uri === "string"
    && value.mediaType === "application/json"
    && typeof value.decisionId === "string"
    && typeof value.sourceLanguage === "string";
}

function isApprovalAiUsageRef(value: unknown): value is NonNullable<ApprovalStoredDecision["aiUsageRef"]> {
  return isRecord(value)
    && value.kind === "backend-record"
    && typeof value.uri === "string"
    && value.mediaType === "application/json"
    && typeof value.inputTokens === "number"
    && typeof value.outputTokens === "number"
    && typeof value.totalTokens === "number";
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
