import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock,
  RuntimeHandlerResult,
  RuntimeIdempotencyStore,
  RuntimeMessageContext,
  RuntimeAdapterMode
} from "@ramideltoro/nutsnews-worker-runtime";

export interface ApprovalDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export interface ApprovalStateStore extends RuntimeIdempotencyStore {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
  ownership(idempotencyKey: string): ApprovalClaimOwnership;
  abortActiveClaims(reason: string): Promise<void>;
  loadEnrichmentRecord(input: ApprovalEnrichmentRecordInput, transaction: ApprovalDatabaseTransaction): Promise<ApprovalEnrichmentRecord>;
  findDecision(key: ApprovalDecisionKey, transaction: ApprovalDatabaseTransaction): Promise<ApprovalStoredDecision | undefined>;
  recordDecision(decision: ApprovalStoredDecision, transaction: ApprovalDatabaseTransaction): Promise<ApprovalStoredDecision>;
  markTranslationPublished(decisionId: string, publication: ApprovalTranslationPublication, transaction: ApprovalDatabaseTransaction): Promise<ApprovalStoredDecision>;
}

export interface ApprovalDatabaseTransaction {
  readonly transactionId: string;
}

export interface ApprovalDatabaseTransactionRunner {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
  withTransaction<T>(
    operation: (transaction: ApprovalDatabaseTransaction) => Promise<T>,
    signal?: AbortSignal
  ): Promise<T>;
}

export interface ApprovalBrokerOutbox {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt, signal?: AbortSignal): Promise<void>;
}

export interface ApprovalBrokerTransport extends RuntimeBrokerTransport {
  publishOwned(command: BrokerPublishCommand, signal: AbortSignal): Promise<BrokerPublishReceipt>;
}

export interface ApprovalQwenClient {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
  review(request: ApprovalQwenRequest, signal?: AbortSignal): Promise<unknown>;
}

export interface ApprovalClaimOwnership {
  readonly signal: AbortSignal;
  assertOwned(): void;
}

export interface ApprovalPrompt {
  readonly id: string;
  readonly version: string;
  readonly purpose: "editorial-approval";
  readonly instructions: string;
}

export interface ApprovalPromptRegistry {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
  getPrompt(id: string): Promise<ApprovalPrompt>;
}

export interface ApprovalWorkTools {
  readonly signal: AbortSignal;
  assertOwnership(): void;
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: ApprovalDatabaseTransaction) => Promise<T>): Promise<T>;
}

export class ApprovalClaimOwnershipError extends Error {
  constructor(message = "Approval idempotency claim ownership is no longer certain.") {
    super(message);
    this.name = "ApprovalClaimOwnershipError";
  }
}

export interface ApprovalWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: ApprovalWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface ApprovalDependencies {
  readonly adapterMode: RuntimeAdapterMode;
  readonly clock: RuntimeClock;
  readonly stateStore: ApprovalStateStore;
  readonly transactionRunner: ApprovalDatabaseTransactionRunner;
  readonly brokerOutbox: ApprovalBrokerOutbox;
  readonly brokerTransport: ApprovalBrokerTransport;
  readonly qwenClient: ApprovalQwenClient;
  readonly promptRegistry: ApprovalPromptRegistry;
  readonly workHandler: ApprovalWorkHandler;
}

export interface ApprovalEnrichmentRecordInput {
  readonly candidateId: string;
  readonly canonicalUrl: string;
  readonly imageStatus: "hydrated" | "no_thumbnail" | "transient_failure";
  readonly imageUrl?: string;
  readonly articleMetadataRef: ApprovalMetadataReference;
}

export interface ApprovalMetadataReference {
  readonly kind: "backend-record";
  readonly uri: string;
  readonly mediaType: "application/json";
  readonly contentFingerprint: string;
  readonly canonicalArticleId: string;
  readonly articleVersion: number;
  readonly title?: string;
  readonly description?: string;
  readonly publishedAt?: string;
  readonly language?: string;
}

export interface ApprovalEnrichmentRecord {
  readonly candidateId: string;
  readonly canonicalArticleId: string;
  readonly articleVersion: number;
  readonly canonicalUrl: string;
  readonly imageStatus: ApprovalEnrichmentRecordInput["imageStatus"];
  readonly imageUrl?: string;
  readonly contentFingerprint: string;
  readonly title: string;
  readonly description?: string;
  readonly publishedAt?: string;
  readonly sourceLanguage: string;
  readonly metadataRef: ApprovalMetadataReference;
}

export interface ApprovalDecisionKey {
  readonly canonicalArticleId: string;
  readonly articleVersion: number;
  readonly promptId: string;
  readonly promptVersion: string;
  readonly model: string;
}

export interface ApprovalTranslationPublication {
  readonly messageId: string;
  readonly idempotencyKey: string;
  readonly publishedAt: string;
}

export interface ApprovalStoredDecision {
  readonly decisionId: string;
  readonly candidateId: string;
  readonly canonicalArticleId: string;
  readonly articleVersion: number;
  readonly canonicalUrl: string;
  readonly decision: "accepted" | "rejected" | "permanent_failure";
  readonly rejectionReason?: string;
  readonly provider: "prefilter" | "local_ai" | "legacy_openai_fallback";
  readonly model: string;
  readonly promptId: string;
  readonly promptVersion: string;
  readonly positivityScore: number;
  readonly confidenceScore: number;
  readonly qualityScore: number;
  readonly sourceLanguage: string;
  readonly sourceSummary?: string;
  readonly contentFingerprint: string;
  readonly reviewRef: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: "application/json";
    readonly decisionId: string;
    readonly canonicalArticleId: string;
    readonly articleVersion: number;
    readonly promptId: string;
    readonly promptVersion: string;
    readonly model: string;
    readonly traceparent: string;
    readonly sourceMessageId: string;
  };
  readonly summaryRef?: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: "application/json";
    readonly decisionId: string;
    readonly sourceLanguage: string;
  };
  readonly aiUsageRef?: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: "application/json";
    readonly inputTokens: number;
    readonly outputTokens: number;
    readonly totalTokens: number;
  };
  readonly sourceMessageId: string;
  readonly correlationId: string;
  readonly traceparent: string;
  readonly latencyMs: number;
  readonly decidedAt: string;
  readonly translationPublication?: ApprovalTranslationPublication;
}

export interface ApprovalQwenRequest {
  readonly model: string;
  readonly prompt: ApprovalPrompt;
  readonly timeoutMs: number;
  readonly maxInputBytes: number;
  readonly deterministic: {
    readonly temperature: 0;
    readonly topP: 1;
  };
  readonly responseSchema: {
    readonly name: "approval_decision_v1";
    readonly requiredFields: readonly string[];
  };
  readonly input: {
    readonly candidateId: string;
    readonly canonicalArticleId: string;
    readonly articleVersion: number;
    readonly canonicalUrl: string;
    readonly title: string;
    readonly description?: string;
    readonly imageUrl?: string;
    readonly sourceLanguage: string;
    readonly contentFingerprint: string;
  };
  readonly inputBytes: number;
}

export class ApprovalQwenError extends Error {
  readonly reason: "qwen-timeout" | "qwen-rate-limited" | "qwen-unauthorized" | "qwen-model-error";
  readonly retryable: boolean;
  readonly retryAfterMs: number | undefined;

  constructor(
    reason: ApprovalQwenError["reason"],
    options: {
      readonly retryable: boolean;
      readonly retryAfterMs?: number;
    }
  ) {
    super(reason);
    this.name = "ApprovalQwenError";
    this.reason = reason;
    this.retryable = options.retryable;
    this.retryAfterMs = options.retryAfterMs;
  }
}
