import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerPublishCommand,
  type RuntimeHandlerResult,
  type RuntimeMessageContext,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { ApprovalConfig } from "./config.js";
import {
  ApprovalQwenError,
  type ApprovalDecisionKey,
  type ApprovalDependencies,
  type ApprovalEnrichmentRecord,
  type ApprovalEnrichmentRecordInput,
  type ApprovalMetadataReference,
  type ApprovalPrompt,
  type ApprovalQwenRequest,
  type ApprovalStoredDecision,
  type ApprovalWorkHandler,
  type ApprovalWorkTools
} from "./dependencies.js";
import { stableUuid } from "./ids.js";

export interface ArticleApprovalWorkHandlerOptions {
  readonly config: ApprovalConfig;
  readonly dependencies: ApprovalDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
}

interface QwenDecisionBase {
  readonly reasonCode: string;
  readonly confidenceScore: number;
  readonly qualityScore: number;
  readonly positivityScore: number;
  readonly latencyMs: number;
  readonly usage?: {
    readonly inputTokens: number;
    readonly outputTokens: number;
    readonly totalTokens: number;
  };
}

type QwenDecision = QwenDecisionBase & (
  | {
      readonly decision: "accepted";
      readonly summary: string;
    }
  | {
      readonly decision: "rejected";
    }
);

interface ValidatedModelDecision {
  readonly ok: true;
  readonly value: QwenDecision;
}

interface InvalidModelDecision {
  readonly ok: false;
  readonly reason: "invalid_ai_decision_schema" | "empty_summary" | "summary_too_short" | "summary_too_long" | "unsafe_summary_content";
  readonly latencyMs: number;
}

type ModelDecisionValidation = ValidatedModelDecision | InvalidModelDecision;

const APPROVAL_QUEUE = getWorkerRoute("approval").mainQueue.name;
const SUMMARY_UNSAFE_RE = /bearer |api_key=|apikey=|token=|secret=|password=|private_key|service_role/iu;
const REASON_CODE_RE = /^[a-z][a-z0-9_-]{1,79}$/u;

export function createArticleApprovalWorkHandler(options: ArticleApprovalWorkHandlerOptions): ApprovalWorkHandler {
  return {
    name: "article-approval-work-handler",
    handle: (context, tools) => handleApproval(context, tools, options)
  };
}

async function handleApproval(
  context: RuntimeMessageContext,
  tools: ApprovalWorkTools,
  options: ArticleApprovalWorkHandlerOptions
): Promise<RuntimeHandlerResult> {
  let input: ApprovalEnrichmentRecordInput;

  try {
    input = approvalInputFromContext(context);
  } catch (error: unknown) {
    return {
      status: "terminal-failure",
      reason: error instanceof Error ? normalizeReason(error.message) : "invalid-approval-input"
    };
  }

  const prompt = await options.dependencies.promptRegistry.getPrompt(options.config.qwen.promptId);
  const decisionKey = approvalDecisionKey(input.articleMetadataRef, prompt, options.config);
  const existing = await tools.withTransaction((transaction) => options.dependencies.stateStore.findDecision(decisionKey, transaction));

  if (existing !== undefined) {
    await emitDecisionTelemetry(options, existing, true);
    await publishAcceptedTranslationIfNeeded(context, existing, tools, options);

    return {
      status: "ok"
    };
  }

  const enrichmentRecord = await tools.withTransaction((transaction) => options.dependencies.stateStore.loadEnrichmentRecord(input, transaction));
  const decidedAt = runtimeNow(options.dependencies.clock);
  const decision = enrichmentRecord.imageStatus === "hydrated"
    ? await modelDecision(context, enrichmentRecord, prompt, decidedAt, options)
    : prefilterRejection(context, enrichmentRecord, prompt, decidedAt, options);

  if (decision.status !== "decision") {
    return decision.result;
  }

  const recorded = await tools.withTransaction((transaction) => options.dependencies.stateStore.recordDecision(decision.value, transaction));

  await emitDecisionTelemetry(options, recorded, false);
  await publishAcceptedTranslationIfNeeded(context, recorded, tools, options);

  return {
    status: "ok"
  };
}

async function modelDecision(
  context: RuntimeMessageContext,
  enrichmentRecord: ApprovalEnrichmentRecord,
  prompt: ApprovalPrompt,
  decidedAt: string,
  options: ArticleApprovalWorkHandlerOptions
): Promise<
  | {
      readonly status: "decision";
      readonly value: ApprovalStoredDecision;
    }
  | {
      readonly status: "runtime";
      readonly result: RuntimeHandlerResult;
    }
> {
  const request = modelRequest(enrichmentRecord, prompt, options.config);

  if (request.inputBytes > options.config.qwen.maxInputBytes) {
    return {
      status: "decision",
      value: storedDecision(context, enrichmentRecord, prompt, options.config, {
        decision: "permanent_failure",
        provider: "local_ai",
        rejectionReason: "model_input_too_large",
        positivityScore: 0,
        confidenceScore: 0,
        qualityScore: 0,
        latencyMs: 0,
        decidedAt
      })
    };
  }

  const startedAtMs = options.dependencies.clock.now().getTime();
  let raw: unknown;

  try {
    raw = await options.dependencies.qwenClient.review(request);
  } catch (error: unknown) {
    if (error instanceof ApprovalQwenError && error.retryable) {
      if (error.retryAfterMs === undefined) {
        return {
          status: "runtime",
          result: {
            status: "retry",
            reason: error.reason
          }
        };
      }

      return {
        status: "runtime",
        result: {
          status: "retry",
          reason: error.reason,
          retryAfterMs: error.retryAfterMs
        }
      };
    }

    return {
      status: "decision",
      value: storedDecision(context, enrichmentRecord, prompt, options.config, {
        decision: "permanent_failure",
        provider: "local_ai",
        rejectionReason: error instanceof ApprovalQwenError ? error.reason : "qwen-unauthorized",
        positivityScore: 0,
        confidenceScore: 0,
        qualityScore: 0,
        latencyMs: elapsedMs(options, startedAtMs),
        decidedAt
      })
    };
  }

  const validation = validateQwenDecision(raw, options.config, elapsedMs(options, startedAtMs));

  if (!validation.ok) {
    return {
      status: "decision",
      value: storedDecision(context, enrichmentRecord, prompt, options.config, {
        decision: "permanent_failure",
        provider: "local_ai",
        rejectionReason: validation.reason,
        positivityScore: 0,
        confidenceScore: 0,
        qualityScore: 0,
        latencyMs: validation.latencyMs,
        decidedAt
      })
    };
  }

  const qwenDecision = validation.value;
  const decisionValues = {
    decision: qwenDecision.decision,
    provider: "local_ai",
    ...(qwenDecision.decision === "accepted" ? {
      sourceSummary: qwenDecision.summary
    } : {
      rejectionReason: qwenDecision.reasonCode
    }),
    positivityScore: qwenDecision.positivityScore,
    confidenceScore: qwenDecision.confidenceScore,
    qualityScore: qwenDecision.qualityScore,
    ...(qwenDecision.usage === undefined ? {} : {
      usage: qwenDecision.usage
    }),
    latencyMs: qwenDecision.latencyMs,
    decidedAt
  } as const;

  return {
    status: "decision",
    value: storedDecision(context, enrichmentRecord, prompt, options.config, decisionValues)
  };
}

function prefilterRejection(
  context: RuntimeMessageContext,
  enrichmentRecord: ApprovalEnrichmentRecord,
  prompt: ApprovalPrompt,
  decidedAt: string,
  options: ArticleApprovalWorkHandlerOptions
): {
  readonly status: "decision";
  readonly value: ApprovalStoredDecision;
} {
  return {
    status: "decision",
    value: storedDecision(context, enrichmentRecord, prompt, options.config, {
      decision: "rejected",
      provider: "prefilter",
      rejectionReason: enrichmentRecord.imageStatus === "no_thumbnail" ? "no_thumbnail" : "transient_image_failure",
      positivityScore: 0,
      confidenceScore: 100,
      qualityScore: 0,
      latencyMs: 0,
      decidedAt
    })
  };
}

function storedDecision(
  context: RuntimeMessageContext,
  enrichmentRecord: ApprovalEnrichmentRecord,
  prompt: ApprovalPrompt,
  config: ApprovalConfig,
  values: {
    readonly decision: "accepted" | "rejected" | "permanent_failure";
    readonly provider: "prefilter" | "local_ai";
    readonly rejectionReason?: string;
    readonly positivityScore: number;
    readonly confidenceScore: number;
    readonly qualityScore: number;
    readonly sourceSummary?: string;
    readonly usage?: QwenDecision["usage"];
    readonly latencyMs: number;
    readonly decidedAt: string;
  }
): ApprovalStoredDecision {
  const decisionId = stableUuid([
    enrichmentRecord.canonicalArticleId,
    String(enrichmentRecord.articleVersion),
    prompt.id,
    prompt.version,
    config.qwen.model
  ]);
  const reviewRef = {
    kind: "backend-record",
    uri: `backend://worker-uplift/approval/${encodeURIComponent(enrichmentRecord.canonicalArticleId)}/${decisionId}/review`,
    mediaType: "application/json",
    decisionId,
    canonicalArticleId: enrichmentRecord.canonicalArticleId,
    articleVersion: enrichmentRecord.articleVersion,
    promptId: prompt.id,
    promptVersion: prompt.version,
    model: config.qwen.model,
    traceparent: context.envelope.traceparent,
    sourceMessageId: context.envelope.messageId
  } as const;
  const summaryRef = values.sourceSummary === undefined
    ? undefined
    : {
        kind: "backend-record",
        uri: `backend://worker-uplift/approval/${encodeURIComponent(enrichmentRecord.canonicalArticleId)}/${decisionId}/source-summary`,
        mediaType: "application/json",
        decisionId,
        sourceLanguage: enrichmentRecord.sourceLanguage
      } as const;
  const aiUsageRef = values.usage === undefined
    ? undefined
    : {
        kind: "backend-record",
        uri: `backend://worker-uplift/approval/${encodeURIComponent(enrichmentRecord.canonicalArticleId)}/${decisionId}/ai-usage`,
        mediaType: "application/json",
        inputTokens: values.usage.inputTokens,
        outputTokens: values.usage.outputTokens,
        totalTokens: values.usage.totalTokens
      } as const;
  const decision: ApprovalStoredDecision = {
    decisionId,
    candidateId: enrichmentRecord.candidateId,
    canonicalArticleId: enrichmentRecord.canonicalArticleId,
    articleVersion: enrichmentRecord.articleVersion,
    canonicalUrl: enrichmentRecord.canonicalUrl,
    decision: values.decision,
    ...(values.rejectionReason === undefined ? {} : {
      rejectionReason: values.rejectionReason
    }),
    provider: values.provider,
    model: config.qwen.model,
    promptId: prompt.id,
    promptVersion: prompt.version,
    positivityScore: values.positivityScore,
    confidenceScore: values.confidenceScore,
    qualityScore: values.qualityScore,
    sourceLanguage: enrichmentRecord.sourceLanguage,
    ...(values.sourceSummary === undefined ? {} : {
      sourceSummary: values.sourceSummary
    }),
    contentFingerprint: enrichmentRecord.contentFingerprint,
    reviewRef,
    ...(summaryRef === undefined ? {} : {
      summaryRef
    }),
    ...(aiUsageRef === undefined ? {} : {
      aiUsageRef
    }),
    sourceMessageId: context.envelope.messageId,
    correlationId: context.envelope.correlationId,
    traceparent: context.envelope.traceparent,
    latencyMs: values.latencyMs,
    decidedAt: values.decidedAt
  };

  assertValidApprovalDecisionPayload(context, decision);

  return decision;
}

async function publishAcceptedTranslationIfNeeded(
  context: RuntimeMessageContext,
  decision: ApprovalStoredDecision,
  tools: ApprovalWorkTools,
  options: ArticleApprovalWorkHandlerOptions
): Promise<void> {
  if (decision.decision !== "accepted" || decision.translationPublication !== undefined) {
    return;
  }

  const command = translationTaskCommand(context, decision, options.config);
  const receipt = await tools.publish(command);

  await tools.recordOutbox(command, receipt);
  await tools.withTransaction((transaction) => options.dependencies.stateStore.markTranslationPublished(decision.decisionId, {
    messageId: receipt.messageId,
    idempotencyKey: command.envelope.idempotencyKey,
    publishedAt: receipt.confirmedAt
  }, transaction));
}

function approvalInputFromContext(context: RuntimeMessageContext): ApprovalEnrichmentRecordInput {
  const imageUrl = optionalString(context.payload.imageUrl);

  return {
    candidateId: stringValue(context.payload.candidateId, "candidateId"),
    canonicalUrl: stringValue(context.payload.canonicalUrl, "canonicalUrl"),
    imageStatus: imageStatusValue(context.payload.imageStatus),
    ...(imageUrl === undefined ? {} : {
      imageUrl
    }),
    articleMetadataRef: metadataRefValue(context.payload.articleMetadataRef)
  };
}

function metadataRefValue(value: unknown): ApprovalMetadataReference {
  if (!isRecord(value)) {
    throw new Error("invalid-article-metadata-ref");
  }

  const title = optionalString(value.title);
  const description = optionalString(value.description);
  const publishedAt = optionalString(value.publishedAt);
  const language = optionalString(value.language);

  return {
    kind: literalValue(value.kind, "backend-record", "articleMetadataRef.kind"),
    uri: stringValue(value.uri, "articleMetadataRef.uri"),
    mediaType: literalValue(value.mediaType, "application/json", "articleMetadataRef.mediaType"),
    contentFingerprint: stringValue(value.contentFingerprint, "articleMetadataRef.contentFingerprint"),
    canonicalArticleId: stringValue(value.canonicalArticleId, "articleMetadataRef.canonicalArticleId"),
    articleVersion: positiveIntegerValue(value.articleVersion, "articleMetadataRef.articleVersion"),
    ...(title === undefined ? {} : {
      title
    }),
    ...(description === undefined ? {} : {
      description
    }),
    ...(publishedAt === undefined ? {} : {
      publishedAt
    }),
    ...(language === undefined ? {} : {
      language
    })
  };
}

function approvalDecisionKey(
  metadataRef: ApprovalMetadataReference,
  prompt: ApprovalPrompt,
  config: ApprovalConfig
): ApprovalDecisionKey {
  return {
    canonicalArticleId: metadataRef.canonicalArticleId,
    articleVersion: metadataRef.articleVersion,
    promptId: prompt.id,
    promptVersion: prompt.version,
    model: config.qwen.model
  };
}

function modelRequest(
  enrichmentRecord: ApprovalEnrichmentRecord,
  prompt: ApprovalPrompt,
  config: ApprovalConfig
): ApprovalQwenRequest {
  const input = boundedModelInput(enrichmentRecord, config.qwen.maxInputBytes);
  const inputBytes = Buffer.byteLength(JSON.stringify(input), "utf8");

  return {
    model: config.qwen.model,
    prompt,
    timeoutMs: config.qwen.totalTimeoutMs,
    maxInputBytes: config.qwen.maxInputBytes,
    deterministic: {
      temperature: 0,
      topP: 1
    },
    responseSchema: {
      name: "approval_decision_v1",
      requiredFields: [
        "decision",
        "reasonCode",
        "confidenceScore",
        "qualityScore",
        "positivityScore",
        "summary"
      ]
    },
    input,
    inputBytes
  };
}

function boundedModelInput(
  enrichmentRecord: ApprovalEnrichmentRecord,
  maxInputBytes: number
): ApprovalQwenRequest["input"] {
  const input = {
    candidateId: enrichmentRecord.candidateId,
    canonicalArticleId: enrichmentRecord.canonicalArticleId,
    articleVersion: enrichmentRecord.articleVersion,
    canonicalUrl: enrichmentRecord.canonicalUrl,
    title: boundedText(enrichmentRecord.title, 512),
    ...(enrichmentRecord.description === undefined ? {} : {
      description: boundedText(enrichmentRecord.description, 2_048)
    }),
    ...(enrichmentRecord.imageUrl === undefined ? {} : {
      imageUrl: enrichmentRecord.imageUrl
    }),
    sourceLanguage: enrichmentRecord.sourceLanguage,
    contentFingerprint: enrichmentRecord.contentFingerprint
  };

  if (Buffer.byteLength(JSON.stringify(input), "utf8") <= maxInputBytes || input.description === undefined) {
    return input;
  }

  return {
    ...input,
    description: boundedText(input.description, Math.max(128, Math.floor(input.description.length / 2)))
  };
}

function validateQwenDecision(raw: unknown, config: ApprovalConfig, fallbackLatencyMs: number): ModelDecisionValidation {
  if (!isRecord(raw)) {
    return invalidDecision("invalid_ai_decision_schema", fallbackLatencyMs);
  }

  const decision = raw.decision;
  const reasonCode = raw.reasonCode;
  const confidenceScore = raw.confidenceScore;
  const qualityScore = raw.qualityScore;
  const positivityScore = raw.positivityScore;
  const summary = raw.summary;
  const latencyMs = typeof raw.latencyMs === "number" && Number.isFinite(raw.latencyMs)
    ? Math.max(0, raw.latencyMs)
    : fallbackLatencyMs;

  if ((decision !== "accepted" && decision !== "rejected") || typeof reasonCode !== "string" || !REASON_CODE_RE.test(reasonCode) || !score(confidenceScore) || !score(qualityScore) || !score(positivityScore)) {
    return invalidDecision("invalid_ai_decision_schema", latencyMs);
  }

  if (decision === "accepted") {
    if (typeof summary !== "string" || summary.trim().length === 0) {
      return invalidDecision("empty_summary", latencyMs);
    }

    const trimmed = summary.trim();

    if (SUMMARY_UNSAFE_RE.test(trimmed)) {
      return invalidDecision("unsafe_summary_content", latencyMs);
    }

    if (trimmed.length < config.summary.minChars) {
      return invalidDecision("summary_too_short", latencyMs);
    }

    if (trimmed.length > config.summary.maxChars) {
      return invalidDecision("summary_too_long", latencyMs);
    }

    const parsedUsage = usage(raw.usage);

    return {
      ok: true,
      value: {
        decision,
        reasonCode,
        confidenceScore,
        qualityScore,
        positivityScore,
        summary: trimmed,
        latencyMs,
        ...(parsedUsage === undefined ? {} : {
          usage: parsedUsage
        })
      }
    };
  }

  const parsedUsage = usage(raw.usage);

  return {
    ok: true,
    value: {
      decision,
      reasonCode,
      confidenceScore,
      qualityScore,
      positivityScore,
      latencyMs,
      ...(parsedUsage === undefined ? {} : {
        usage: parsedUsage
      })
    }
  };
}

function invalidDecision(reason: InvalidModelDecision["reason"], latencyMs: number): InvalidModelDecision {
  return {
    ok: false,
    reason,
    latencyMs
  };
}

function usage(value: unknown): QwenDecision["usage"] | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const inputTokens = value.inputTokens;
  const outputTokens = value.outputTokens;
  const totalTokens = value.totalTokens;

  if (!nonNegativeInteger(inputTokens) || !nonNegativeInteger(outputTokens) || !nonNegativeInteger(totalTokens)) {
    return undefined;
  }

  return {
    inputTokens,
    outputTokens,
    totalTokens
  };
}

function translationTaskCommand(
  context: RuntimeMessageContext,
  decision: ApprovalStoredDecision,
  config: ApprovalConfig
): BrokerPublishCommand {
  const route = getWorkerRoute("translation");
  const idempotencyKey = `approval:translation:${decision.decisionId}`;
  const payload = translationTaskPayload(context, decision, idempotencyKey, config);
  const validation = validateStagePayload(payload);

  if (!validation.ok) {
    throw new Error(`Invalid translation task payload: ${validation.issues.map((issue) => `${issue.path}:${issue.code}`).join(", ")}`);
  }

  return {
    envelope: assertWorkerEnvelope({
      schemaId: route.schemaId,
      schemaVersion: 1,
      route: "translation",
      messageId: stableUuid([
        "translation-message",
        decision.decisionId
      ]),
      causationId: context.envelope.messageId,
      correlationId: context.envelope.correlationId,
      traceparent: context.envelope.traceparent,
      ...(context.envelope.tracestate === undefined ? {} : {
        tracestate: context.envelope.tracestate
      }),
      idempotencyKey,
      aggregate: {
        type: "article",
        id: decision.canonicalArticleId,
        version: decision.articleVersion
      },
      occurredAt: decision.decidedAt,
      attempt: {
        count: 1,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: decision.decidedAt
      },
      producer: {
        name: config.serviceName,
        version: config.serviceVersion
      },
      payloadRef: {
        kind: "backend-record",
        uri: `backend://worker-uplift/approval/${encodeURIComponent(decision.canonicalArticleId)}/${decision.decisionId}/translation-task`,
        mediaType: "application/json",
        sizeBytes: getStagePayloadSizeBytes(payload)
      }
    }),
    payload
  };
}

function translationTaskPayload(
  context: RuntimeMessageContext,
  decision: ApprovalStoredDecision,
  idempotencyKey: string,
  config: ApprovalConfig
): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: stringValue(context.payload.pipelineRunId, "pipelineRunId"),
    stageExecutionId: stableUuid([
      "translation-stage-execution",
      decision.decisionId
    ]),
    sourceMessageId: context.envelope.messageId,
    idempotencyKey,
    traceparent: context.envelope.traceparent,
    ...(context.envelope.tracestate === undefined ? {} : {
      tracestate: context.envelope.tracestate
    }),
    producedAt: decision.decidedAt,
    articleId: decision.canonicalArticleId,
    sourceLanguage: decision.sourceLanguage,
    targetLanguages: config.targetLanguages,
    reason: "new_article",
    existingLanguageCodes: []
  };
}

function assertValidApprovalDecisionPayload(
  context: RuntimeMessageContext,
  decision: ApprovalStoredDecision
): void {
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.approvalDecision,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: stringValue(context.payload.pipelineRunId, "pipelineRunId"),
    stageExecutionId: decision.decisionId,
    sourceMessageId: context.envelope.messageId,
    idempotencyKey: `approval:decision:${decision.decisionId}`,
    traceparent: context.envelope.traceparent,
    ...(context.envelope.tracestate === undefined ? {} : {
      tracestate: context.envelope.tracestate
    }),
    producedAt: decision.decidedAt,
    candidateId: decision.candidateId,
    decision: decision.decision,
    ...(decision.rejectionReason === undefined ? {} : {
      rejectionReason: decision.rejectionReason
    }),
    provider: decision.provider,
    model: decision.model,
    positivityScore: decision.positivityScore,
    reviewRef: decision.reviewRef,
    ...(decision.aiUsageRef === undefined ? {} : {
      aiUsageRef: decision.aiUsageRef
    })
  };
  const validation = validateStagePayload(payload);

  if (!validation.ok) {
    throw new Error(`Invalid approval decision payload: ${validation.issues.map((issue) => `${issue.path}:${issue.code}`).join(", ")}`);
  }
}

async function emitDecisionTelemetry(
  options: ArticleApprovalWorkHandlerOptions,
  decision: ApprovalStoredDecision,
  reusedDecision: boolean
): Promise<void> {
  await emitRuntimeTelemetry(options.telemetry, {
    name: "runtime.dependency.observed",
    level: decision.decision === "permanent_failure" ? "warn" : "info",
    at: runtimeNow(options.dependencies.clock),
    stage: "approval",
    queue: APPROVAL_QUEUE,
    outcome: decision.decision === "permanent_failure" ? "failure" : "success",
    attributes: {
      event: "approval.article.reviewed",
      dependency: "article-approval",
      decision: decision.decision,
      rejectionReason: decision.rejectionReason,
      provider: decision.provider,
      model: decision.model,
      promptId: decision.promptId,
      promptVersion: decision.promptVersion,
      reusedDecision,
      candidateId: decision.candidateId,
      canonicalArticleId: decision.canonicalArticleId,
      articleVersion: decision.articleVersion
    }
  });
}

function score(value: unknown): value is number {
  return Number.isInteger(value) && typeof value === "number" && value >= 0 && value <= 100;
}

function nonNegativeInteger(value: unknown): value is number {
  return Number.isInteger(value) && typeof value === "number" && value >= 0;
}

function imageStatusValue(value: unknown): ApprovalEnrichmentRecordInput["imageStatus"] {
  if (value === "hydrated" || value === "no_thumbnail" || value === "transient_failure") {
    return value;
  }

  throw new Error("invalid-image-status");
}

function stringValue(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`invalid-${field}`);
  }

  return value;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function positiveIntegerValue(value: unknown, field: string): number {
  if (!Number.isInteger(value) || typeof value !== "number" || value < 1) {
    throw new Error(`invalid-${field}`);
  }

  return value;
}

function literalValue<T extends string>(value: unknown, expected: T, field: string): T {
  if (value !== expected) {
    throw new Error(`invalid-${field}`);
  }

  return expected;
}

function boundedText(value: string, maxChars: number): string {
  return value.trim().slice(0, maxChars);
}

function elapsedMs(options: ArticleApprovalWorkHandlerOptions, startedAtMs: number): number {
  return Math.max(0, options.dependencies.clock.now().getTime() - startedAtMs);
}

function normalizeReason(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 80) || "approval-error";
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
