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
import { createApprovalService } from "../src/service.js";
import {
  InMemoryApprovalStateStore,
  LocalApprovalQwenClient,
  LocalBrokerTransport,
  ManualApprovalClock,
  createLocalApprovalDependencies,
  createMinimalApprovalEnvelope,
  createMinimalApprovalPayload
} from "../src/test-doubles.js";

type ExpectedEvalDecision = "accepted" | "rejected" | "permanent_failure";

interface ApprovalEvalCase {
  readonly id: string;
  readonly articleNumber: number;
  readonly labelSource: "sanitized-parity-fixture";
  readonly expectedDecision: ExpectedEvalDecision;
  readonly expectedTranslation: boolean;
  readonly imageStatus?: "hydrated" | "no_thumbnail";
  readonly qwenResponse?: unknown;
}

interface ApprovalEvalResult {
  readonly id: string;
  readonly expectedDecision: ExpectedEvalDecision;
  readonly actualDecision: ExpectedEvalDecision | "missing";
  readonly expectedTranslation: boolean;
  readonly actualTranslation: boolean;
  readonly summaryValid: boolean;
  readonly safe: boolean;
  readonly qwenCalls: number;
}

const APPROVAL_EVAL_THRESHOLDS = {
  decisionAgreement: 1,
  summaryValidity: 1,
  safety: 1,
  regressionFailures: 0
} as const;

const APPROVAL_EVAL_CORPUS = [
  {
    id: "parity-positive-public-interest",
    articleNumber: 501,
    labelSource: "sanitized-parity-fixture",
    expectedDecision: "accepted",
    expectedTranslation: true,
    qwenResponse: {
      decision: "accepted",
      reasonCode: "newsworthy",
      confidenceScore: 96,
      qualityScore: 92,
      positivityScore: 83,
      summary: "Public-interest reporting describes a concrete constructive update with enough detail for translation.",
      latencyMs: 29
    }
  },
  {
    id: "parity-low-quality-rejection",
    articleNumber: 502,
    labelSource: "sanitized-parity-fixture",
    expectedDecision: "rejected",
    expectedTranslation: false,
    qwenResponse: {
      decision: "rejected",
      reasonCode: "low_quality",
      confidenceScore: 91,
      qualityScore: 18,
      positivityScore: 11,
      latencyMs: 24
    }
  },
  {
    id: "parity-no-thumbnail-prefilter",
    articleNumber: 503,
    labelSource: "sanitized-parity-fixture",
    expectedDecision: "rejected",
    expectedTranslation: false,
    imageStatus: "no_thumbnail"
  },
  {
    id: "parity-malformed-model-output",
    articleNumber: 504,
    labelSource: "sanitized-parity-fixture",
    expectedDecision: "permanent_failure",
    expectedTranslation: false,
    qwenResponse: {
      decision: "approve",
      reasonCode: "newsworthy",
      confidenceScore: 99,
      qualityScore: 99,
      positivityScore: 99,
      latencyMs: 18
    }
  },
  {
    id: "parity-empty-summary",
    articleNumber: 505,
    labelSource: "sanitized-parity-fixture",
    expectedDecision: "permanent_failure",
    expectedTranslation: false,
    qwenResponse: {
      decision: "accepted",
      reasonCode: "newsworthy",
      confidenceScore: 92,
      qualityScore: 91,
      positivityScore: 78,
      summary: " ",
      latencyMs: 17
    }
  },
  {
    id: "parity-unsafe-summary",
    articleNumber: 506,
    labelSource: "sanitized-parity-fixture",
    expectedDecision: "permanent_failure",
    expectedTranslation: false,
    qwenResponse: {
      decision: "accepted",
      reasonCode: "newsworthy",
      confidenceScore: 92,
      qualityScore: 91,
      positivityScore: 78,
      summary: "This output includes token=redacted-like-material and must never be accepted.",
      latencyMs: 17
    }
  }
] as const satisfies readonly ApprovalEvalCase[];

describe("approval eval corpus", () => {
  it("meets quality, summary, safety, and regression thresholds before image promotion", async () => {
    const results: ApprovalEvalResult[] = [];

    for (const testCase of APPROVAL_EVAL_CORPUS) {
      results.push(await runEvalCase(testCase));
    }

    const agreementCount = results.filter((result) => result.actualDecision === result.expectedDecision).length;
    const acceptedResults = results.filter((result) => result.expectedDecision === "accepted");
    const summaryValidCount = acceptedResults.filter((result) => result.summaryValid).length;
    const safeCount = results.filter((result) => result.safe).length;
    const regressionFailures = results.filter((result) => result.actualTranslation !== result.expectedTranslation
      || result.actualDecision === "missing"
      || (result.expectedDecision === "rejected" && result.id !== "parity-no-thumbnail-prefilter" && result.qwenCalls !== 1)
      || (result.id === "parity-no-thumbnail-prefilter" && result.qwenCalls !== 0)).length;
    const report = {
      totalCases: results.length,
      decisionAgreement: agreementCount / results.length,
      summaryValidity: summaryValidCount / acceptedResults.length,
      safety: safeCount / results.length,
      regressionFailures
    };

    expect(report).toMatchObject({
      totalCases: APPROVAL_EVAL_CORPUS.length,
      regressionFailures: APPROVAL_EVAL_THRESHOLDS.regressionFailures
    });
    expect(report.decisionAgreement).toBeGreaterThanOrEqual(APPROVAL_EVAL_THRESHOLDS.decisionAgreement);
    expect(report.summaryValidity).toBeGreaterThanOrEqual(APPROVAL_EVAL_THRESHOLDS.summaryValidity);
    expect(report.safety).toBeGreaterThanOrEqual(APPROVAL_EVAL_THRESHOLDS.safety);
  });
});

async function runEvalCase(testCase: ApprovalEvalCase): Promise<ApprovalEvalResult> {
  const context = createApprovalEvalContext();

  if (testCase.qwenResponse !== undefined) {
    context.qwenClient.response = testCase.qwenResponse;
  }

  await context.service.start();

  const result = await context.broker.deliverApproval(createEvalDelivery(testCase));

  await context.service.stop();

  const decision = context.stateStore.decisions[0];
  const summary = decision?.sourceSummary;
  const promptInstructions = context.qwenClient.requests[0]?.prompt.instructions ?? "";
  const telemetryJson = JSON.stringify(context.telemetry.events);
  const summaryValid = testCase.expectedDecision !== "accepted"
    ? summary === undefined
    : typeof summary === "string"
      && summary.length >= context.config.summary.minChars
      && summary.length <= context.config.summary.maxChars;
  const promptAbsentFromTelemetry = promptInstructions.length === 0 || !telemetryJson.includes(promptInstructions);
  const safe = result.action === "ack"
    && promptAbsentFromTelemetry
    && !telemetryJson.toLowerCase().includes("token=")
    && !telemetryJson.toLowerCase().includes("secret=");

  return {
    id: testCase.id,
    expectedDecision: testCase.expectedDecision,
    actualDecision: decision?.decision ?? "missing",
    expectedTranslation: testCase.expectedTranslation,
    actualTranslation: context.broker.published.length > 0,
    summaryValid,
    safe,
    qwenCalls: context.qwenClient.requests.length
  };
}

function createApprovalEvalContext() {
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
    qwenClient: dependencies.qwenClient as LocalApprovalQwenClient,
    service,
    stateStore: dependencies.stateStore as InMemoryApprovalStateStore,
    telemetry
  };
}

function createEvalDelivery(testCase: ApprovalEvalCase) {
  const articleId = `article-${String(testCase.articleNumber)}`;
  const candidateId = `candidate-${String(testCase.articleNumber)}`;
  const fingerprint = `fingerprint-${String(testCase.articleNumber)}`;
  const payload = createMinimalApprovalPayload({
    idempotencyKey: `enrichment:approval:${articleId}:${fingerprint}`,
    candidateId,
    canonicalUrl: `https://articles.example.test/eval/${String(testCase.articleNumber)}`,
    imageStatus: testCase.imageStatus ?? "hydrated",
    articleMetadataRef: {
      kind: "backend-record",
      uri: `backend://worker-uplift/enrichment/${articleId}/${fingerprint}`,
      mediaType: "application/json",
      contentFingerprint: fingerprint,
      canonicalArticleId: articleId,
      articleVersion: 1,
      title: `Sanitized parity fixture ${String(testCase.articleNumber)}`,
      description: "Sanitized labeled metadata retained by reference for approval evals.",
      language: "en"
    }
  });

  return {
    envelope: createMinimalApprovalEnvelope({
      messageId: messageIdFor(testCase.articleNumber),
      idempotencyKey: `enrichment:approval:${articleId}:${fingerprint}`,
      aggregate: {
        type: "article",
        id: articleId,
        version: 1
      }
    }),
    payload: testCase.imageStatus === "no_thumbnail" ? withoutImageUrl(payload) : payload,
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}

function withoutImageUrl(payload: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> {
  return Object.fromEntries(Object.entries(payload).filter(([key]) => key !== "imageUrl"));
}

function messageIdFor(index: number): string {
  return `018f1598-2dd5-7c4f-9f92-8f7a7f8c${String(5_000 + index).slice(-4)}`;
}
