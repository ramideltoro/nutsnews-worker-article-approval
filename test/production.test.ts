import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadApprovalConfig } from "../src/config.js";
import {
  ApprovalQwenError,
  type ApprovalQwenRequest
} from "../src/dependencies.js";
import {
  LocalAiApprovalQwenClient,
  createProductionApprovalDependencies
} from "../src/production.js";

const clock = {
  now: () => new Date("2026-07-23T00:00:00.000Z")
};

describe("production approval dependencies", () => {
  it("wires production mode to RabbitMQ, Postgres, and local AI dependencies", async () => {
    const env = productionEnv();
    const config = loadApprovalConfig({
      ...env,
      NUTSNEWS_APPROVAL_DEPENDENCY_MODE: "production"
    });
    const dependencies = createProductionApprovalDependencies({
      config,
      clock,
      env
    });

    expect(dependencies.brokerTransport.name).toBe("rabbitmq-payload-transport");
    expect(dependencies.stateStore.name).toBe("postgres-approval-state");
    expect(dependencies.transactionRunner.name).toBe("postgres-approval-transactions");
    expect(dependencies.brokerOutbox.name).toBe("postgres-approval-outbox");
    expect(dependencies.qwenClient.name).toBe("local-ai-approval-client");
    expect(dependencies.promptRegistry.name).toBe("static-approval-prompt-registry");

    await dependencies.close();
  });

  it("maps the legacy local AI review response into an approval decision", async () => {
    const fetcher = vi.fn(() => Promise.resolve(new Response(JSON.stringify({
      decision: "accept",
      category: "community-focused",
      positivity_score: 9,
      summary: "Students built free book libraries in their community, giving families easier access to uplifting reading time together.",
      reason: "Positive community story.",
      prompt_tokens: 266,
      completion_tokens: 64,
      total_tokens: 330,
      duration_ms: 8236
    }), {
      status: 200
    })));
    const client = new LocalAiApprovalQwenClient({
      baseUrl: "https://ai.example.test/",
      apiKey: "local-key",
      clock,
      fetcher
    });

    await expect(client.review(reviewRequest())).resolves.toMatchObject({
      decision: "accepted",
      reasonCode: "community-focused",
      confidenceScore: 92,
      qualityScore: 90,
      positivityScore: 90,
      latencyMs: 8236,
      usage: {
        inputTokens: 266,
        outputTokens: 64,
        totalTokens: 330
      }
    });
    expect(fetcher).toHaveBeenCalledWith("https://ai.example.test/review", expect.objectContaining({
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-nutsnews-ai-key": "local-key"
      }
    }));
  });

  it("fails closed before calling local AI when the API key is not a valid header", async () => {
    const fetcher = vi.fn();
    const client = new LocalAiApprovalQwenClient({
      baseUrl: "https://ai.example.test",
      apiKey: "bad\nkey",
      clock,
      fetcher
    });

    await expect(client.review(reviewRequest())).rejects.toMatchObject({
      name: "ApprovalQwenError",
      reason: "qwen-unauthorized",
      retryable: false
    } satisfies Partial<ApprovalQwenError>);
    expect(fetcher).not.toHaveBeenCalled();
  });
});

function productionEnv(): NodeJS.ProcessEnv {
  return {
    NUTSNEWS_APPROVAL_DATABASE_URL: "postgres://approval:secret@example.invalid:5432/nutsnews",
    NUTSNEWS_APPROVAL_RABBITMQ_URL: "amqp://approval:secret@example.invalid:5672",
    NUTSNEWS_APPROVAL_QWEN_BASE_URL: "https://ai.example.test",
    NUTSNEWS_APPROVAL_QWEN_API_KEY: "local-key",
    NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS: "1",
    NUTSNEWS_APPROVAL_PREFETCH: "2",
    NUTSNEWS_APPROVAL_CONCURRENCY: "1"
  };
}

function reviewRequest(): ApprovalQwenRequest {
  return {
    model: "qwen2.5:3b",
    prompt: {
      id: "editorial-approval-v1",
      version: "0.1.0",
      purpose: "editorial-approval",
      instructions: "Return JSON."
    },
    timeoutMs: 30_000,
    maxInputBytes: 32_768,
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
    input: {
      candidateId: "candidate-001",
      canonicalArticleId: "article-001",
      articleVersion: 1,
      canonicalUrl: "https://articles.example.test/story",
      title: "Students build free library boxes",
      description: "Students build free library boxes for neighborhood readers.",
      sourceLanguage: "en",
      contentFingerprint: "fingerprint001"
    },
    inputBytes: 512
  };
}
