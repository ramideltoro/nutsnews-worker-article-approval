import {
  describe,
  expect,
  it
} from "vitest";

import {
  ApprovalConfigError,
  loadApprovalConfig
} from "../src/config.js";

describe("loadApprovalConfig", () => {
  it("loads low-concurrency local test defaults without secret values", () => {
    const config = loadApprovalConfig({
      HOSTNAME: "approval-host"
    });

    expect(config).toMatchObject({
      serviceName: "nutsnews-worker-article-approval",
      dependencyMode: "test",
      host: "approval-host",
      concurrency: 2,
      prefetch: 4,
      qwen: {
        model: "qwen2.5:3b",
        promptId: "editorial-approval-v1",
        totalTimeoutMs: 30_000,
        maxInputBytes: 32_768,
        maxParallelCalls: 1,
        maxQueuedCalls: 3,
        backpressureRetryAfterMs: 5_000
      },
      targetLanguages: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ],
      summary: {
        minChars: 40,
        maxChars: 600
      },
      openAiFallback: {
        enabled: false,
        protectedFlag: false,
        budgetUsd: 0,
        provenanceMarker: "",
        alertTopic: ""
      },
      shadowMode: true,
      dependencies: {
        databaseConfigured: false,
        rabbitmqConfigured: false,
        qwenEndpointConfigured: false,
        qwenCredentialConfigured: false
      }
    });
  });

  it("fails production config by missing secret names only", () => {
    expect(() => loadApprovalConfig({
      NUTSNEWS_APPROVAL_DEPENDENCY_MODE: "production"
    })).toThrow(ApprovalConfigError);

    try {
      loadApprovalConfig({
        NUTSNEWS_APPROVAL_DEPENDENCY_MODE: "production"
      });
    } catch (error: unknown) {
      expect(error).toBeInstanceOf(ApprovalConfigError);
      const configError = error as ApprovalConfigError;

      expect(configError.issues).toEqual([
        "NUTSNEWS_APPROVAL_DATABASE_URL is required when NUTSNEWS_APPROVAL_DEPENDENCY_MODE=production.",
        "NUTSNEWS_APPROVAL_RABBITMQ_URL is required when NUTSNEWS_APPROVAL_DEPENDENCY_MODE=production.",
        "NUTSNEWS_APPROVAL_QWEN_BASE_URL is required when NUTSNEWS_APPROVAL_DEPENDENCY_MODE=production.",
        "NUTSNEWS_APPROVAL_QWEN_API_KEY is required when NUTSNEWS_APPROVAL_DEPENDENCY_MODE=production."
      ]);
      expect(configError.message).not.toContain("postgres://");
      expect(configError.message).not.toContain("amqp://");
      expect(configError.message).not.toContain("sk-");
    }
  });

  it("rejects unsafe concurrency bounds and shadow cutover in this repo", () => {
    expect(() => loadApprovalConfig({
      NUTSNEWS_APPROVAL_CONCURRENCY: "8",
      NUTSNEWS_APPROVAL_PREFETCH: "2",
      NUTSNEWS_APPROVAL_QWEN_TOTAL_TIMEOUT_MS: "10",
      NUTSNEWS_APPROVAL_QWEN_MAX_INPUT_BYTES: "16",
      NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS: "9",
      NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS: "8",
      NUTSNEWS_APPROVAL_SUMMARY_MIN_CHARS: "700",
      NUTSNEWS_APPROVAL_SUMMARY_MAX_CHARS: "80",
      NUTSNEWS_APPROVAL_SHADOW_MODE: "false"
    })).toThrow(ApprovalConfigError);
  });

  it("parses approval prompt, target language, and summary bounds overrides", () => {
    const config = loadApprovalConfig({
      NUTSNEWS_APPROVAL_QWEN_MODEL: "qwen-test",
      NUTSNEWS_APPROVAL_PROMPT_ID: "editorial-approval-v2",
      NUTSNEWS_APPROVAL_PREFETCH: "5",
      NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS: "2",
      NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS: "3",
      NUTSNEWS_APPROVAL_QWEN_BACKPRESSURE_RETRY_AFTER_MS: "12000",
      NUTSNEWS_APPROVAL_TARGET_LANGUAGES: "fr, ja, fr, de",
      NUTSNEWS_APPROVAL_SUMMARY_MIN_CHARS: "20",
      NUTSNEWS_APPROVAL_SUMMARY_MAX_CHARS: "300"
    });

    expect(config.qwen).toMatchObject({
      model: "qwen-test",
      promptId: "editorial-approval-v2",
      maxParallelCalls: 2,
      maxQueuedCalls: 3,
      backpressureRetryAfterMs: 12_000
    });
    expect(config.targetLanguages).toEqual([
      "fr",
      "ja",
      "de"
    ]);
    expect(config.summary).toEqual({
      minChars: 20,
      maxChars: 300
    });
  });

  it("requires protected fallback controls before OpenAI fallback can be enabled", () => {
    expect(() => loadApprovalConfig({
      NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ENABLED: "true"
    })).toThrow(ApprovalConfigError);

    const config = loadApprovalConfig({
      NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ENABLED: "true",
      NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROTECTED_FLAG: "true",
      NUTSNEWS_APPROVAL_OPENAI_FALLBACK_BUDGET_USD: "25.50",
      NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROVENANCE_MARKER: "legacy_openai_fallback",
      NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ALERT_TOPIC: "ops.approval-fallback"
    });

    expect(config.openAiFallback).toEqual({
      enabled: true,
      protectedFlag: true,
      budgetUsd: 25.5,
      provenanceMarker: "legacy_openai_fallback",
      alertTopic: "ops.approval-fallback"
    });
  });

  it("accepts explicit production dependency presence without retaining sensitive values", () => {
    const config = loadApprovalConfig({
      NUTSNEWS_APPROVAL_DEPENDENCY_MODE: "production",
      NUTSNEWS_APPROVAL_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_APPROVAL_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_APPROVAL_QWEN_BASE_URL: "https://qwen.internal.invalid/v1",
      NUTSNEWS_APPROVAL_QWEN_API_KEY: "sk-not-real",
      NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
    });

    expect(config.dependencies).toEqual({
      databaseConfigured: true,
      rabbitmqConfigured: true,
      qwenEndpointConfigured: true,
      qwenCredentialConfigured: true
    });
    expect(JSON.stringify(config)).not.toContain("postgres://example.invalid");
    expect(JSON.stringify(config)).not.toContain("amqp://example.invalid");
    expect(JSON.stringify(config)).not.toContain("qwen.internal.invalid");
    expect(JSON.stringify(config)).not.toContain("sk-not-real");
  });
});
