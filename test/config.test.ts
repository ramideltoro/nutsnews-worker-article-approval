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
        totalTimeoutMs: 30_000,
        maxInputBytes: 32_768
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
      NUTSNEWS_APPROVAL_SHADOW_MODE: "false"
    })).toThrow(ApprovalConfigError);
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
