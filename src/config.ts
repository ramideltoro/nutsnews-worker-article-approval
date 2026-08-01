import os from "node:os";

export const APPROVAL_SERVICE_NAME = "nutsnews-worker-article-approval" as const;
export const APPROVAL_SERVICE_VERSION = "0.1.0" as const;

export type ApprovalDependencyMode = "test" | "production";
export type ApprovalTelemetryLogMode = "stdout" | "silent";

export interface ApprovalConfigVariable {
  readonly name: string;
  readonly description: string;
  readonly requiredInProduction: boolean;
  readonly sensitive: boolean;
  readonly defaultValue?: string;
}

export const APPROVAL_CONFIG_SCHEMA = [
  variable("NUTSNEWS_ENVIRONMENT", "Runtime environment label for logs and metrics.", false, false, "local"),
  variable("NUTSNEWS_APPROVAL_BUILD_REVISION", "Immutable lowercase 40-character Git commit revision baked into the production image.", true, false, "development"),
  variable("NUTSNEWS_APPROVAL_HTTP_HOST", "Health and metrics bind host.", false, false, "0.0.0.0"),
  variable("NUTSNEWS_APPROVAL_HTTP_PORT", "Health and metrics bind port.", false, false, "8080"),
  variable("NUTSNEWS_APPROVAL_DEPENDENCY_MODE", "Use test dependencies locally or require production dependency presence.", false, false, "test"),
  variable("NUTSNEWS_APPROVAL_DATABASE_URL", "Backend shadow database connection string for approval state.", true, true),
  variable("NUTSNEWS_APPROVAL_RABBITMQ_URL", "Private RabbitMQ connection string.", true, true),
  variable("NUTSNEWS_APPROVAL_QWEN_BASE_URL", "Private Qwen-compatible approval endpoint.", true, true),
  variable("NUTSNEWS_APPROVAL_QWEN_API_KEY", "Credential for the Qwen-compatible approval endpoint.", true, true),
  variable("NUTSNEWS_APPROVAL_QWEN_MODEL", "Model identifier used by the injected approval client.", false, false, "qwen2.5:3b"),
  variable("NUTSNEWS_APPROVAL_PROMPT_ID", "Versioned approval prompt identifier.", false, false, "editorial-approval-v1"),
  variable("NUTSNEWS_APPROVAL_TARGET_LANGUAGES", "Comma-separated translation target languages requested for accepted articles.", false, false, "fr,ja,de-CH,de,el"),
  variable("NUTSNEWS_APPROVAL_CONCURRENCY", "Maximum concurrent approval message handlers.", false, false, "2"),
  variable("NUTSNEWS_APPROVAL_PREFETCH", "Broker prefetch bound for approval deliveries.", false, false, "4"),
  variable("NUTSNEWS_APPROVAL_QWEN_TOTAL_TIMEOUT_MS", "Maximum approval endpoint call timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_APPROVAL_QWEN_MAX_INPUT_BYTES", "Maximum prompt input reference size accepted by approval.", false, false, "32768"),
  variable("NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS", "Maximum concurrent Qwen calls allowed per worker process.", false, false, "1"),
  variable("NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS", "Maximum in-process approval deliveries allowed to wait for Qwen capacity.", false, false, "3"),
  variable("NUTSNEWS_APPROVAL_QWEN_BACKPRESSURE_RETRY_AFTER_MS", "Retry delay returned when Qwen capacity is saturated.", false, false, "5000"),
  variable("NUTSNEWS_APPROVAL_SUMMARY_MIN_CHARS", "Minimum accepted source summary length.", false, false, "40"),
  variable("NUTSNEWS_APPROVAL_SUMMARY_MAX_CHARS", "Maximum accepted source summary length.", false, false, "600"),
  variable("NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ENABLED", "Protected flag for legacy OpenAI fallback. Default false.", false, false, "false"),
  variable("NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROTECTED_FLAG", "Operator confirmation required before legacy OpenAI fallback can run.", false, false, "false"),
  variable("NUTSNEWS_APPROVAL_OPENAI_FALLBACK_BUDGET_USD", "Maximum approved OpenAI fallback budget in USD.", false, false, "0"),
  variable("NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROVENANCE_MARKER", "Required provenance marker for any fallback decision.", false, false, ""),
  variable("NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ALERT_TOPIC", "Required alert topic or route when fallback is enabled.", false, false, ""),
  variable("NUTSNEWS_APPROVAL_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_APPROVAL_SHADOW_MODE", "Keep approval output isolated from legacy ingestion.", false, false, "true"),
  variable("NUTSNEWS_APPROVAL_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_APPROVAL_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true")
] as const satisfies readonly ApprovalConfigVariable[];

export interface ApprovalConfig {
  readonly serviceName: typeof APPROVAL_SERVICE_NAME;
  readonly serviceVersion: typeof APPROVAL_SERVICE_VERSION;
  readonly environment: string;
  readonly buildRevision: string;
  readonly host: string;
  readonly http: {
    readonly host: string;
    readonly port: number;
  };
  readonly dependencyMode: ApprovalDependencyMode;
  readonly dependencies: {
    readonly databaseConfigured: boolean;
    readonly rabbitmqConfigured: boolean;
    readonly qwenEndpointConfigured: boolean;
    readonly qwenCredentialConfigured: boolean;
  };
  readonly qwen: {
    readonly model: string;
    readonly promptId: string;
    readonly totalTimeoutMs: number;
    readonly maxInputBytes: number;
    readonly maxParallelCalls: number;
    readonly maxQueuedCalls: number;
    readonly backpressureRetryAfterMs: number;
  };
  readonly targetLanguages: readonly string[];
  readonly summary: {
    readonly minChars: number;
    readonly maxChars: number;
  };
  readonly openAiFallback: {
    readonly enabled: boolean;
    readonly protectedFlag: boolean;
    readonly budgetUsd: number;
    readonly provenanceMarker: string;
    readonly alertTopic: string;
  };
  readonly concurrency: number;
  readonly prefetch: number;
  readonly shutdownTimeoutMs: number;
  readonly shadowMode: boolean;
  readonly telemetryLogs: ApprovalTelemetryLogMode;
  readonly metricsEnabled: boolean;
}

export class ApprovalConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`Invalid approval configuration: ${issues.join("; ")}`);
    this.name = "ApprovalConfigError";
    this.issues = issues;
  }
}

export function loadApprovalConfig(env: NodeJS.ProcessEnv = process.env): ApprovalConfig {
  const issues: string[] = [];
  const dependencyMode = parseDependencyMode(env.NUTSNEWS_APPROVAL_DEPENDENCY_MODE, issues);
  const dependencies = {
    databaseConfigured: hasValue(env.NUTSNEWS_APPROVAL_DATABASE_URL),
    rabbitmqConfigured: hasValue(env.NUTSNEWS_APPROVAL_RABBITMQ_URL),
    qwenEndpointConfigured: hasValue(env.NUTSNEWS_APPROVAL_QWEN_BASE_URL),
    qwenCredentialConfigured: hasValue(env.NUTSNEWS_APPROVAL_QWEN_API_KEY)
  };

  if (dependencyMode === "production") {
    requireConfigured("NUTSNEWS_APPROVAL_DATABASE_URL", dependencies.databaseConfigured, issues);
    requireConfigured("NUTSNEWS_APPROVAL_RABBITMQ_URL", dependencies.rabbitmqConfigured, issues);
    requireConfigured("NUTSNEWS_APPROVAL_QWEN_BASE_URL", dependencies.qwenEndpointConfigured, issues);
    requireConfigured("NUTSNEWS_APPROVAL_QWEN_API_KEY", dependencies.qwenCredentialConfigured, issues);
  }

  const buildRevision = parseBuildRevision(env.NUTSNEWS_APPROVAL_BUILD_REVISION, dependencyMode, issues);

  const concurrency = parseInteger(env.NUTSNEWS_APPROVAL_CONCURRENCY, "NUTSNEWS_APPROVAL_CONCURRENCY", 2, 1, 16, issues);
  const prefetch = parseInteger(env.NUTSNEWS_APPROVAL_PREFETCH, "NUTSNEWS_APPROVAL_PREFETCH", 4, 1, 64, issues);
  const config: ApprovalConfig = {
    serviceName: APPROVAL_SERVICE_NAME,
    serviceVersion: APPROVAL_SERVICE_VERSION,
    environment: nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local"),
    buildRevision,
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    http: {
      host: nonEmpty(env.NUTSNEWS_APPROVAL_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_APPROVAL_HTTP_PORT, "NUTSNEWS_APPROVAL_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    dependencies,
    qwen: {
      model: nonEmpty(env.NUTSNEWS_APPROVAL_QWEN_MODEL, "qwen2.5:3b"),
      promptId: nonEmpty(env.NUTSNEWS_APPROVAL_PROMPT_ID, "editorial-approval-v1"),
      totalTimeoutMs: parseInteger(env.NUTSNEWS_APPROVAL_QWEN_TOTAL_TIMEOUT_MS, "NUTSNEWS_APPROVAL_QWEN_TOTAL_TIMEOUT_MS", 30_000, 1_000, 180_000, issues),
      maxInputBytes: parseInteger(env.NUTSNEWS_APPROVAL_QWEN_MAX_INPUT_BYTES, "NUTSNEWS_APPROVAL_QWEN_MAX_INPUT_BYTES", 32_768, 1_024, 1_048_576, issues),
      maxParallelCalls: parseInteger(env.NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS, "NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS", 1, 1, 16, issues),
      maxQueuedCalls: parseInteger(env.NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS, "NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS", 3, 0, 64, issues),
      backpressureRetryAfterMs: parseInteger(env.NUTSNEWS_APPROVAL_QWEN_BACKPRESSURE_RETRY_AFTER_MS, "NUTSNEWS_APPROVAL_QWEN_BACKPRESSURE_RETRY_AFTER_MS", 5_000, 1_000, 600_000, issues)
    },
    targetLanguages: parseList(env.NUTSNEWS_APPROVAL_TARGET_LANGUAGES, "NUTSNEWS_APPROVAL_TARGET_LANGUAGES", "fr,ja,de-CH,de,el", issues),
    summary: {
      minChars: parseInteger(env.NUTSNEWS_APPROVAL_SUMMARY_MIN_CHARS, "NUTSNEWS_APPROVAL_SUMMARY_MIN_CHARS", 40, 1, 1_000, issues),
      maxChars: parseInteger(env.NUTSNEWS_APPROVAL_SUMMARY_MAX_CHARS, "NUTSNEWS_APPROVAL_SUMMARY_MAX_CHARS", 600, 40, 4_000, issues)
    },
    openAiFallback: {
      enabled: parseBoolean(env.NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ENABLED, "NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ENABLED", false, issues),
      protectedFlag: parseBoolean(env.NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROTECTED_FLAG, "NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROTECTED_FLAG", false, issues),
      budgetUsd: parseMoney(env.NUTSNEWS_APPROVAL_OPENAI_FALLBACK_BUDGET_USD, "NUTSNEWS_APPROVAL_OPENAI_FALLBACK_BUDGET_USD", 0, 0, 10_000, issues),
      provenanceMarker: nonEmpty(env.NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROVENANCE_MARKER, ""),
      alertTopic: nonEmpty(env.NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ALERT_TOPIC, "")
    },
    concurrency,
    prefetch,
    shutdownTimeoutMs: parseInteger(env.NUTSNEWS_APPROVAL_SHUTDOWN_TIMEOUT_MS, "NUTSNEWS_APPROVAL_SHUTDOWN_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    shadowMode: parseBoolean(env.NUTSNEWS_APPROVAL_SHADOW_MODE, "NUTSNEWS_APPROVAL_SHADOW_MODE", true, issues),
    telemetryLogs: parseTelemetryLogMode(env.NUTSNEWS_APPROVAL_TELEMETRY_LOGS, issues),
    metricsEnabled: parseBoolean(env.NUTSNEWS_APPROVAL_METRICS_ENABLED, "NUTSNEWS_APPROVAL_METRICS_ENABLED", true, issues)
  };

  if (config.prefetch < config.concurrency) {
    issues.push("NUTSNEWS_APPROVAL_PREFETCH must be greater than or equal to NUTSNEWS_APPROVAL_CONCURRENCY.");
  }

  if (!config.shadowMode) {
    issues.push("NUTSNEWS_APPROVAL_SHADOW_MODE must remain true until backend-owned deployment enables cutover.");
  }

  if (config.summary.maxChars < config.summary.minChars) {
    issues.push("NUTSNEWS_APPROVAL_SUMMARY_MAX_CHARS must be greater than or equal to NUTSNEWS_APPROVAL_SUMMARY_MIN_CHARS.");
  }

  if (config.qwen.maxParallelCalls > config.concurrency) {
    issues.push("NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS must be less than or equal to NUTSNEWS_APPROVAL_CONCURRENCY.");
  }

  if (config.qwen.maxParallelCalls + config.qwen.maxQueuedCalls > config.prefetch) {
    issues.push("NUTSNEWS_APPROVAL_QWEN_MAX_PARALLEL_CALLS plus NUTSNEWS_APPROVAL_QWEN_MAX_QUEUED_CALLS must be less than or equal to NUTSNEWS_APPROVAL_PREFETCH.");
  }

  if (config.openAiFallback.enabled) {
    if (!config.openAiFallback.protectedFlag) {
      issues.push("NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROTECTED_FLAG must be true when fallback is enabled.");
    }

    if (config.openAiFallback.budgetUsd <= 0) {
      issues.push("NUTSNEWS_APPROVAL_OPENAI_FALLBACK_BUDGET_USD must be greater than 0 when fallback is enabled.");
    }

    if (config.openAiFallback.provenanceMarker !== "legacy_openai_fallback") {
      issues.push("NUTSNEWS_APPROVAL_OPENAI_FALLBACK_PROVENANCE_MARKER must be legacy_openai_fallback when fallback is enabled.");
    }

    if (config.openAiFallback.alertTopic.length === 0) {
      issues.push("NUTSNEWS_APPROVAL_OPENAI_FALLBACK_ALERT_TOPIC is required when fallback is enabled.");
    }
  }

  if (issues.length > 0) {
    throw new ApprovalConfigError(issues);
  }

  return config;
}

function variable(
  name: string,
  description: string,
  requiredInProduction: boolean,
  sensitive: boolean,
  defaultValue?: string
): ApprovalConfigVariable {
  return {
    name,
    description,
    requiredInProduction,
    sensitive,
    ...(defaultValue === undefined ? {} : {
      defaultValue
    })
  };
}

function nonEmpty(value: string | undefined, fallback: string): string {
  if (value === undefined) {
    return fallback;
  }

  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : fallback;
}

function hasValue(value: string | undefined): boolean {
  return value !== undefined && value.trim().length > 0;
}

function parseDependencyMode(value: string | undefined, issues: string[]): ApprovalDependencyMode {
  const normalized = nonEmpty(value, "test");

  if (normalized === "test" || normalized === "production") {
    return normalized;
  }

  issues.push("NUTSNEWS_APPROVAL_DEPENDENCY_MODE must be test or production.");
  return "test";
}

function parseBuildRevision(
  value: string | undefined,
  dependencyMode: ApprovalDependencyMode,
  issues: string[]
): string {
  const revision = nonEmpty(value, "development");

  if (dependencyMode === "production" && !/^[0-9a-f]{40}$/u.test(revision)) {
    issues.push("NUTSNEWS_APPROVAL_BUILD_REVISION must be a lowercase 40-character Git commit SHA when NUTSNEWS_APPROVAL_DEPENDENCY_MODE=production.");
  }

  return revision;
}

function parseTelemetryLogMode(value: string | undefined, issues: string[]): ApprovalTelemetryLogMode {
  const normalized = nonEmpty(value, "stdout");

  if (normalized === "stdout" || normalized === "silent") {
    return normalized;
  }

  issues.push("NUTSNEWS_APPROVAL_TELEMETRY_LOGS must be stdout or silent.");
  return "stdout";
}

function parseBoolean(
  value: string | undefined,
  key: string,
  fallback: boolean,
  issues: string[]
): boolean {
  if (!hasValue(value)) {
    return fallback;
  }

  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();

  if (normalized === "true" || normalized === "1") {
    return true;
  }

  if (normalized === "false" || normalized === "0") {
    return false;
  }

  issues.push(`${key} must be true or false.`);
  return fallback;
}

function parseInteger(
  value: string | undefined,
  key: string,
  fallback: number,
  min: number,
  max: number,
  issues: string[]
): number {
  if (!hasValue(value)) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    issues.push(`${key} must be an integer between ${String(min)} and ${String(max)}.`);
    return fallback;
  }

  return parsed;
}

function parseMoney(
  value: string | undefined,
  key: string,
  fallback: number,
  min: number,
  max: number,
  issues: string[]
): number {
  if (!hasValue(value)) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed < min || parsed > max) {
    issues.push(`${key} must be a decimal number between ${String(min)} and ${String(max)}.`);
    return fallback;
  }

  return Math.round(parsed * 100) / 100;
}

function parseList(value: string | undefined, key: string, fallback: string, issues: string[]): readonly string[] {
  const entries = nonEmpty(value, fallback)
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);

  if (entries.length === 0) {
    issues.push(`${key} must include at least one value.`);
    return fallback.split(",");
  }

  return Array.from(new Set(entries));
}

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_APPROVAL_DEPENDENCY_MODE=production.`);
  }
}
