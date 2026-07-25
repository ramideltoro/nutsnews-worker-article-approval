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
  variable("NUTSNEWS_APPROVAL_HTTP_HOST", "Health and metrics bind host.", false, false, "0.0.0.0"),
  variable("NUTSNEWS_APPROVAL_HTTP_PORT", "Health and metrics bind port.", false, false, "8080"),
  variable("NUTSNEWS_APPROVAL_DEPENDENCY_MODE", "Use test dependencies locally or require production dependency presence.", false, false, "test"),
  variable("NUTSNEWS_APPROVAL_DATABASE_URL", "Backend shadow database connection string for approval state.", true, true),
  variable("NUTSNEWS_APPROVAL_RABBITMQ_URL", "Private RabbitMQ connection string.", true, true),
  variable("NUTSNEWS_APPROVAL_QWEN_BASE_URL", "Private Qwen-compatible approval endpoint.", true, true),
  variable("NUTSNEWS_APPROVAL_QWEN_API_KEY", "Credential for the Qwen-compatible approval endpoint.", true, true),
  variable("NUTSNEWS_APPROVAL_QWEN_MODEL", "Model identifier used by the injected approval client.", false, false, "qwen2.5:3b"),
  variable("NUTSNEWS_APPROVAL_CONCURRENCY", "Maximum concurrent approval message handlers.", false, false, "2"),
  variable("NUTSNEWS_APPROVAL_PREFETCH", "Broker prefetch bound for approval deliveries.", false, false, "4"),
  variable("NUTSNEWS_APPROVAL_QWEN_TOTAL_TIMEOUT_MS", "Maximum approval endpoint call timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_APPROVAL_QWEN_MAX_INPUT_BYTES", "Maximum prompt input reference size accepted by approval.", false, false, "32768"),
  variable("NUTSNEWS_APPROVAL_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_APPROVAL_SHADOW_MODE", "Keep approval output isolated from legacy ingestion.", false, false, "true"),
  variable("NUTSNEWS_APPROVAL_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_APPROVAL_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true")
] as const satisfies readonly ApprovalConfigVariable[];

export interface ApprovalConfig {
  readonly serviceName: typeof APPROVAL_SERVICE_NAME;
  readonly serviceVersion: typeof APPROVAL_SERVICE_VERSION;
  readonly environment: string;
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
    readonly totalTimeoutMs: number;
    readonly maxInputBytes: number;
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

  const concurrency = parseInteger(env.NUTSNEWS_APPROVAL_CONCURRENCY, "NUTSNEWS_APPROVAL_CONCURRENCY", 2, 1, 16, issues);
  const prefetch = parseInteger(env.NUTSNEWS_APPROVAL_PREFETCH, "NUTSNEWS_APPROVAL_PREFETCH", 4, 1, 64, issues);
  const config: ApprovalConfig = {
    serviceName: APPROVAL_SERVICE_NAME,
    serviceVersion: APPROVAL_SERVICE_VERSION,
    environment: nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local"),
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    http: {
      host: nonEmpty(env.NUTSNEWS_APPROVAL_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_APPROVAL_HTTP_PORT, "NUTSNEWS_APPROVAL_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    dependencies,
    qwen: {
      model: nonEmpty(env.NUTSNEWS_APPROVAL_QWEN_MODEL, "qwen2.5:3b"),
      totalTimeoutMs: parseInteger(env.NUTSNEWS_APPROVAL_QWEN_TOTAL_TIMEOUT_MS, "NUTSNEWS_APPROVAL_QWEN_TOTAL_TIMEOUT_MS", 30_000, 1_000, 180_000, issues),
      maxInputBytes: parseInteger(env.NUTSNEWS_APPROVAL_QWEN_MAX_INPUT_BYTES, "NUTSNEWS_APPROVAL_QWEN_MAX_INPUT_BYTES", 32_768, 1_024, 1_048_576, issues)
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

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_APPROVAL_DEPENDENCY_MODE=production.`);
  }
}
