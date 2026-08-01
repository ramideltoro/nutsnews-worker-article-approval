import {
  describe,
  expect,
  it
} from "vitest";

import { createApprovalPrometheusTelemetrySink } from "../src/metrics.js";

const IDENTITY = {
  service: "nutsnews-worker-article-approval",
  version: "0.1.0",
  environment: "production",
  host: "backend-vps",
  revision: "0123456789abcdef0123456789abcdef01234567",
  deployment: "shadow",
  adapter: "production"
} as const;

describe("approval Runtime1 health metric compatibility", () => {
  it("forwards health events and exposes each Runtime-owned health family exactly once", async () => {
    const metrics = createApprovalPrometheusTelemetrySink({
      identity: IDENTITY
    });

    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-08-01T00:00:01.000Z",
      outcome: "ok",
      attributes: {
        probe: "readiness",
        checks: [
          {
            name: "qwen-client",
            status: "ok",
            durationMs: 12
          }
        ]
      }
    });

    const output = metrics.collect();
    const lines = output.split("\n");
    const healthProbeSamples = samples(lines, "nutsnews_worker_health_probe");
    const healthCheckSamples = samples(lines, "nutsnews_worker_health_check");

    expectFamilyMetadataOnce(lines, "nutsnews_worker_health_probe", "gauge");
    expectFamilyMetadataOnce(lines, "nutsnews_worker_health_check", "gauge");
    expectFamilyMetadataOnce(lines, "nutsnews_worker_health_check_duration_seconds", "histogram");
    expect(healthProbeSamples).toHaveLength(9);
    expect(healthCheckSamples).toHaveLength(3);
    expect(new Set(healthProbeSamples.map(seriesKey)).size).toBe(healthProbeSamples.length);
    expect(new Set(healthCheckSamples.map(seriesKey)).size).toBe(healthCheckSamples.length);
    expectSampleValue(output, "nutsnews_worker_health_check", {
      probe: "readiness",
      check: "qwen-client",
      outcome: "ok"
    }, 1);
    expectSampleValue(output, "nutsnews_worker_health_check", {
      probe: "readiness",
      check: "qwen-client",
      outcome: "unhealthy"
    }, 0);
    expect(output).toContain('probe="readiness",check="qwen-client",le="0.025"} 1');
    expect(output).toContain('probe="readiness",check="qwen-client"} 0.012');
    expect(output).not.toContain('check="other"');
  });

  it("leaves expected-active ownership to Runtime and advances last success monotonically", async () => {
    const metrics = createApprovalPrometheusTelemetrySink({
      identity: IDENTITY
    });

    await metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-08-01T00:10:00.000Z",
      stage: "approval",
      queue: "nutsnews.worker.approval.v1",
      outcome: "success"
    });
    await metrics.emit({
      name: "runtime.message.duplicate",
      level: "info",
      at: "2026-08-01T00:05:00.000Z",
      stage: "approval",
      queue: "nutsnews.worker.approval.v1",
      outcome: "duplicate"
    });

    const lines = metrics.collect().split("\n");
    const expectedTimestamp = Math.floor(Date.parse("2026-08-01T00:10:00.000Z") / 1_000);

    expectFamilyMetadataOnce(lines, "nutsnews_worker_expected_active", "gauge");
    expectFamilyMetadataOnce(lines, "nutsnews_worker_last_success_timestamp_seconds", "gauge");
    expect(samples(lines, "nutsnews_worker_expected_active")).toEqual([
      'nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-approval"} 0'
    ]);
    expect(samples(lines, "nutsnews_worker_last_success_timestamp_seconds")).toEqual([
      `nutsnews_worker_last_success_timestamp_seconds{environment="production",service="nutsnews-worker-article-approval"} ${String(expectedTimestamp)}`
    ]);

    const epochMetrics = createApprovalPrometheusTelemetrySink({
      identity: IDENTITY
    });

    await epochMetrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "1970-01-01T00:00:00.000Z",
      stage: "approval",
      queue: "nutsnews.worker.approval.v1",
      outcome: "success"
    });
    expect(samples(
      epochMetrics.collect().split("\n"),
      "nutsnews_worker_last_success_timestamp_seconds"
    )).toEqual([
      'nutsnews_worker_last_success_timestamp_seconds{environment="production",service="nutsnews-worker-article-approval"} 0'
    ]);
  });
});

function samples(lines: readonly string[], metric: string): readonly string[] {
  return lines.filter((line) => line.startsWith(`${metric}{`));
}

function seriesKey(line: string): string {
  return line.slice(0, line.lastIndexOf(" "));
}

function expectSampleValue(
  output: string,
  metric: string,
  labels: Readonly<Record<string, string>>,
  expectedValue: number
): void {
  const matches = output
    .split("\n")
    .filter((line) => line.startsWith(`${metric}{`)
      && Object.entries(labels).every(([name, value]) => line.includes(`${name}="${value}"`)));

  expect(matches).toHaveLength(1);
  expect(Number(matches[0]?.split(" ").at(-1))).toBe(expectedValue);
}

function expectFamilyMetadataOnce(
  lines: readonly string[],
  metric: string,
  type: "gauge" | "histogram"
): void {
  expect(lines.filter((line) => line.startsWith(`# HELP ${metric} `))).toHaveLength(1);
  expect(lines.filter((line) => line === `# TYPE ${metric} ${type}`)).toHaveLength(1);
}
