import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import { loadApprovalConfig } from "../src/config.js";
import {
  createApprovalHttpServer,
  type ApprovalHttpServer
} from "../src/http.js";
import { createApprovalPrometheusTelemetrySink } from "../src/metrics.js";
import type {
  ApprovalReconciliationReport,
  ApprovalReconciler
} from "../src/reconciliation.js";
import { createApprovalService } from "../src/service.js";
import {
  LocalApprovalQwenClient,
  createLocalApprovalDependencies,
  createMinimalApprovalDelivery
} from "../src/test-doubles.js";

let activeServer: ApprovalHttpServer | undefined;

afterEach(async () => {
  if (activeServer !== undefined) {
    await activeServer.close();
    activeServer = undefined;
  }
});

describe("approval HTTP endpoints", () => {
  it("serves liveness, readiness, startup, metrics, and value-free config schema", async () => {
    const config = loadApprovalConfig({
      NUTSNEWS_APPROVAL_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_APPROVAL_HTTP_PORT: "0",
      NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
    });
    const metrics = createApprovalPrometheusTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    const dependencies = createLocalApprovalDependencies();
    const service = createApprovalService({
      config,
      dependencies,
      telemetry: metrics,
      metrics
    });
    activeServer = createApprovalHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await service.processDelivery(createMinimalApprovalDelivery());
    await activeServer.listen();

    await expectJsonStatus(activeServer.url("/live"), 200, "ok");
    await expectJsonStatus(activeServer.url("/startup"), 200, "ok");
    await expectJsonStatus(activeServer.url("/ready"), 200, "ok");

    const metricsResponse = await fetch(activeServer.url("/metrics"));
    expect(metricsResponse.status).toBe(200);
    const metricsBody = await metricsResponse.text();
    expect(metricsBody).not.toContain("nutsnews_worker_dependency_duration_ms");
    expect(metricsBody).toContain("nutsnews_worker_uplift_stage_events_total");
    expect(metricsBody).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="approval",le="30"} 1');
    expect(metricsBody).toContain('nutsnews_worker_expected_active{environment="local",service="nutsnews-worker-article-approval"} 0');
    expectMetricSample(metricsBody, "nutsnews_worker_health_probe", {
      probe: "liveness",
      outcome: "ok"
    }, 1);
    expectMetricSample(metricsBody, "nutsnews_worker_health_probe", {
      probe: "startup",
      outcome: "ok"
    }, 1);
    expectMetricSample(metricsBody, "nutsnews_worker_health_probe", {
      probe: "readiness",
      outcome: "ok"
    }, 1);
    expect(metricsBody).toContain('check="qwen-client"');
    expect(metricsBody).not.toContain('check="other"');

    (dependencies.qwenClient as LocalApprovalQwenClient).status = "unhealthy";
    const staleSafeMetricsResponse = await fetch(activeServer.url("/metrics"));
    const staleSafeMetricsBody = await staleSafeMetricsResponse.text();

    expectMetricSample(staleSafeMetricsBody, "nutsnews_worker_health_probe", {
      probe: "readiness",
      outcome: "unhealthy"
    }, 1);
    expectMetricSample(staleSafeMetricsBody, "nutsnews_worker_health_check", {
      probe: "readiness",
      check: "qwen-client",
      outcome: "unhealthy"
    }, 1);

    const schemaResponse = await fetch(activeServer.url("/config-schema"));
    expect(schemaResponse.status).toBe(200);
    const schema = await schemaResponse.json() as { readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[] };

    expect(schema.variables.some((variable) => variable.name === "NUTSNEWS_APPROVAL_QWEN_API_KEY" && variable.sensitive)).toBe(true);
    expect(JSON.stringify(schema)).not.toContain("amqp://");
    expect(JSON.stringify(schema)).not.toContain("postgres://");
    expect(JSON.stringify(schema)).not.toContain("sk-");

    await service.stop();
  });

  it("protects the reconciliation endpoint with bearer auth", async () => {
    const config = loadApprovalConfig({
      NUTSNEWS_APPROVAL_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_APPROVAL_HTTP_PORT: "0",
      NUTSNEWS_APPROVAL_TELEMETRY_LOGS: "silent"
    });
    const service = createApprovalService({
      config,
      dependencies: createLocalApprovalDependencies()
    });
    const reconciler: ApprovalReconciler = {
      name: "test-reconciler",
      reconcile: (request) => Promise.resolve({
        service: "approval",
        mode: request.mode,
        status: "dry_run",
        requestedAt: "2026-07-23T00:00:00.000Z",
        maxItems: 1,
        minAgeSeconds: 900,
        selectedCount: 0,
        replayedCount: 0,
        failedClosedCount: 0,
        skippedCount: 0,
        writesPerformed: false,
        dryRun: true,
        productionVisibilityEnabled: false,
        legacyRuntimeRequired: false,
        protectedApplyRequired: true,
        candidates: [],
        errors: [],
        metrics: {
          candidateCount: 0,
          replayedCount: 0,
          failedClosedCount: 0,
          skippedCount: 0
        }
      } satisfies ApprovalReconciliationReport)
    };
    activeServer = createApprovalHttpServer({
      config,
      service,
      reconciler,
      reconciliationToken: "test-token"
    });

    await service.start();
    await activeServer.listen();

    const unauthorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(unauthorized.status).toBe(401);

    const authorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      headers: {
        authorization: "Bearer test-token"
      },
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(authorized.status).toBe(200);
    await expect(authorized.json()).resolves.toMatchObject({
      status: "dry_run",
      writesPerformed: false,
      productionVisibilityEnabled: false
    });

    await service.stop();
  });
});

function expectMetricSample(
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

async function expectJsonStatus(url: string, statusCode: number, status: string): Promise<void> {
  const response = await fetch(url);
  const body = await response.json() as { readonly status: string };

  expect(response.status).toBe(statusCode);
  expect(body.status).toBe(status);
}
