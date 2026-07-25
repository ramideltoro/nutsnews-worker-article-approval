import { createPrometheusRuntimeTelemetrySink } from "@ramideltoro/nutsnews-worker-runtime";
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
import { createApprovalService } from "../src/service.js";
import { createLocalApprovalDependencies } from "../src/test-doubles.js";

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
    const metrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    const service = createApprovalService({
      config,
      dependencies: createLocalApprovalDependencies(),
      metrics
    });
    activeServer = createApprovalHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await activeServer.listen();

    await expectJsonStatus(activeServer.url("/live"), 200, "ok");
    await expectJsonStatus(activeServer.url("/startup"), 200, "ok");
    await expectJsonStatus(activeServer.url("/ready"), 200, "ok");

    const metricsResponse = await fetch(activeServer.url("/metrics"));
    expect(metricsResponse.status).toBe(200);
    expect(await metricsResponse.text()).toContain("nutsnews_worker_dependency_duration_ms");

    const schemaResponse = await fetch(activeServer.url("/config-schema"));
    expect(schemaResponse.status).toBe(200);
    const schema = await schemaResponse.json() as { readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[] };

    expect(schema.variables.some((variable) => variable.name === "NUTSNEWS_APPROVAL_QWEN_API_KEY" && variable.sensitive)).toBe(true);
    expect(JSON.stringify(schema)).not.toContain("amqp://");
    expect(JSON.stringify(schema)).not.toContain("postgres://");
    expect(JSON.stringify(schema)).not.toContain("sk-");

    await service.stop();
  });
});

async function expectJsonStatus(url: string, statusCode: number, status: string): Promise<void> {
  const response = await fetch(url);
  const body = await response.json() as { readonly status: string };

  expect(response.status).toBe(statusCode);
  expect(body.status).toBe(status);
}
