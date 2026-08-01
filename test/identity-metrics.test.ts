import {
  describe,
  expect,
  it
} from "vitest";

import { APPROVAL_CONFIG_SCHEMA } from "../src/config.js";
import { createApprovalPrometheusTelemetrySink } from "../src/metrics.js";

const BUILD_REVISION = "0123456789abcdef0123456789abcdef01234567";

describe("approval immutable telemetry identity", () => {
  it("exports one bounded non-unknown build and deployment series", () => {
    const output = createApprovalPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-approval",
        version: "0.1.0",
        environment: "production",
        host: "backend-vps",
        revision: BUILD_REVISION,
        deployment: "shadow",
        adapter: "production"
      }
    }).collect();
    const identitySamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_build_info{")
      || line.startsWith("nutsnews_worker_deployment_info{"));
    const expectedActiveSamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_expected_active{"));

    expect(identitySamples).toHaveLength(2);
    expect(expectedActiveSamples).toEqual([
      'nutsnews_worker_expected_active{environment="production",service="nutsnews-worker-article-approval"} 0'
    ]);
    expect(identitySamples.join("\n")).toContain(`revision="${BUILD_REVISION}"`);
    expect(identitySamples.join("\n")).toContain('deployment="shadow"');
    expect(identitySamples.join("\n")).toContain('adapter="production"');
    expect(identitySamples.join("\n")).not.toContain("unknown");
  });

  it("declares the immutable revision as required and non-sensitive in production", () => {
    expect(APPROVAL_CONFIG_SCHEMA.find((variable) => variable.name === "NUTSNEWS_APPROVAL_BUILD_REVISION")).toMatchObject({
      requiredInProduction: true,
      sensitive: false
    });
  });
});
