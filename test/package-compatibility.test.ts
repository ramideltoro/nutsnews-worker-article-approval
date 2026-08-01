import { readFile } from "node:fs/promises";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import { getRuntimePackageMetadata } from "@ramideltoro/nutsnews-worker-runtime";
import { describe, expect, it } from "vitest";

import {
  SUPPORTED_CONTRACTS_PACKAGE_VERSION,
  SUPPORTED_RUNTIME_PACKAGE_VERSION
} from "../src/index.js";

describe("package compatibility", () => {
  it("accepts the installed worker runtime release", () => {
    const contracts = getContractPackageMetadata();
    const runtime = getRuntimePackageMetadata();

    expect(contracts.packageVersion).toBe(SUPPORTED_CONTRACTS_PACKAGE_VERSION);
    expect(runtime.packageVersion).toBe(SUPPORTED_RUNTIME_PACKAGE_VERSION);
    expect(runtime.contractsPackageVersion).toBe(SUPPORTED_CONTRACTS_PACKAGE_VERSION);
    expect(SUPPORTED_CONTRACTS_PACKAGE_VERSION).toBe("1.0.0");
    expect(SUPPORTED_RUNTIME_PACKAGE_VERSION).toBe("1.0.0");
  });

  it("locks both packages to immutable GitHub Packages artifacts without overrides", async () => {
    const manifest = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8")) as {
      readonly dependencies?: Readonly<Record<string, string>>;
      readonly overrides?: unknown;
    };
    const lock = JSON.parse(await readFile(new URL("../package-lock.json", import.meta.url), "utf8")) as {
      readonly packages?: Readonly<Record<string, {
        readonly version?: string;
        readonly resolved?: string;
        readonly integrity?: string;
        readonly dependencies?: Readonly<Record<string, string>>;
      }>>;
    };
    const contracts = lock.packages?.["node_modules/@ramideltoro/nutsnews-worker-contracts"];
    const runtime = lock.packages?.["node_modules/@ramideltoro/nutsnews-worker-runtime"];

    expect(manifest.dependencies?.["@ramideltoro/nutsnews-worker-contracts"]).toBe("1.0.0");
    expect(manifest.dependencies?.["@ramideltoro/nutsnews-worker-runtime"]).toBe("1.0.0");
    expect(manifest.overrides).toBeUndefined();
    expect(contracts).toMatchObject({
      version: "1.0.0"
    });
    expect(contracts?.resolved).toMatch(/^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-contracts\/1\.0\.0\//u);
    expect(contracts?.integrity).toMatch(/^sha512-/u);
    expect(runtime).toMatchObject({
      version: "1.0.0"
    });
    expect(runtime?.resolved).toMatch(/^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-runtime\/1\.0\.0\//u);
    expect(runtime?.integrity).toMatch(/^sha512-/u);
    expect(runtime?.dependencies?.["@ramideltoro/nutsnews-worker-contracts"]).toBe("1.0.0");
  });
});
