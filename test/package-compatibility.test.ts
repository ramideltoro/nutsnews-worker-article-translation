import { readFileSync } from "node:fs";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import { getRuntimePackageMetadata } from "@ramideltoro/nutsnews-worker-runtime";
import { describe, expect, it } from "vitest";

import {
  SUPPORTED_CONTRACT_PACKAGE_VERSION,
  SUPPORTED_RUNTIME_PACKAGE_VERSION
} from "../src/index.js";

const CONTRACTS_PACKAGE = "@ramideltoro/nutsnews-worker-contracts";
const RUNTIME_PACKAGE = "@ramideltoro/nutsnews-worker-runtime";

function asRecord(value: unknown, name: string): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${name} must be an object.`);
  }

  return value as Readonly<Record<string, unknown>>;
}

function stringField(record: Readonly<Record<string, unknown>>, field: string): string {
  const value = record[field];

  if (typeof value !== "string") {
    throw new Error(`${field} must be a string.`);
  }

  return value;
}

describe("package compatibility", () => {
  it("accepts the installed worker runtime release", () => {
    const contracts = getContractPackageMetadata();
    const runtime = getRuntimePackageMetadata();

    expect(contracts.packageVersion).toBe(SUPPORTED_CONTRACT_PACKAGE_VERSION);
    expect(runtime.packageVersion).toBe(SUPPORTED_RUNTIME_PACKAGE_VERSION);
    expect(runtime.contractsPackageVersion).toBe(SUPPORTED_CONTRACT_PACKAGE_VERSION);
    expect(SUPPORTED_CONTRACT_PACKAGE_VERSION).toBe("1.0.0");
    expect(SUPPORTED_RUNTIME_PACKAGE_VERSION).toBe("1.0.0");
  });

  it("locks both shared packages to immutable GitHub Packages artifacts", () => {
    const parsedManifest: unknown = JSON.parse(
      readFileSync(new URL("../package.json", import.meta.url), "utf8")
    );
    const parsedLockfile: unknown = JSON.parse(
      readFileSync(new URL("../package-lock.json", import.meta.url), "utf8")
    );
    const manifest = asRecord(parsedManifest, "package.json");
    const manifestDependencies = asRecord(manifest.dependencies, "package.json dependencies");
    const lockfile = asRecord(parsedLockfile, "package-lock.json");
    const packages = asRecord(lockfile.packages, "package-lock.json packages");
    const root = asRecord(packages[""], "package-lock.json root package");
    const rootDependencies = asRecord(root.dependencies, "package-lock.json root dependencies");
    const contracts = asRecord(
      packages[`node_modules/${CONTRACTS_PACKAGE}`],
      "contracts lock entry"
    );
    const runtime = asRecord(
      packages[`node_modules/${RUNTIME_PACKAGE}`],
      "runtime lock entry"
    );
    const runtimeDependencies = asRecord(runtime.dependencies, "runtime lock dependencies");

    expect(manifestDependencies[CONTRACTS_PACKAGE]).toBe("1.0.0");
    expect(manifestDependencies[RUNTIME_PACKAGE]).toBe("1.0.0");
    expect(manifest).not.toHaveProperty("overrides");
    expect(rootDependencies[CONTRACTS_PACKAGE]).toBe("1.0.0");
    expect(rootDependencies[RUNTIME_PACKAGE]).toBe("1.0.0");
    expect(stringField(contracts, "version")).toBe("1.0.0");
    expect(stringField(runtime, "version")).toBe("1.0.0");
    expect(runtimeDependencies[CONTRACTS_PACKAGE]).toBe("1.0.0");
    expect(stringField(contracts, "resolved")).toMatch(
      /^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-contracts\/1\.0\.0\/[a-f0-9]+$/u
    );
    expect(stringField(runtime, "resolved")).toMatch(
      /^https:\/\/npm\.pkg\.github\.com\/download\/@ramideltoro\/nutsnews-worker-runtime\/1\.0\.0\/[a-f0-9]+$/u
    );
    expect(stringField(contracts, "integrity")).toMatch(/^sha512-[A-Za-z0-9+/]+={0,2}$/u);
    expect(stringField(runtime, "integrity")).toMatch(/^sha512-[A-Za-z0-9+/]+={0,2}$/u);
  });
});
