import { getRuntimePackageMetadata } from "@ramideltoro/nutsnews-worker-runtime";
import { describe, expect, it } from "vitest";

import { SUPPORTED_RUNTIME_PACKAGE_VERSION } from "../src/index.js";

describe("package compatibility", () => {
  it("accepts the installed worker runtime release", () => {
    expect(getRuntimePackageMetadata().packageVersion).toBe(SUPPORTED_RUNTIME_PACKAGE_VERSION);
    expect(SUPPORTED_RUNTIME_PACKAGE_VERSION).toBe("0.5.0");
  });
});
