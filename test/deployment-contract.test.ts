import { readFile } from "node:fs/promises";

import {
  describe,
  expect,
  it
} from "vitest";

describe("translation image identity contract", () => {
  it("bakes the immutable revision and uses liveness for shadow container health", async () => {
    const [dockerfile, ciWorkflow, publishWorkflow] = await Promise.all([
      readFile(new URL("../Dockerfile", import.meta.url), "utf8"),
      readFile(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8"),
      readFile(new URL("../.github/workflows/publish-container.yml", import.meta.url), "utf8")
    ]);

    expect(dockerfile).toContain("ARG BUILD_REVISION=development");
    expect(dockerfile).toContain("NUTSNEWS_TRANSLATION_BUILD_REVISION=${BUILD_REVISION}");
    expect(dockerfile).toContain("http://127.0.0.1:8080/live");
    expect(ciWorkflow).toContain("--build-arg BUILD_REVISION=${{ github.sha }}");
    expect(publishWorkflow).toContain("BUILD_REVISION=${{ github.sha }}");
  });
});
