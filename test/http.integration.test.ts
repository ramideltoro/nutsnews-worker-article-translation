import { createPrometheusRuntimeTelemetrySink } from "@ramideltoro/nutsnews-worker-runtime";
import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import { loadTranslationConfig } from "../src/config.js";
import {
  createTranslationHttpServer,
  type TranslationHttpServer
} from "../src/http.js";
import {
  type TranslationReconciliationReport,
  type TranslationReconciler
} from "../src/reconciliation.js";
import { createTranslationService } from "../src/service.js";
import { createLocalTranslationDependencies } from "../src/test-doubles.js";

let activeServer: TranslationHttpServer | undefined;

afterEach(async () => {
  if (activeServer !== undefined) {
    await activeServer.close();
    activeServer = undefined;
  }
});

describe("translation HTTP endpoints", () => {
  it("serves liveness, readiness, startup, metrics, and value-free config schema", async () => {
    const config = loadTranslationConfig({
      NUTSNEWS_TRANSLATION_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_TRANSLATION_HTTP_PORT: "0",
      NUTSNEWS_TRANSLATION_TELEMETRY_LOGS: "silent"
    });
    const metrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    const service = createTranslationService({
      config,
      dependencies: createLocalTranslationDependencies(),
      metrics
    });
    activeServer = createTranslationHttpServer({
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

    expect(schema.variables.some((variable) => variable.name === "NUTSNEWS_TRANSLATION_QWEN_API_KEY" && variable.sensitive)).toBe(true);
    expect(JSON.stringify(schema)).not.toContain("amqp://");
    expect(JSON.stringify(schema)).not.toContain("postgres://");
    expect(JSON.stringify(schema)).not.toContain("sk-");

    await service.stop();
  });

  it("protects the reconciliation endpoint with bearer auth", async () => {
    const config = loadTranslationConfig({
      NUTSNEWS_TRANSLATION_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_TRANSLATION_HTTP_PORT: "0",
      NUTSNEWS_TRANSLATION_TELEMETRY_LOGS: "silent"
    });
    const service = createTranslationService({
      config,
      dependencies: createLocalTranslationDependencies()
    });
    const reconciler: TranslationReconciler = {
      name: "test-reconciler",
      reconcile: (request) => Promise.resolve({
        service: "translation",
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
      } satisfies TranslationReconciliationReport)
    };
    activeServer = createTranslationHttpServer({
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

async function expectJsonStatus(url: string, statusCode: number, status: string): Promise<void> {
  const response = await fetch(url);
  const body = await response.json() as { readonly status: string };

  expect(response.status).toBe(statusCode);
  expect(body.status).toBe(status);
}
