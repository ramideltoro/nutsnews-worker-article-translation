import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadTranslationConfig } from "../src/config.js";
import { createTranslationService } from "../src/service.js";
import {
  InMemoryTranslationStateStore,
  LocalTranslationBrokerOutbox,
  LocalTranslationLanguagePolicy,
  LocalTranslationQualityValidator,
  LocalTranslationQwenClient,
  LocalTranslationTransactionRunner,
  LocalTranslationWorkHandler,
  LocalBrokerTransport,
  createLocalTranslationDependencies,
  createMinimalTranslationDelivery
} from "../src/test-doubles.js";

describe("createTranslationService", () => {
  it("starts, becomes ready, registers translation and persistence routes, and drains cleanly", async () => {
    const context = createServiceContext();

    await context.service.start();

    expect(context.service.isStarted).toBe(true);
    expect(context.service.consumer?.stage).toBe("translation");
    expect(context.broker.assertedRoutes.map((route) => route.stage)).toEqual([
      "translation",
      "persistence"
    ]);
    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.startup()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("ok");
    expect(context.metrics.collect()).toContain("nutsnews_worker_dependency_duration_ms");

    await context.service.stop();

    expect(context.service.isStarted).toBe(false);
    expect(context.service.broker.state).toBe("closed");
    expect(context.telemetry.events.some((event) => event.name === "runtime.broker.state_changed")).toBe(true);
  });

  it("delegates a valid translation delivery and acks duplicate replays without business logic", async () => {
    const context = createServiceContext();
    const delivery = createMinimalTranslationDelivery();

    await context.service.start();

    await expect(context.broker.deliverTranslation(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverTranslation(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.workHandler.handled[0]?.payload).toMatchObject({
      articleId: "article-001",
      sourceLanguage: "en",
      targetLanguages: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ]
    });

    await context.service.stop();
  });

  it("rejects payloads that are not consumed by the translation service", async () => {
    const context = createServiceContext();

    await context.service.start();

    await expect(context.broker.deliverTranslation({
      envelope: createMinimalTranslationDelivery().envelope,
      payload: enrichmentResultPayload(),
      receivedAt: "2026-07-23T00:00:01.000Z"
    })).resolves.toMatchObject({
      action: "dlq",
      reason: "payload-consumer-mismatch"
    });

    expect(context.workHandler.handled).toHaveLength(0);

    await context.service.stop();
  });

  it("waits for an in-flight delivery during shutdown without wall-clock sleeps", async () => {
    const context = createServiceContext();
    const gate = deferred<undefined>();
    const started = deferred<undefined>();

    context.workHandler.handleGate = gate.promise;
    context.workHandler.onHandleStart = () => {
      started.resolve(undefined);
    };

    await context.service.start();
    const delivery = context.broker.deliverTranslation();
    await started.promise;
    const stop = context.service.stop();

    expect(context.service.isDraining).toBe(true);
    expect(context.workHandler.handled).toHaveLength(0);

    gate.resolve(undefined);
    await expect(delivery).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await stop;

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.service.isStarted).toBe(false);
  });

  it("keeps liveness independent from AI endpoint readiness", async () => {
    const context = createServiceContext();

    context.qwenClient.status = "unhealthy";
    await context.service.start();

    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("unhealthy");

    await context.service.stop();
  });

  it("observes language policy and quality validator readiness", async () => {
    const context = createServiceContext();

    context.languagePolicy.status = "degraded";
    context.qualityValidator.status = "unhealthy";
    await context.service.start();

    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("unhealthy");

    await context.service.stop();
  });
});

function createServiceContext() {
  const config = loadTranslationConfig({
    NUTSNEWS_TRANSLATION_HTTP_PORT: "0",
    NUTSNEWS_TRANSLATION_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalTranslationDependencies();
  const telemetry = createBufferedRuntimeTelemetrySink();
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
    dependencies,
    telemetry,
    metrics
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    metrics,
    outbox: dependencies.brokerOutbox as LocalTranslationBrokerOutbox,
    languagePolicy: dependencies.languagePolicy as LocalTranslationLanguagePolicy,
    qualityValidator: dependencies.qualityValidator as LocalTranslationQualityValidator,
    qwenClient: dependencies.qwenClient as LocalTranslationQwenClient,
    service,
    stateStore: dependencies.stateStore as InMemoryTranslationStateStore,
    telemetry,
    transactionRunner: dependencies.transactionRunner as LocalTranslationTransactionRunner,
    workHandler: dependencies.workHandler as LocalTranslationWorkHandler
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject
  };
}

function enrichmentResultPayload(): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.enrichmentResult,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4701",
    idempotencyKey: "enrichment:approval:enrichment-req-001:fingerprint001",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: "2026-07-23T00:00:00.000Z",
    candidateId: "candidate-world-001",
    canonicalUrl: "https://articles.example.test/world/story-001",
    imageStatus: "hydrated",
    articleMetadataRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/enrichment/article-001/fingerprint001",
      mediaType: "application/json",
      contentFingerprint: "fingerprint001",
      canonicalArticleId: "article-001",
      articleVersion: 1
    }
  };
}
