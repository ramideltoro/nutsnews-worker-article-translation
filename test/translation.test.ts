import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadTranslationConfig } from "../src/config.js";
import { TranslationQwenError } from "../src/dependencies.js";
import { createTranslationPrometheusMetricsSink } from "../src/metrics.js";
import { createTranslationService } from "../src/service.js";
import {
  InMemoryTranslationStateStore,
  LocalTranslationBrokerOutbox,
  LocalTranslationQwenClient,
  LocalBrokerTransport,
  ManualTranslationClock,
  createLocalTranslationDependencies,
  createMinimalTranslationEnvelope,
  createMinimalTranslationPayload
} from "../src/test-doubles.js";
import { createArticleTranslationWorkHandler } from "../src/translation.js";

describe("createArticleTranslationWorkHandler", () => {
  it("fans out accepted articles into validated per-language Qwen results and persistence commands", async () => {
    const context = createTranslationContext();

    await context.service.start();

    await expect(context.broker.deliverTranslation()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.qwenClient.requests.map((request) => request.input.targetLanguage)).toEqual([
      "fr",
      "ja",
      "de-CH",
      "de",
      "el"
    ]);
    expect(context.qwenClient.requests[0]).toMatchObject({
      model: "qwen2.5:3b",
      timeoutMs: 30_000,
      maxInputBytes: 32_768,
      deterministic: {
        temperature: 0,
        topP: 1
      },
      responseSchema: {
        name: "translation_result_v1",
        requiredFields: [
          "summary",
          "qualityScore"
        ]
      },
      input: {
        articleId: "article-001",
        articleVersion: 1,
        sourceLanguage: "en",
        targetLanguage: "fr"
      }
    });
    expect(context.qwenClient.requests[0]?.prompt).toMatchObject({
      id: "summary-translation-v1",
      version: "0.1.0"
    });
    expect(context.stateStore.languageResults).toHaveLength(5);
    expect(context.stateStore.languageResults.map((result) => result.status)).toEqual([
      "success",
      "success",
      "success",
      "success",
      "success"
    ]);
    expect(context.stateStore.languageResults.map((result) => result.targetLanguage)).toEqual([
      "fr",
      "ja",
      "de-CH",
      "de",
      "el"
    ]);
    expect(context.stateStore.languageResults[0]).toMatchObject({
      articleId: "article-001",
      articleVersion: 1,
      sourceLanguage: "en",
      targetLanguage: "fr",
      promptId: "summary-translation-v1",
      promptVersion: "0.1.0",
      model: "qwen2.5:3b",
      latencyMs: 41
    });
    expect(context.stateStore.languageResults[0]?.summaryRef).toMatchObject({
      kind: "backend-record",
      articleId: "article-001",
      targetLanguage: "fr"
    });
    expect(context.stateStore.languageResults[0]?.aiUsageRef).toMatchObject({
      inputTokens: 120,
      outputTokens: 38,
      totalTokens: 158
    });
    expect(context.broker.published).toHaveLength(6);
    expect(context.outbox.records).toHaveLength(6);
    expect(context.broker.published.every((command) => validateStagePayload(command.payload).ok)).toBe(true);
    expect(persistenceCommands(context).map((command) => entityRef(command).targetLanguage)).toEqual([
      "fr",
      "ja",
      "de-CH",
      "de",
      "el"
    ]);
    expect(statusCommand(context)?.payload).toMatchObject({
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationResult,
      articleId: "article-001",
      translationStatus: "complete",
      completedLanguageCodes: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ],
      missingLanguageCodes: []
    });
    expect(context.stateStore.languageResults.every((result) => result.persistencePublication !== undefined)).toBe(true);
    expect(context.metrics.collect()).toContain("nutsnews_translation_language_results_total");
    expect(context.metrics.collect()).toContain('language="fr"');
    expect(context.metrics.collect()).toContain('provider="local_ai"');
    expect(context.metrics.collect()).toContain("nutsnews_translation_language_tokens_total");
    expect(context.metrics.collect()).toContain('token_kind="total"');

    const telemetryJson = JSON.stringify(context.telemetry.events);

    expect(telemetryJson).toContain("targetLanguage");
    expect(telemetryJson).toContain("latencyMs");
    expect(telemetryJson).not.toContain("Translated public-interest summary");
    expect(telemetryJson).not.toContain(context.qwenClient.requests[0]?.prompt.instructions);
  });

  it("retries one transient language failure without rolling back or duplicating successful languages", async () => {
    const context = createTranslationContext();

    context.qwenClient.errorsByLanguage.set("ja", new TranslationQwenError("qwen-timeout", {
      retryable: true,
      retryAfterMs: 5_000
    }));

    await context.service.start();

    await expect(context.broker.deliverTranslation()).resolves.toMatchObject({
      action: "retry",
      reason: "qwen-timeout",
      retryAfterMs: 5_000
    });

    expect(context.stateStore.languageResults).toHaveLength(1);
    expect(context.stateStore.languageResults[0]).toMatchObject({
      targetLanguage: "fr",
      status: "success"
    });
    expect(persistenceCommands(context)).toHaveLength(1);
    expect(context.metrics.collect()).toContain('result="retry"');
    expect(context.metrics.collect()).toContain('retry="retryable"');

    context.qwenClient.errorsByLanguage.delete("ja");

    await expect(context.broker.deliverTranslation()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.qwenClient.requests.map((request) => request.input.targetLanguage)).toEqual([
      "fr",
      "ja",
      "ja",
      "de-CH",
      "de",
      "el"
    ]);
    expect(context.stateStore.languageResults.map((result) => result.targetLanguage)).toEqual([
      "fr",
      "ja",
      "de-CH",
      "de",
      "el"
    ]);
    expect(persistenceCommands(context).filter((command) => entityRef(command).targetLanguage === "fr")).toHaveLength(1);
    expect(statusCommand(context)?.payload).toMatchObject({
      translationStatus: "complete",
      missingLanguageCodes: []
    });
  });

  it("does not resubmit already successful article-version-language-prompt-model combinations on replay", async () => {
    const context = createTranslationContext();

    await context.service.start();

    await expect(context.broker.deliverTranslation()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    const firstPublishedCount = context.broker.published.length;
    const replayDelivery = {
      envelope: createMinimalTranslationEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4811",
        idempotencyKey: "approval:translation:article-001:replay"
      }),
      payload: createMinimalTranslationPayload({
        idempotencyKey: "approval:translation:article-001:replay",
        reason: "replay"
      }),
      receivedAt: "2026-07-23T00:00:02.000Z"
    };

    await expect(context.broker.deliverTranslation(replayDelivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.qwenClient.requests).toHaveLength(5);
    expect(context.stateStore.languageResults).toHaveLength(5);
    expect(persistenceCommands(context)).toHaveLength(5);
    expect(context.broker.published).toHaveLength(firstPublishedCount + 1);
  });

  it("records one permanent language failure without suppressing successful languages", async () => {
    const context = createTranslationContext();

    context.qwenClient.responsesByLanguage.set("ja", {
      summary: "Too weak to publish.",
      qualityScore: 42,
      latencyMs: 19
    });

    await context.service.start();

    await expect(context.broker.deliverTranslation()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.stateStore.languageResults).toHaveLength(5);
    expect(context.stateStore.languageResults.find((result) => result.targetLanguage === "ja")).toMatchObject({
      status: "permanent_failure",
      failureReason: "translation_quality_below_threshold"
    });
    expect(persistenceCommands(context).map((command) => entityRef(command).targetLanguage)).toEqual([
      "fr",
      "de-CH",
      "de",
      "el"
    ]);
    expect(statusCommand(context)?.payload).toMatchObject({
      translationStatus: "permanent_failure",
      completedLanguageCodes: [
        "fr",
        "de-CH",
        "de",
        "el"
      ],
      missingLanguageCodes: [
        "ja"
      ]
    });
  });

  it("skips languages already present on the translation task", async () => {
    const context = createTranslationContext();
    const delivery = {
      envelope: createMinimalTranslationEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4821",
        idempotencyKey: "approval:translation:article-001:existing-fr"
      }),
      payload: createMinimalTranslationPayload({
        idempotencyKey: "approval:translation:article-001:existing-fr",
        existingLanguageCodes: [
          "fr"
        ]
      }),
      receivedAt: "2026-07-23T00:00:03.000Z"
    };

    await context.service.start();

    await expect(context.broker.deliverTranslation(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    expect(context.qwenClient.requests.map((request) => request.input.targetLanguage)).toEqual([
      "ja",
      "de-CH",
      "de",
      "el"
    ]);
    expect(context.stateStore.languageResults.map((result) => result.targetLanguage)).toEqual([
      "ja",
      "de-CH",
      "de",
      "el"
    ]);
    expect(statusCommand(context)?.payload).toMatchObject({
      translationStatus: "complete",
      missingLanguageCodes: []
    });
  });
});

function createTranslationContext() {
  const config = loadTranslationConfig({
    NUTSNEWS_TRANSLATION_HTTP_PORT: "0",
    NUTSNEWS_TRANSLATION_TELEMETRY_LOGS: "silent"
  });
  const telemetry = createBufferedRuntimeTelemetrySink();
  const clock = new ManualTranslationClock();
  const metrics = createTranslationPrometheusMetricsSink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    }
  });
  const telemetryAndMetrics = {
    emit: async (event: Parameters<typeof telemetry.emit>[0]) => {
      await telemetry.emit(event);
      await metrics.emit(event);
    }
  };
  const baseDependencies = createLocalTranslationDependencies({
    clock
  });
  const dependencies = {
    ...baseDependencies,
    workHandler: createArticleTranslationWorkHandler({
      config,
      dependencies: baseDependencies,
      telemetry: telemetryAndMetrics
    })
  };
  const service = createTranslationService({
    config,
    dependencies,
    telemetry: telemetryAndMetrics,
    metrics
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    config,
    metrics,
    outbox: dependencies.brokerOutbox as LocalTranslationBrokerOutbox,
    qwenClient: dependencies.qwenClient as LocalTranslationQwenClient,
    service,
    stateStore: dependencies.stateStore as InMemoryTranslationStateStore,
    telemetry
  };
}

function persistenceCommands(context: ReturnType<typeof createTranslationContext>) {
  return context.broker.published.filter((command) => command.payload.schemaId === STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand);
}

function statusCommand(context: ReturnType<typeof createTranslationContext>) {
  return context.broker.published.find((command) => command.payload.schemaId === STAGE_PAYLOAD_SCHEMA_IDS.translationResult);
}

function entityRef(command: ReturnType<typeof persistenceCommands>[number]) {
  const refs = command.payload.entityRefs;

  if (!Array.isArray(refs) || refs.length !== 1 || typeof refs[0] !== "object" || refs[0] === null || Array.isArray(refs[0])) {
    throw new Error("Expected one persistence entity ref.");
  }

  return refs[0] as Readonly<Record<string, unknown>>;
}
