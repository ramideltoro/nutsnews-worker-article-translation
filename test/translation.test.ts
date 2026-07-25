import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  WORKER_DELIVERY_BEHAVIOR,
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
import {
  TranslationQwenError,
  type TranslationWorkTools
} from "../src/dependencies.js";
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
import {
  createArticleTranslationWorkHandler,
  publishTranslationBacklogRecoveryTask,
  type TranslationTaskInput
} from "../src/translation.js";

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

  it("uses distinct broker message ids for per-article translation status commands", async () => {
    const context = createTranslationContext();
    const secondDelivery = {
      envelope: createMinimalTranslationEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4851",
        idempotencyKey: "approval:translation:article-002",
        aggregate: {
          type: "article",
          id: "article-002",
          version: 1
        }
      }),
      payload: createMinimalTranslationPayload({
        pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3602",
        stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4703",
        sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4704",
        idempotencyKey: "approval:translation:article-002",
        articleId: "article-002"
      }),
      receivedAt: "2026-07-23T00:00:05.000Z"
    };

    await context.service.start();

    await expect(context.broker.deliverTranslation()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverTranslation(secondDelivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    await context.service.stop();

    const messages = statusCommands(context).map((command) => command.envelope.messageId);

    expect(messages).toHaveLength(2);
    expect(new Set(messages).size).toBe(2);
    expect(statusCommands(context).map((command) => command.envelope.idempotencyKey)).toEqual([
      "translation:result:article-001:1",
      "translation:result:article-002:1"
    ]);
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

  it("bounds quality re-prompts and resumes without redoing earlier successful languages", async () => {
    const context = createTranslationContext();

    context.qwenClient.responsesByLanguage.set("ja", {
      summary: "The article reports a useful public-interest development with details.",
      qualityScore: 42,
      latencyMs: 19
    });

    await context.service.start();

    await expect(context.broker.deliverTranslation()).resolves.toMatchObject({
      action: "retry",
      reason: "translation-quality-translation_quality_below_threshold"
    });

    expect(context.stateStore.languageResults).toHaveLength(1);
    expect(context.stateStore.languageResults[0]).toMatchObject({
      targetLanguage: "fr",
      status: "success"
    });
    expect(context.metrics.collect()).toContain('result="retry"');

    context.qwenClient.responsesByLanguage.delete("ja");

    await expect(context.broker.deliverTranslation(retryDelivery(2))).resolves.toMatchObject({
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

  it("records one permanent language failure after quality re-prompt attempts are exhausted", async () => {
    const context = createTranslationContext();

    context.qwenClient.responsesByLanguage.set("ja", {
      summary: "この記事は、地域社会に役立つ進展を具体的に伝えています。",
      qualityScore: 42,
      latencyMs: 19
    });

    await context.service.start();

    await expect(context.broker.deliverTranslation(retryDelivery(2))).resolves.toMatchObject({
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

  it("publishes backlog recovery tasks for missing durable language results only", async () => {
    const context = createTranslationContext();
    const initialFrOnly = {
      envelope: createMinimalTranslationEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4831",
        idempotencyKey: "approval:translation:article-001:fr-only"
      }),
      payload: createMinimalTranslationPayload({
        idempotencyKey: "approval:translation:article-001:fr-only",
        targetLanguages: [
          "fr"
        ]
      }),
      receivedAt: "2026-07-23T00:00:04.000Z"
    };

    await context.service.start();

    await expect(context.broker.deliverTranslation(initialFrOnly)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    const recovery = await publishTranslationBacklogRecoveryTask(backlogContext(), backlogRequest(), workTools(context), {
      config: context.config,
      dependencies: context.dependencies,
      telemetry: context.telemetryAndMetrics
    });

    expect(recovery).toMatchObject({
      status: "published",
      completedLanguageCodes: [
        "fr"
      ],
      missingLanguageCodes: [
        "ja",
        "de-CH",
        "de",
        "el"
      ]
    });

    if (recovery.status !== "published") {
      throw new Error("Expected backlog recovery task to publish.");
    }

    expect(recovery.command.payload).toMatchObject({
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
      reason: "backlog_recovery",
      existingLanguageCodes: [
        "fr"
      ]
    });

    await expect(context.broker.deliverTranslation({
      envelope: recovery.command.envelope,
      payload: recovery.command.payload,
      receivedAt: "2026-07-23T00:00:05.000Z"
    })).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    const noop = await publishTranslationBacklogRecoveryTask(backlogContext(), backlogRequest(), workTools(context), {
      config: context.config,
      dependencies: context.dependencies,
      telemetry: context.telemetryAndMetrics
    });

    await context.service.stop();

    expect(noop).toMatchObject({
      status: "noop",
      missingLanguageCodes: []
    });
    expect(context.qwenClient.requests.map((request) => request.input.targetLanguage)).toEqual([
      "fr",
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
    dependencies,
    metrics,
    outbox: dependencies.brokerOutbox as LocalTranslationBrokerOutbox,
    qwenClient: dependencies.qwenClient as LocalTranslationQwenClient,
    service,
    stateStore: dependencies.stateStore as InMemoryTranslationStateStore,
    telemetry,
    telemetryAndMetrics
  };
}

function retryDelivery(attemptCount: number) {
  const occurredAt = "2026-07-23T00:00:00.000Z";

  return {
    envelope: createMinimalTranslationEnvelope({
      attempt: {
        count: attemptCount,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: occurredAt
      }
    }),
    payload: createMinimalTranslationPayload(),
    receivedAt: "2026-07-23T00:00:02.000Z"
  };
}

function backlogContext() {
  return {
    envelope: createMinimalTranslationEnvelope({
      messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4841",
      idempotencyKey: "translation:backlog:article-001"
    }),
    payload: createMinimalTranslationPayload({
      idempotencyKey: "translation:backlog:article-001",
      reason: "backlog_recovery"
    }),
    stage: "translation" as const,
    receivedAt: "2026-07-23T00:00:04.000Z"
  };
}

function backlogRequest(): TranslationTaskInput {
  return {
    articleId: "article-001",
    articleVersion: 1,
    sourceLanguage: "en",
    targetLanguages: [
      "fr",
      "ja",
      "de-CH",
      "de",
      "el"
    ],
    existingLanguageCodes: [],
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601"
  };
}

function workTools(context: ReturnType<typeof createTranslationContext>): TranslationWorkTools {
  return {
    publish: (command) => context.service.broker.publish(command),
    recordOutbox: (command, receipt) => context.outbox.record(command, receipt),
    withTransaction: <T>(operation: Parameters<TranslationWorkTools["withTransaction"]>[0]) => context.dependencies.transactionRunner.withTransaction(operation) as Promise<T>
  };
}

function persistenceCommands(context: ReturnType<typeof createTranslationContext>) {
  return context.broker.published.filter((command) => command.payload.schemaId === STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand);
}

function statusCommand(context: ReturnType<typeof createTranslationContext>) {
  return statusCommands(context)[0];
}

function statusCommands(context: ReturnType<typeof createTranslationContext>) {
  return context.broker.published.filter((command) => command.payload.schemaId === STAGE_PAYLOAD_SCHEMA_IDS.translationResult);
}

function entityRef(command: ReturnType<typeof persistenceCommands>[number]) {
  const refs = command.payload.entityRefs;

  if (!Array.isArray(refs) || refs.length !== 1 || typeof refs[0] !== "object" || refs[0] === null || Array.isArray(refs[0])) {
    throw new Error("Expected one persistence entity ref.");
  }

  return refs[0] as Readonly<Record<string, unknown>>;
}
