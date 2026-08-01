import {
  getWorkerRoute,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  LocalTranslationBrokerOutbox,
  LocalTranslationLanguagePolicy,
  LocalTranslationPromptRegistry,
  LocalTranslationQualityValidator,
  LocalTranslationQwenClient,
  LocalTranslationTransactionRunner,
  LocalBrokerTransport,
  createMinimalTranslationDelivery
} from "../src/test-doubles.js";

describe("translation test doubles", () => {
  it("requires a registered local broker consumer before delivery", async () => {
    const broker = new LocalBrokerTransport();

    await broker.connect();

    await expect(broker.deliverTranslation(createMinimalTranslationDelivery())).rejects.toThrow("No local consumer is registered for translation.");
  });

  it("records local transaction and outbox boundaries without external dependencies", async () => {
    const runner = new LocalTranslationTransactionRunner();
    const outbox = new LocalTranslationBrokerOutbox();
    const route = getWorkerRoute("persistence");
    const command = {
      envelope: {
        schemaId: route.schemaId,
        schemaVersion: 1,
        route: "persistence",
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4901",
        causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4801",
        correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4601",
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        idempotencyKey: "translation:persistence:article-001",
        aggregate: {
          type: "article",
          id: "article-001",
          version: 1
        },
        occurredAt: "2026-07-23T00:00:00.000Z",
        attempt: {
          count: 1,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z"
        },
        producer: {
          name: "translation",
          version: "0.1.0"
        },
        payloadRef: {
          kind: "backend-record",
          uri: "backend://worker-uplift/translation/article-001/persistence-command",
          mediaType: "application/json",
          sizeBytes: 512
        }
      } satisfies WorkerMessageEnvelope,
      payload: {}
    };

    await expect(runner.withTransaction((transaction) => Promise.resolve(transaction.transactionId))).resolves.toBe("local-transaction-1");
    await outbox.record(command, {
      messageId: command.envelope.messageId,
      stage: "persistence",
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });

    expect(runner.transactions).toHaveLength(1);
    expect(outbox.records).toHaveLength(1);
  });

  it("provides injectable Qwen, prompt, language-policy, and quality readiness doubles", async () => {
    const qwenClient = new LocalTranslationQwenClient();
    const promptRegistry = new LocalTranslationPromptRegistry();
    const languagePolicy = new LocalTranslationLanguagePolicy();
    const qualityValidator = new LocalTranslationQualityValidator();

    expect(qwenClient.probe()).toEqual({
      status: "ok",
      summary: "local Qwen endpoint ready"
    });
    await expect(qwenClient.translate({
      model: "qwen2.5:3b",
      prompt: {
        id: "summary-translation-v1",
        version: "0.1.0",
        purpose: "summary-translation",
        instructions: "Translate."
      },
      timeoutMs: 30_000,
      maxInputBytes: 32_768,
      deterministic: {
        temperature: 0,
        topP: 1
      },
      responseSchema: {
        name: "translation_result_v1",
        requiredFields: [
          "title",
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
    })).resolves.toMatchObject({
      qualityScore: 93
    });
    await expect(promptRegistry.getPrompt("summary-translation-v1")).resolves.toMatchObject({
      id: "summary-translation-v1",
      version: "0.1.0"
    });
    await expect(languagePolicy.getPolicy()).resolves.toEqual({
      policyId: "required-summaries-v1",
      version: "0.1.0",
      requiredLanguageCodes: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ],
      perLanguageConcurrency: 1
    });
    expect(qualityValidator.probe()).toEqual({
      status: "ok",
      summary: "local quality validator ready"
    });
  });
});
