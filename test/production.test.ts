import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadTranslationConfig } from "../src/config.js";
import {
  TranslationQwenError,
  type TranslationQwenRequest
} from "../src/dependencies.js";
import {
  LocalAiTranslationQwenClient,
  createProductionTranslationDependencies
} from "../src/production.js";

const clock = {
  now: () => new Date("2026-07-23T00:00:00.000Z")
};

describe("production translation dependencies", () => {
  it("wires production mode to RabbitMQ, Postgres, local AI, and static policy dependencies", async () => {
    const env = productionEnv();
    const config = loadTranslationConfig({
      ...env,
      NUTSNEWS_TRANSLATION_DEPENDENCY_MODE: "production"
    });
    const dependencies = createProductionTranslationDependencies({
      config,
      clock,
      env
    });

    expect(dependencies.brokerTransport.name).toBe("rabbitmq-payload-transport");
    expect(dependencies.stateStore.name).toBe("postgres-translation-state");
    expect(dependencies.transactionRunner.name).toBe("postgres-translation-transactions");
    expect(dependencies.brokerOutbox.name).toBe("postgres-translation-outbox");
    expect(dependencies.qwenClient.name).toBe("local-ai-translation-client");
    expect(dependencies.promptRegistry.name).toBe("static-translation-prompt-registry");
    expect(dependencies.languagePolicy.name).toBe("static-translation-language-policy");
    await expect(dependencies.languagePolicy.getPolicy()).resolves.toMatchObject({
      requiredLanguageCodes: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ],
      perLanguageConcurrency: 1
    });

    await dependencies.close();
  });

  it("maps the legacy local AI translation response into a translation result", async () => {
    const fetcher = vi.fn(() => Promise.resolve(new Response(JSON.stringify({
      language_code: "fr",
      title: "Bibliotheques de quartier",
      summary: "Des eleves creent des boites a livres pour leur quartier, donnant aux familles un acces plus simple a des lectures positives.",
      prompt_tokens: 120,
      completion_tokens: 38,
      total_tokens: 158,
      duration_ms: 4210
    }), {
      status: 200
    })));
    const client = new LocalAiTranslationQwenClient({
      baseUrl: "https://ai.example.test/",
      apiKey: "local-key",
      clock,
      fetcher
    });

    await expect(client.translate(translationRequest())).resolves.toMatchObject({
      summary: "Des eleves creent des boites a livres pour leur quartier, donnant aux familles un acces plus simple a des lectures positives.",
      qualityScore: 92,
      latencyMs: 4210,
      usage: {
        inputTokens: 120,
        outputTokens: 38,
        totalTokens: 158
      }
    });
    expect(fetcher).toHaveBeenCalledWith("https://ai.example.test/translate", expect.objectContaining({
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-nutsnews-ai-key": "local-key"
      }
    }));
  });

  it("fails closed before calling local AI when the API key is not a valid header", async () => {
    const fetcher = vi.fn();
    const client = new LocalAiTranslationQwenClient({
      baseUrl: "https://ai.example.test",
      apiKey: "bad\nkey",
      clock,
      fetcher
    });

    await expect(client.translate(translationRequest())).rejects.toMatchObject({
      name: "TranslationQwenError",
      reason: "qwen-unauthorized",
      retryable: false
    } satisfies Partial<TranslationQwenError>);
    expect(fetcher).not.toHaveBeenCalled();
  });
});

function productionEnv(): NodeJS.ProcessEnv {
  return {
    NUTSNEWS_TRANSLATION_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
    NUTSNEWS_TRANSLATION_DATABASE_URL: "postgres://translation:secret@example.invalid:5432/nutsnews",
    NUTSNEWS_TRANSLATION_RABBITMQ_URL: "amqp://translation:secret@example.invalid:5672",
    NUTSNEWS_TRANSLATION_QWEN_BASE_URL: "https://ai.example.test",
    NUTSNEWS_TRANSLATION_QWEN_API_KEY: "local-key",
    NUTSNEWS_TRANSLATION_PREFETCH: "2",
    NUTSNEWS_TRANSLATION_CONCURRENCY: "1"
  };
}

function translationRequest(): TranslationQwenRequest {
  return {
    model: "qwen2.5:3b",
    prompt: {
      id: "summary-translation-v1",
      version: "0.1.0",
      purpose: "summary-translation",
      instructions: "Return JSON."
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
  };
}
