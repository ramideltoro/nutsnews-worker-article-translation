import {
  describe,
  expect,
  it
} from "vitest";

import {
  TranslationConfigError,
  loadTranslationConfig
} from "../src/config.js";

describe("loadTranslationConfig", () => {
  it("loads low-concurrency local test defaults without secret values", () => {
    const config = loadTranslationConfig({
      HOSTNAME: "translation-host"
    });

    expect(config).toMatchObject({
      serviceName: "nutsnews-worker-article-translation",
      dependencyMode: "test",
      host: "translation-host",
      concurrency: 2,
      prefetch: 4,
      qwen: {
        model: "qwen2.5:3b",
        totalTimeoutMs: 30_000,
        maxInputBytes: 32_768
      },
      languagePolicy: {
        policyId: "required-summaries-v1",
        targetLanguages: [
          "fr",
          "ja",
          "de-CH",
          "de",
          "el"
        ],
        perLanguageConcurrency: 1
      },
      quality: {
        minScore: 80
      },
      shadowMode: true,
      dependencies: {
        databaseConfigured: false,
        rabbitmqConfigured: false,
        qwenEndpointConfigured: false,
        qwenCredentialConfigured: false
      }
    });
  });

  it("fails production config by missing secret names only", () => {
    expect(() => loadTranslationConfig({
      NUTSNEWS_TRANSLATION_DEPENDENCY_MODE: "production"
    })).toThrow(TranslationConfigError);

    try {
      loadTranslationConfig({
        NUTSNEWS_TRANSLATION_DEPENDENCY_MODE: "production"
      });
    } catch (error: unknown) {
      expect(error).toBeInstanceOf(TranslationConfigError);
      const configError = error as TranslationConfigError;

      expect(configError.issues).toEqual([
        "NUTSNEWS_TRANSLATION_DATABASE_URL is required when NUTSNEWS_TRANSLATION_DEPENDENCY_MODE=production.",
        "NUTSNEWS_TRANSLATION_RABBITMQ_URL is required when NUTSNEWS_TRANSLATION_DEPENDENCY_MODE=production.",
        "NUTSNEWS_TRANSLATION_QWEN_BASE_URL is required when NUTSNEWS_TRANSLATION_DEPENDENCY_MODE=production.",
        "NUTSNEWS_TRANSLATION_QWEN_API_KEY is required when NUTSNEWS_TRANSLATION_DEPENDENCY_MODE=production."
      ]);
      expect(configError.message).not.toContain("postgres://");
      expect(configError.message).not.toContain("amqp://");
      expect(configError.message).not.toContain("sk-");
    }
  });

  it("rejects unsafe concurrency bounds and shadow cutover in this repo", () => {
    expect(() => loadTranslationConfig({
      NUTSNEWS_TRANSLATION_CONCURRENCY: "8",
      NUTSNEWS_TRANSLATION_PREFETCH: "2",
      NUTSNEWS_TRANSLATION_QWEN_TOTAL_TIMEOUT_MS: "10",
      NUTSNEWS_TRANSLATION_QWEN_MAX_INPUT_BYTES: "16",
      NUTSNEWS_TRANSLATION_PER_LANGUAGE_CONCURRENCY: "9",
      NUTSNEWS_TRANSLATION_QUALITY_MIN_SCORE: "120",
      NUTSNEWS_TRANSLATION_SHADOW_MODE: "false"
    })).toThrow(TranslationConfigError);
  });

  it("parses language policy and quality overrides", () => {
    const config = loadTranslationConfig({
      NUTSNEWS_TRANSLATION_LANGUAGE_POLICY_ID: "required-summaries-v2",
      NUTSNEWS_TRANSLATION_TARGET_LANGUAGES: "fr, ja, fr, de",
      NUTSNEWS_TRANSLATION_PER_LANGUAGE_CONCURRENCY: "2",
      NUTSNEWS_TRANSLATION_QUALITY_MIN_SCORE: "85"
    });

    expect(config.languagePolicy).toEqual({
      policyId: "required-summaries-v2",
      targetLanguages: [
        "fr",
        "ja",
        "de"
      ],
      perLanguageConcurrency: 2
    });
    expect(config.quality).toEqual({
      minScore: 85
    });
  });

  it("accepts explicit production dependency presence without retaining sensitive values", () => {
    const config = loadTranslationConfig({
      NUTSNEWS_TRANSLATION_DEPENDENCY_MODE: "production",
      NUTSNEWS_TRANSLATION_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_TRANSLATION_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_TRANSLATION_QWEN_BASE_URL: "https://qwen.internal.invalid/v1",
      NUTSNEWS_TRANSLATION_QWEN_API_KEY: "sk-not-real",
      NUTSNEWS_TRANSLATION_TELEMETRY_LOGS: "silent"
    });

    expect(config.dependencies).toEqual({
      databaseConfigured: true,
      rabbitmqConfigured: true,
      qwenEndpointConfigured: true,
      qwenCredentialConfigured: true
    });
    expect(JSON.stringify(config)).not.toContain("postgres://example.invalid");
    expect(JSON.stringify(config)).not.toContain("amqp://example.invalid");
    expect(JSON.stringify(config)).not.toContain("qwen.internal.invalid");
    expect(JSON.stringify(config)).not.toContain("sk-not-real");
  });
});
