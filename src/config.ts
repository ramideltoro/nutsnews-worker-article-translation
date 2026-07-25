import os from "node:os";

import { WORKER_DELIVERY_BEHAVIOR } from "@ramideltoro/nutsnews-worker-contracts";

export const TRANSLATION_SERVICE_NAME = "nutsnews-worker-article-translation" as const;
export const TRANSLATION_SERVICE_VERSION = "0.1.0" as const;

export type TranslationDependencyMode = "test" | "production";
export type TranslationTelemetryLogMode = "stdout" | "silent";

export interface TranslationConfigVariable {
  readonly name: string;
  readonly description: string;
  readonly requiredInProduction: boolean;
  readonly sensitive: boolean;
  readonly defaultValue?: string;
}

export const TRANSLATION_CONFIG_SCHEMA = [
  variable("NUTSNEWS_ENVIRONMENT", "Runtime environment label for logs and metrics.", false, false, "local"),
  variable("NUTSNEWS_TRANSLATION_HTTP_HOST", "Health and metrics bind host.", false, false, "0.0.0.0"),
  variable("NUTSNEWS_TRANSLATION_HTTP_PORT", "Health and metrics bind port.", false, false, "8080"),
  variable("NUTSNEWS_TRANSLATION_DEPENDENCY_MODE", "Use test dependencies locally or require production dependency presence.", false, false, "test"),
  variable("NUTSNEWS_TRANSLATION_DATABASE_URL", "Backend shadow database connection string for translation state.", true, true),
  variable("NUTSNEWS_TRANSLATION_RABBITMQ_URL", "Private RabbitMQ connection string.", true, true),
  variable("NUTSNEWS_TRANSLATION_QWEN_BASE_URL", "Private Qwen-compatible translation endpoint.", true, true),
  variable("NUTSNEWS_TRANSLATION_QWEN_API_KEY", "Credential for the Qwen-compatible translation endpoint.", true, true),
  variable("NUTSNEWS_TRANSLATION_QWEN_MODEL", "Model identifier used by the injected translation client.", false, false, "qwen2.5:3b"),
  variable("NUTSNEWS_TRANSLATION_PROMPT_ID", "Versioned summary translation prompt identifier.", false, false, "summary-translation-v1"),
  variable("NUTSNEWS_TRANSLATION_LANGUAGE_POLICY_ID", "Versioned language policy identifier.", false, false, "required-summaries-v1"),
  variable("NUTSNEWS_TRANSLATION_TARGET_LANGUAGES", "Comma-separated required summary language codes.", false, false, "fr,ja,de-CH,de,el"),
  variable("NUTSNEWS_TRANSLATION_PER_LANGUAGE_CONCURRENCY", "Maximum concurrent translation calls for one target language.", false, false, "1"),
  variable("NUTSNEWS_TRANSLATION_QUALITY_MIN_SCORE", "Minimum bounded translation quality score accepted by quality validation.", false, false, "80"),
  variable("NUTSNEWS_TRANSLATION_SUMMARY_MIN_CHARS", "Minimum translated summary length accepted by quality validation.", false, false, "24"),
  variable("NUTSNEWS_TRANSLATION_SUMMARY_MAX_CHARS", "Maximum translated summary length accepted by quality validation.", false, false, "420"),
  variable("NUTSNEWS_TRANSLATION_QUALITY_REPROMPT_MAX_ATTEMPTS", "Maximum delivery attempts used for retryable quality re-prompts before permanent failure.", false, false, "2"),
  variable("NUTSNEWS_TRANSLATION_CONCURRENCY", "Maximum concurrent translation message handlers.", false, false, "2"),
  variable("NUTSNEWS_TRANSLATION_PREFETCH", "Broker prefetch bound for translation deliveries.", false, false, "4"),
  variable("NUTSNEWS_TRANSLATION_QWEN_TOTAL_TIMEOUT_MS", "Maximum translation endpoint call timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_TRANSLATION_QWEN_MAX_INPUT_BYTES", "Maximum prompt input reference size accepted by translation.", false, false, "32768"),
  variable("NUTSNEWS_TRANSLATION_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_TRANSLATION_SHADOW_MODE", "Keep translation output isolated from legacy ingestion.", false, false, "true"),
  variable("NUTSNEWS_TRANSLATION_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_TRANSLATION_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true")
] as const satisfies readonly TranslationConfigVariable[];

export interface TranslationConfig {
  readonly serviceName: typeof TRANSLATION_SERVICE_NAME;
  readonly serviceVersion: typeof TRANSLATION_SERVICE_VERSION;
  readonly environment: string;
  readonly host: string;
  readonly http: {
    readonly host: string;
    readonly port: number;
  };
  readonly dependencyMode: TranslationDependencyMode;
  readonly dependencies: {
    readonly databaseConfigured: boolean;
    readonly rabbitmqConfigured: boolean;
    readonly qwenEndpointConfigured: boolean;
    readonly qwenCredentialConfigured: boolean;
  };
  readonly qwen: {
    readonly model: string;
    readonly promptId: string;
    readonly totalTimeoutMs: number;
    readonly maxInputBytes: number;
  };
  readonly languagePolicy: {
    readonly policyId: string;
    readonly targetLanguages: readonly string[];
    readonly perLanguageConcurrency: number;
  };
  readonly quality: {
    readonly minScore: number;
    readonly minSummaryChars: number;
    readonly maxSummaryChars: number;
    readonly repromptMaxAttempts: number;
  };
  readonly concurrency: number;
  readonly prefetch: number;
  readonly shutdownTimeoutMs: number;
  readonly shadowMode: boolean;
  readonly telemetryLogs: TranslationTelemetryLogMode;
  readonly metricsEnabled: boolean;
}

export class TranslationConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`Invalid translation configuration: ${issues.join("; ")}`);
    this.name = "TranslationConfigError";
    this.issues = issues;
  }
}

export function loadTranslationConfig(env: NodeJS.ProcessEnv = process.env): TranslationConfig {
  const issues: string[] = [];
  const dependencyMode = parseDependencyMode(env.NUTSNEWS_TRANSLATION_DEPENDENCY_MODE, issues);
  const dependencies = {
    databaseConfigured: hasValue(env.NUTSNEWS_TRANSLATION_DATABASE_URL),
    rabbitmqConfigured: hasValue(env.NUTSNEWS_TRANSLATION_RABBITMQ_URL),
    qwenEndpointConfigured: hasValue(env.NUTSNEWS_TRANSLATION_QWEN_BASE_URL),
    qwenCredentialConfigured: hasValue(env.NUTSNEWS_TRANSLATION_QWEN_API_KEY)
  };

  if (dependencyMode === "production") {
    requireConfigured("NUTSNEWS_TRANSLATION_DATABASE_URL", dependencies.databaseConfigured, issues);
    requireConfigured("NUTSNEWS_TRANSLATION_RABBITMQ_URL", dependencies.rabbitmqConfigured, issues);
    requireConfigured("NUTSNEWS_TRANSLATION_QWEN_BASE_URL", dependencies.qwenEndpointConfigured, issues);
    requireConfigured("NUTSNEWS_TRANSLATION_QWEN_API_KEY", dependencies.qwenCredentialConfigured, issues);
  }

  const concurrency = parseInteger(env.NUTSNEWS_TRANSLATION_CONCURRENCY, "NUTSNEWS_TRANSLATION_CONCURRENCY", 2, 1, 16, issues);
  const prefetch = parseInteger(env.NUTSNEWS_TRANSLATION_PREFETCH, "NUTSNEWS_TRANSLATION_PREFETCH", 4, 1, 64, issues);
  const config: TranslationConfig = {
    serviceName: TRANSLATION_SERVICE_NAME,
    serviceVersion: TRANSLATION_SERVICE_VERSION,
    environment: nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local"),
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    http: {
      host: nonEmpty(env.NUTSNEWS_TRANSLATION_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_TRANSLATION_HTTP_PORT, "NUTSNEWS_TRANSLATION_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    dependencies,
    qwen: {
      model: nonEmpty(env.NUTSNEWS_TRANSLATION_QWEN_MODEL, "qwen2.5:3b"),
      promptId: nonEmpty(env.NUTSNEWS_TRANSLATION_PROMPT_ID, "summary-translation-v1"),
      totalTimeoutMs: parseInteger(env.NUTSNEWS_TRANSLATION_QWEN_TOTAL_TIMEOUT_MS, "NUTSNEWS_TRANSLATION_QWEN_TOTAL_TIMEOUT_MS", 30_000, 1_000, 180_000, issues),
      maxInputBytes: parseInteger(env.NUTSNEWS_TRANSLATION_QWEN_MAX_INPUT_BYTES, "NUTSNEWS_TRANSLATION_QWEN_MAX_INPUT_BYTES", 32_768, 1_024, 1_048_576, issues)
    },
    languagePolicy: {
      policyId: nonEmpty(env.NUTSNEWS_TRANSLATION_LANGUAGE_POLICY_ID, "required-summaries-v1"),
      targetLanguages: parseList(env.NUTSNEWS_TRANSLATION_TARGET_LANGUAGES, "NUTSNEWS_TRANSLATION_TARGET_LANGUAGES", "fr,ja,de-CH,de,el", issues),
      perLanguageConcurrency: parseInteger(env.NUTSNEWS_TRANSLATION_PER_LANGUAGE_CONCURRENCY, "NUTSNEWS_TRANSLATION_PER_LANGUAGE_CONCURRENCY", 1, 1, 8, issues)
    },
    quality: {
      minScore: parseInteger(env.NUTSNEWS_TRANSLATION_QUALITY_MIN_SCORE, "NUTSNEWS_TRANSLATION_QUALITY_MIN_SCORE", 80, 0, 100, issues),
      minSummaryChars: parseInteger(env.NUTSNEWS_TRANSLATION_SUMMARY_MIN_CHARS, "NUTSNEWS_TRANSLATION_SUMMARY_MIN_CHARS", 24, 1, 1_000, issues),
      maxSummaryChars: parseInteger(env.NUTSNEWS_TRANSLATION_SUMMARY_MAX_CHARS, "NUTSNEWS_TRANSLATION_SUMMARY_MAX_CHARS", 420, 24, 2_000, issues),
      repromptMaxAttempts: parseInteger(env.NUTSNEWS_TRANSLATION_QUALITY_REPROMPT_MAX_ATTEMPTS, "NUTSNEWS_TRANSLATION_QUALITY_REPROMPT_MAX_ATTEMPTS", 2, 1, WORKER_DELIVERY_BEHAVIOR.maxAttempts, issues)
    },
    concurrency,
    prefetch,
    shutdownTimeoutMs: parseInteger(env.NUTSNEWS_TRANSLATION_SHUTDOWN_TIMEOUT_MS, "NUTSNEWS_TRANSLATION_SHUTDOWN_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    shadowMode: parseBoolean(env.NUTSNEWS_TRANSLATION_SHADOW_MODE, "NUTSNEWS_TRANSLATION_SHADOW_MODE", true, issues),
    telemetryLogs: parseTelemetryLogMode(env.NUTSNEWS_TRANSLATION_TELEMETRY_LOGS, issues),
    metricsEnabled: parseBoolean(env.NUTSNEWS_TRANSLATION_METRICS_ENABLED, "NUTSNEWS_TRANSLATION_METRICS_ENABLED", true, issues)
  };

  if (config.prefetch < config.concurrency) {
    issues.push("NUTSNEWS_TRANSLATION_PREFETCH must be greater than or equal to NUTSNEWS_TRANSLATION_CONCURRENCY.");
  }

  if (config.languagePolicy.perLanguageConcurrency > config.concurrency) {
    issues.push("NUTSNEWS_TRANSLATION_PER_LANGUAGE_CONCURRENCY must be less than or equal to NUTSNEWS_TRANSLATION_CONCURRENCY.");
  }

  if (config.quality.minSummaryChars > config.quality.maxSummaryChars) {
    issues.push("NUTSNEWS_TRANSLATION_SUMMARY_MIN_CHARS must be less than or equal to NUTSNEWS_TRANSLATION_SUMMARY_MAX_CHARS.");
  }

  if (!config.shadowMode) {
    issues.push("NUTSNEWS_TRANSLATION_SHADOW_MODE must remain true until backend-owned deployment enables cutover.");
  }

  if (issues.length > 0) {
    throw new TranslationConfigError(issues);
  }

  return config;
}

function variable(
  name: string,
  description: string,
  requiredInProduction: boolean,
  sensitive: boolean,
  defaultValue?: string
): TranslationConfigVariable {
  return {
    name,
    description,
    requiredInProduction,
    sensitive,
    ...(defaultValue === undefined ? {} : {
      defaultValue
    })
  };
}

function nonEmpty(value: string | undefined, fallback: string): string {
  if (value === undefined) {
    return fallback;
  }

  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : fallback;
}

function hasValue(value: string | undefined): boolean {
  return value !== undefined && value.trim().length > 0;
}

function parseDependencyMode(value: string | undefined, issues: string[]): TranslationDependencyMode {
  const normalized = nonEmpty(value, "test");

  if (normalized === "test" || normalized === "production") {
    return normalized;
  }

  issues.push("NUTSNEWS_TRANSLATION_DEPENDENCY_MODE must be test or production.");
  return "test";
}

function parseTelemetryLogMode(value: string | undefined, issues: string[]): TranslationTelemetryLogMode {
  const normalized = nonEmpty(value, "stdout");

  if (normalized === "stdout" || normalized === "silent") {
    return normalized;
  }

  issues.push("NUTSNEWS_TRANSLATION_TELEMETRY_LOGS must be stdout or silent.");
  return "stdout";
}

function parseBoolean(
  value: string | undefined,
  key: string,
  fallback: boolean,
  issues: string[]
): boolean {
  if (!hasValue(value)) {
    return fallback;
  }

  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();

  if (normalized === "true" || normalized === "1") {
    return true;
  }

  if (normalized === "false" || normalized === "0") {
    return false;
  }

  issues.push(`${key} must be true or false.`);
  return fallback;
}

function parseInteger(
  value: string | undefined,
  key: string,
  fallback: number,
  min: number,
  max: number,
  issues: string[]
): number {
  if (!hasValue(value)) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    issues.push(`${key} must be an integer between ${String(min)} and ${String(max)}.`);
    return fallback;
  }

  return parsed;
}

function parseList(value: string | undefined, key: string, fallback: string, issues: string[]): readonly string[] {
  const entries = nonEmpty(value, fallback)
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);

  if (entries.length === 0) {
    issues.push(`${key} must include at least one value.`);
    return fallback.split(",");
  }

  return Array.from(new Set(entries));
}

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_TRANSLATION_DEPENDENCY_MODE=production.`);
  }
}
