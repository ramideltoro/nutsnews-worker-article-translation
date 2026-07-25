import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeTelemetryEvent
} from "@ramideltoro/nutsnews-worker-runtime";

type TranslationMetricLabels = Readonly<Record<string, string>>;
interface TranslationMetricIdentity {
  readonly environment: string;
  readonly host: string;
  readonly service: string;
  readonly version: string;
}

interface TranslationLanguageMetricDimensions {
  readonly language: string;
  readonly provider: string;
  readonly result: string;
  readonly retry: string;
}

export interface TranslationPrometheusMetricsSink extends PrometheusRuntimeTelemetrySink {
  readonly translationMetricLabels: readonly [
    "environment",
    "host",
    "service",
    "version",
    "language",
    "provider",
    "result",
    "retry"
  ];
}

export function createTranslationPrometheusMetricsSink(
  options: PrometheusRuntimeTelemetrySinkOptions
): TranslationPrometheusMetricsSink {
  const runtimeMetrics = createPrometheusRuntimeTelemetrySink(options);
  const counters = new Map<string, Map<string, number>>();
  const summaries = new Map<string, Map<string, { count: number; sum: number }>>();
  const identity = {
    environment: options.identity.environment,
    host: options.identity.host ?? "unknown",
    service: options.identity.service,
    version: options.identity.version
  };

  return {
    get allowedLabels() {
      return runtimeMetrics.allowedLabels;
    },
    translationMetricLabels: [
      "environment",
      "host",
      "service",
      "version",
      "language",
      "provider",
      "result",
      "retry"
    ],
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      await runtimeMetrics.emit(event);
      observeTranslationLanguageEvent(event, identity, counters, summaries);
    },
    collect(): string {
      return `${runtimeMetrics.collect()}${collectCustomMetrics(counters, summaries)}`;
    },
    recordDependencyLatency(queue, durationMs, outcome): void {
      runtimeMetrics.recordDependencyLatency(queue, durationMs, outcome);
    },
    setInFlight(queue, value): void {
      runtimeMetrics.setInFlight(queue, value);
    },
    setShutdownDraining(draining): void {
      runtimeMetrics.setShutdownDraining(draining);
    }
  };
}

function observeTranslationLanguageEvent(
  event: RuntimeTelemetryEvent,
  identity: TranslationMetricIdentity,
  counters: Map<string, Map<string, number>>,
  summaries: Map<string, Map<string, { count: number; sum: number }>>
): void {
  if (event.name !== "runtime.dependency.observed" || event.attributes?.event !== "translation.language.reviewed") {
    return;
  }

  const language = stringAttribute(event, "targetLanguage");
  const provider = stringAttribute(event, "provider") ?? "unknown";
  const result = stringAttribute(event, "result") ?? event.outcome ?? "unknown";
  const retry = result === "retry"
    ? "retryable"
    : result === "permanent_failure"
      ? "not_retryable"
      : "none";
  const labels = labelsFor(identity, {
    language: language ?? "unknown",
    provider,
    result,
    retry
  });
  const latencyMs = event.durationMs ?? numberAttribute(event, "latencyMs");

  incrementCounter(counters, "nutsnews_translation_language_results_total", labels, 1);

  if (latencyMs !== undefined) {
    observeSummary(summaries, "nutsnews_translation_language_latency_ms", labels, latencyMs);
  }

  recordTokenMetric(counters, labels, "input", numberAttribute(event, "inputTokens"));
  recordTokenMetric(counters, labels, "output", numberAttribute(event, "outputTokens"));
  recordTokenMetric(counters, labels, "total", numberAttribute(event, "totalTokens"));
}

function recordTokenMetric(
  counters: Map<string, Map<string, number>>,
  labels: TranslationMetricLabels,
  tokenKind: string,
  value: number | undefined
): void {
  if (value === undefined) {
    return;
  }

  incrementCounter(counters, "nutsnews_translation_language_tokens_total", {
    ...labels,
    token_kind: tokenKind
  }, value);
}

function collectCustomMetrics(
  counters: Map<string, Map<string, number>>,
  summaries: Map<string, Map<string, { count: number; sum: number }>>
): string {
  const lines: string[] = [];

  collectCounter(lines, counters, "nutsnews_translation_language_results_total", "Per-language translation outcomes by bounded provider, language, result, and retry class.");
  collectCounter(lines, counters, "nutsnews_translation_language_tokens_total", "Per-language translation token counts by bounded token kind.");
  collectSummary(lines, summaries, "nutsnews_translation_language_latency_ms", "Per-language Qwen translation latency in milliseconds.");

  return lines.length === 0 ? "" : `${lines.join("\n")}\n`;
}

function collectCounter(
  lines: string[],
  counters: Map<string, Map<string, number>>,
  metric: string,
  help: string
): void {
  const samples = counters.get(metric);

  if (samples === undefined) {
    return;
  }

  lines.push(`# HELP ${metric} ${help}`);
  lines.push(`# TYPE ${metric} counter`);

  for (const [key, value] of Array.from(samples.entries()).sort(([left], [right]) => left.localeCompare(right))) {
    lines.push(`${metric}${key} ${formatMetricNumber(value)}`);
  }
}

function collectSummary(
  lines: string[],
  summaries: Map<string, Map<string, { count: number; sum: number }>>,
  metric: string,
  help: string
): void {
  const samples = summaries.get(metric);

  if (samples === undefined) {
    return;
  }

  lines.push(`# HELP ${metric} ${help}`);
  lines.push(`# TYPE ${metric} summary`);

  for (const [key, value] of Array.from(samples.entries()).sort(([left], [right]) => left.localeCompare(right))) {
    lines.push(`${metric}_count${key} ${formatMetricNumber(value.count)}`);
    lines.push(`${metric}_sum${key} ${formatMetricNumber(value.sum)}`);
  }
}

function labelsFor(
  identity: TranslationMetricIdentity,
  labels: TranslationLanguageMetricDimensions
): TranslationMetricLabels {
  return {
    environment: boundedLabel(identity.environment),
    host: boundedLabel(identity.host),
    service: boundedLabel(identity.service),
    version: boundedLabel(identity.version),
    language: boundedLabel(labels.language),
    provider: boundedLabel(labels.provider),
    result: boundedLabel(labels.result),
    retry: boundedLabel(labels.retry)
  };
}

function incrementCounter(
  counters: Map<string, Map<string, number>>,
  metric: string,
  labels: TranslationMetricLabels,
  value: number
): void {
  const samples = samplesFor(counters, metric);
  const key = labelsKey(labels);

  samples.set(key, (samples.get(key) ?? 0) + Math.max(0, value));
}

function observeSummary(
  summaries: Map<string, Map<string, { count: number; sum: number }>>,
  metric: string,
  labels: TranslationMetricLabels,
  value: number
): void {
  const samples = samplesFor(summaries, metric);
  const key = labelsKey(labels);
  const existing = samples.get(key) ?? {
    count: 0,
    sum: 0
  };

  samples.set(key, {
    count: existing.count + 1,
    sum: existing.sum + Math.max(0, value)
  });
}

function samplesFor<T>(container: Map<string, Map<string, T>>, metric: string): Map<string, T> {
  const existing = container.get(metric);

  if (existing !== undefined) {
    return existing;
  }

  const samples = new Map<string, T>();

  container.set(metric, samples);

  return samples;
}

function labelsKey(labels: TranslationMetricLabels): string {
  const entries = Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}="${escapeLabelValue(value)}"`);

  return `{${entries.join(",")}}`;
}

function stringAttribute(event: RuntimeTelemetryEvent, key: string): string | undefined {
  const value = event.attributes?.[key];

  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function numberAttribute(event: RuntimeTelemetryEvent, key: string): number | undefined {
  const value = event.attributes?.[key];

  return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : undefined;
}

function boundedLabel(value: string): string {
  const bounded = value
    .trim()
    .replace(/[^a-zA-Z0-9_.:-]+/gu, "_")
    .slice(0, 64);

  return bounded.length > 0 ? bounded : "unknown";
}

function escapeLabelValue(value: string): string {
  return value
    .replace(/\\/gu, "\\\\")
    .replace(/\n/gu, "\\n")
    .replace(/"/gu, "\\\"");
}

function formatMetricNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(6).replace(/0+$/u, "").replace(/\.$/u, "");
}
