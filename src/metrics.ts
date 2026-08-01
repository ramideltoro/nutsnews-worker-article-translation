import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeServiceIdentity,
  type RuntimeTelemetryEvent
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  emitTelemetryBestEffort,
  runTelemetryBestEffort
} from "./telemetry.js";

type MetricLabels = Readonly<Record<string, string>>;

interface TranslationMetricIdentity {
  readonly environment: string;
}

export interface TranslationRuntimeMetricIdentity extends RuntimeServiceIdentity {
  readonly revision?: string;
  readonly deployment?: "local" | "test" | "shadow" | "production" | "unknown";
  readonly adapter?: "in_memory" | "mixed" | "production" | "unknown";
}

interface HistogramState {
  readonly bucketCounts: number[];
  count: number;
  sum: number;
}

export interface TranslationPrometheusMetricsSinkOptions extends Omit<
  PrometheusRuntimeTelemetrySinkOptions,
  "cardinality" | "expectedActive" | "identity"
> {
  readonly identity: TranslationRuntimeMetricIdentity;
  readonly expectedActive: boolean;
  readonly allowedLanguages?: readonly string[];
}

export type TranslationRuntimeMetricsSink = PrometheusRuntimeTelemetrySink;

export interface TranslationPrometheusMetricsSink extends TranslationRuntimeMetricsSink {
  readonly stageMetricLabels: readonly [
    "environment",
    "service",
    "outcome"
  ];
  readonly translationMetricLabels: readonly [
    "environment",
    "service",
    "stage",
    "outcome",
    "language",
    "provider"
  ];
}

const TRANSLATION_STAGE = "translation";
const TRANSLATION_QUEUE = "nutsnews.worker.translation.v1";
const DEFAULT_LANGUAGES = [
  "fr",
  "ja",
  "de-CH",
  "de",
  "el"
] as const;
const SUPPORTED_LANGUAGES = new Set<string>(DEFAULT_LANGUAGES);
const ALLOWED_PROVIDERS = new Set([
  "local_ai"
]);
const ALLOWED_OUTCOMES = new Set([
  "success",
  "duplicate",
  "invalid",
  "failure",
  "retry",
  "dlq"
]);
const COMPLETING_MESSAGE_EVENTS = new Set([
  "runtime.message.accepted",
  "runtime.message.duplicate",
  "runtime.message.invalid",
  "runtime.message.retry",
  "runtime.message.dlq"
]);
const TRANSLATION_STAGE_OUTCOMES = [
  "success",
  "duplicate",
  "invalid",
  "retry",
  "dlq",
  "failure"
] as const;
const RUNTIME_DEPENDENCIES = [
  "article-translation",
  "article-translation-work-handler",
  "translation-shell"
] as const;
export const TRANSLATION_RUNTIME_HEALTH_CHECKS = [
  "process",
  "service-started",
  "broker-lifecycle",
  "rabbitmq-consumer",
  "translation-state",
  "database-transactions",
  "broker-outbox",
  "qwen-client",
  "prompt-registry",
  "language-policy",
  "quality-validator",
  "shadow-mode"
] as const;
export const TRANSLATION_DURATION_BUCKETS_SECONDS = [
  0.005,
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  30,
  60,
  120,
  300
] as const;

export function createTranslationPrometheusMetricsSink(
  options: TranslationPrometheusMetricsSinkOptions
): TranslationPrometheusMetricsSink {
  const runtimeMetrics = createPrometheusRuntimeTelemetrySink({
    identity: options.identity,
    defaultQueue: options.defaultQueue ?? TRANSLATION_QUEUE,
    cardinality: {
      dependencies: RUNTIME_DEPENDENCIES,
      healthChecks: TRANSLATION_RUNTIME_HEALTH_CHECKS
    },
    expectedActive: options.expectedActive
  });
  const counters = new Map<string, Map<string, number>>();
  const histograms = new Map<string, Map<string, HistogramState>>();
  const identity = {
    environment: options.identity.environment
  };
  initializeCanonicalStageMetrics(identity, counters, histograms);
  const allowedLanguages = new Set(
    (options.allowedLanguages ?? DEFAULT_LANGUAGES)
      .filter((language) => SUPPORTED_LANGUAGES.has(language))
  );
  let lastSuccessTimestampSeconds = 0;

  runtimeMetrics.setLastSuccessTimestamp(lastSuccessTimestampSeconds);
  runtimeMetrics.setInFlight(options.defaultQueue ?? TRANSLATION_QUEUE, 0);

  return {
    get allowedLabels() {
      return runtimeMetrics.allowedLabels;
    },
    stageMetricLabels: [
      "environment",
      "service",
      "outcome"
    ],
    translationMetricLabels: [
      "environment",
      "service",
      "stage",
      "outcome",
      "language",
      "provider"
    ],
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      await emitTelemetryBestEffort(runtimeMetrics, event);
      const consumerFailure = consumerFailureHealthEvent(event);

      if (consumerFailure !== undefined) {
        await emitTelemetryBestEffort(runtimeMetrics, consumerFailure);
      }

      runTelemetryBestEffort(() => {
        const eventTimestamp = lastSuccessTimestampFromEvent(event);

        if (eventTimestamp !== undefined) {
          lastSuccessTimestampSeconds = setMonotonicLastSuccessTimestamp(
            runtimeMetrics,
            lastSuccessTimestampSeconds,
            eventTimestamp
          );
        }
      });
      runTelemetryBestEffort(() => observeStageEvent(event, identity, counters, histograms));
      runTelemetryBestEffort(() => observeTranslationLanguageEvent(event, identity, allowedLanguages, counters, histograms));
    },
    collect(): string {
      let runtimeOutput = "";

      runTelemetryBestEffort(() => {
        runtimeOutput = runtimeMetrics.collect();
      });

      return `${runtimeOutput}${collectCustomMetrics(counters, histograms)}`;
    },
    setInFlight(queue, value): void {
      runTelemetryBestEffort(() => runtimeMetrics.setInFlight(queue, value));
    },
    setShutdownDraining(draining): void {
      runTelemetryBestEffort(() => runtimeMetrics.setShutdownDraining(draining));
    },
    setExpectedActive(expected): void {
      runTelemetryBestEffort(() => runtimeMetrics.setExpectedActive(expected));
    },
    setLastSuccessTimestamp(timestampSeconds): void {
      lastSuccessTimestampSeconds = setMonotonicLastSuccessTimestamp(
        runtimeMetrics,
        lastSuccessTimestampSeconds,
        timestampSeconds
      );
    }
  };
}

function observeStageEvent(
  event: RuntimeTelemetryEvent,
  identity: TranslationMetricIdentity,
  counters: Map<string, Map<string, number>>,
  histograms: Map<string, Map<string, HistogramState>>
): void {
  if (
    !COMPLETING_MESSAGE_EVENTS.has(event.name)
    || event.stage !== TRANSLATION_STAGE
    || event.queue !== TRANSLATION_QUEUE
  ) {
    return;
  }

  const labels = stageLabels(identity, event);
  incrementCounter(counters, "nutsnews_worker_uplift_stage_events_total", labels, 1);

  const durationSeconds = secondsFromMilliseconds(event.durationMs);

  if (durationSeconds !== undefined) {
    observeHistogram(
      histograms,
      "nutsnews_worker_uplift_stage_latency_seconds",
      stageHistogramLabels(identity),
      durationSeconds
    );
  }
}

function observeTranslationLanguageEvent(
  event: RuntimeTelemetryEvent,
  identity: TranslationMetricIdentity,
  allowedLanguages: ReadonlySet<string>,
  counters: Map<string, Map<string, number>>,
  histograms: Map<string, Map<string, HistogramState>>
): void {
  if (
    event.name !== "runtime.dependency.observed"
    || event.stage !== TRANSLATION_STAGE
    || event.queue !== TRANSLATION_QUEUE
    || event.attributes?.event !== "translation.language.reviewed"
  ) {
    return;
  }

  const labels = languageLabels(identity, event, allowedLanguages);
  const latencyMs = finiteNonNegativeNumber(event.durationMs) ?? numberAttribute(event, "latencyMs");
  const reusedResult = event.attributes.reusedResult === true;

  incrementCounter(counters, "nutsnews_translation_language_results_total", labels, 1);

  if (reusedResult) {
    return;
  }

  if (latencyMs !== undefined) {
    observeHistogram(
      histograms,
      "nutsnews_translation_language_duration_seconds",
      labels,
      latencyMs / 1_000
    );
  }

  recordTokenMetric(counters, labels, "input", numberAttribute(event, "inputTokens"));
  recordTokenMetric(counters, labels, "output", numberAttribute(event, "outputTokens"));
  recordTokenMetric(counters, labels, "total", numberAttribute(event, "totalTokens"));
}

function recordTokenMetric(
  counters: Map<string, Map<string, number>>,
  labels: MetricLabels,
  kind: "input" | "output" | "total",
  value: number | undefined
): void {
  if (value === undefined) {
    return;
  }

  incrementCounter(counters, `nutsnews_translation_language_${kind}_tokens_total`, labels, value);
}

function collectCustomMetrics(
  counters: Map<string, Map<string, number>>,
  histograms: Map<string, Map<string, HistogramState>>
): string {
  const lines: string[] = [];

  collectCounter(lines, counters, "nutsnews_worker_uplift_stage_events_total", "Worker stage delivery outcomes using bounded lifecycle labels.");
  collectHistogram(lines, histograms, "nutsnews_worker_uplift_stage_latency_seconds", "Worker stage delivery latency in seconds.");
  collectCounter(lines, counters, "nutsnews_translation_language_results_total", "Per-language translation outcomes using bounded language and provider labels.");
  collectHistogram(lines, histograms, "nutsnews_translation_language_duration_seconds", "Per-language translation dependency latency in seconds.");
  collectCounter(lines, counters, "nutsnews_translation_language_input_tokens_total", "Per-language translation input tokens.");
  collectCounter(lines, counters, "nutsnews_translation_language_output_tokens_total", "Per-language translation output tokens.");
  collectCounter(lines, counters, "nutsnews_translation_language_total_tokens_total", "Per-language translation total tokens.");

  return lines.length === 0 ? "" : `${lines.join("\n")}\n`;
}

function initializeCanonicalStageMetrics(
  identity: TranslationMetricIdentity,
  counters: Map<string, Map<string, number>>,
  histograms: Map<string, Map<string, HistogramState>>
): void {
  const stageCounters = samplesFor(counters, "nutsnews_worker_uplift_stage_events_total");

  for (const outcome of TRANSLATION_STAGE_OUTCOMES) {
    stageCounters.set(labelsKey({
      ...stageHistogramLabels(identity),
      outcome
    }), 0);
  }

  samplesFor(histograms, "nutsnews_worker_uplift_stage_latency_seconds").set(
    labelsKey(stageHistogramLabels(identity)),
    {
      bucketCounts: TRANSLATION_DURATION_BUCKETS_SECONDS.map(() => 0),
      count: 0,
      sum: 0
    }
  );
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

  for (const [key, value] of sortedEntries(samples)) {
    lines.push(`${metric}${key} ${formatMetricNumber(value)}`);
  }
}

function collectHistogram(
  lines: string[],
  histograms: Map<string, Map<string, HistogramState>>,
  metric: string,
  help: string
): void {
  const samples = histograms.get(metric);

  if (samples === undefined) {
    return;
  }

  lines.push(`# HELP ${metric} ${help}`);
  lines.push(`# TYPE ${metric} histogram`);

  for (const [key, state] of sortedEntries(samples)) {
    for (const [index, boundary] of TRANSLATION_DURATION_BUCKETS_SECONDS.entries()) {
      lines.push(`${metric}_bucket${labelsWithLe(key, String(boundary))} ${formatMetricNumber(state.bucketCounts[index] ?? 0)}`);
    }

    lines.push(`${metric}_bucket${labelsWithLe(key, "+Inf")} ${formatMetricNumber(state.count)}`);
    lines.push(`${metric}_sum${key} ${formatMetricNumber(state.sum)}`);
    lines.push(`${metric}_count${key} ${formatMetricNumber(state.count)}`);
  }
}

function stageLabels(identity: TranslationMetricIdentity, event: RuntimeTelemetryEvent): MetricLabels {
  return {
    ...stageHistogramLabels(identity),
    outcome: stageOutcome(event)
  };
}

function stageHistogramLabels(identity: TranslationMetricIdentity): MetricLabels {
  return {
    environment: boundedLabel(identity.environment),
    service: TRANSLATION_STAGE
  };
}

function languageBaseLabels(
  identity: TranslationMetricIdentity,
  event: RuntimeTelemetryEvent,
  outcome: string
): MetricLabels {
  return {
    ...stageHistogramLabels(identity),
    stage: event.stage === TRANSLATION_STAGE ? TRANSLATION_STAGE : "unknown",
    outcome
  };
}

function languageLabels(
  identity: TranslationMetricIdentity,
  event: RuntimeTelemetryEvent,
  allowedLanguages: ReadonlySet<string>
): MetricLabels {
  const language = stringAttribute(event, "targetLanguage");
  const provider = stringAttribute(event, "provider");

  return {
    ...languageBaseLabels(identity, event, boundedOutcome(event.outcome)),
    language: language !== undefined && allowedLanguages.has(language) ? boundedLabel(language) : "unknown",
    provider: provider !== undefined && ALLOWED_PROVIDERS.has(provider) ? provider : "unknown"
  };
}

function stageOutcome(event: RuntimeTelemetryEvent): string {
  switch (event.name) {
    case "runtime.message.accepted":
      return "success";
    case "runtime.message.duplicate":
      return "duplicate";
    case "runtime.message.invalid":
      return "invalid";
    case "runtime.message.retry":
      return "retry";
    case "runtime.message.dlq":
      return "dlq";
    default:
      return boundedOutcome(event.outcome);
  }
}

function boundedOutcome(outcome: string | undefined): string {
  return outcome !== undefined && ALLOWED_OUTCOMES.has(outcome) ? outcome : "failure";
}

function incrementCounter(
  counters: Map<string, Map<string, number>>,
  metric: string,
  labels: MetricLabels,
  value: number
): void {
  const samples = samplesFor(counters, metric);
  const key = labelsKey(labels);

  samples.set(key, (samples.get(key) ?? 0) + Math.max(0, value));
}

function observeHistogram(
  histograms: Map<string, Map<string, HistogramState>>,
  metric: string,
  labels: MetricLabels,
  value: number
): void {
  const samples = samplesFor(histograms, metric);
  const key = labelsKey(labels);
  const existing = samples.get(key) ?? {
    bucketCounts: TRANSLATION_DURATION_BUCKETS_SECONDS.map(() => 0),
    count: 0,
    sum: 0
  };
  const boundedValue = Math.max(0, value);

  for (const [index, boundary] of TRANSLATION_DURATION_BUCKETS_SECONDS.entries()) {
    if (boundedValue <= boundary) {
      existing.bucketCounts[index] = (existing.bucketCounts[index] ?? 0) + 1;
    }
  }

  existing.count += 1;
  existing.sum += boundedValue;
  samples.set(key, existing);
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

function labelsKey(labels: MetricLabels): string {
  const entries = Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}="${escapeLabelValue(value)}"`);

  return `{${entries.join(",")}}`;
}

function labelsWithLe(key: string, boundary: string): string {
  return `${key.slice(0, -1)},le="${boundary}"}`;
}

function stringAttribute(event: RuntimeTelemetryEvent, key: string): string | undefined {
  const value = event.attributes?.[key];

  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function numberAttribute(event: RuntimeTelemetryEvent, key: string): number | undefined {
  return finiteNonNegativeNumber(event.attributes?.[key]);
}

function finiteNonNegativeNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : undefined;
}

function secondsFromMilliseconds(value: unknown): number | undefined {
  const milliseconds = finiteNonNegativeNumber(value);

  return milliseconds === undefined ? undefined : milliseconds / 1_000;
}

function lastSuccessTimestampFromEvent(event: RuntimeTelemetryEvent): number | undefined {
  if (event.stage !== TRANSLATION_STAGE || event.queue !== TRANSLATION_QUEUE) {
    return undefined;
  }

  const isAccepted = event.name === "runtime.message.accepted";
  const isAlreadyCompletedDuplicate = event.name === "runtime.message.duplicate"
    && stringAttribute(event, "completedAt") !== undefined;

  if (!isAccepted && !isAlreadyCompletedDuplicate) {
    return undefined;
  }

  const timestampSeconds = Date.parse(event.at) / 1_000;

  return Number.isFinite(timestampSeconds) && timestampSeconds >= 0
    ? Math.floor(timestampSeconds)
    : undefined;
}

function consumerFailureHealthEvent(event: RuntimeTelemetryEvent): RuntimeTelemetryEvent | undefined {
  if (
    event.name !== "runtime.broker.consumer_state_changed"
    || event.stage !== TRANSLATION_STAGE
    || event.queue !== TRANSLATION_QUEUE
  ) {
    return undefined;
  }

  const activeConsumers = finiteNonNegativeNumber(event.attributes?.activeConsumers)
    ?? (event.outcome === "active" ? 1 : 0);

  if (activeConsumers > 0) {
    return undefined;
  }

  return {
    name: "runtime.health.evaluated",
    level: "warn",
    at: event.at,
    stage: TRANSLATION_STAGE,
    outcome: "unhealthy",
    attributes: {
      probe: "readiness",
      status: "unhealthy",
      checkCount: 1,
      checks: [
        {
          name: "rabbitmq-consumer",
          status: "unhealthy",
          critical: true,
          // Runtime requires the field structurally, while a transition is not
          // a timed probe execution. A non-finite value updates only gauges.
          durationMs: Number.NaN
        }
      ]
    }
  };
}

function setMonotonicLastSuccessTimestamp(
  runtimeMetrics: PrometheusRuntimeTelemetrySink,
  previousTimestampSeconds: number,
  timestampSeconds: number
): number {
  if (!Number.isFinite(timestampSeconds) || timestampSeconds < 0) {
    runtimeMetrics.setLastSuccessTimestamp(timestampSeconds);

    return previousTimestampSeconds;
  }

  const nextTimestampSeconds = Math.max(
    previousTimestampSeconds,
    Math.floor(timestampSeconds)
  );

  runtimeMetrics.setLastSuccessTimestamp(nextTimestampSeconds);

  return nextTimestampSeconds;
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

function sortedEntries<T>(map: Map<string, T>): [string, T][] {
  return [
    ...map.entries()
  ].sort(([left], [right]) => left.localeCompare(right));
}
