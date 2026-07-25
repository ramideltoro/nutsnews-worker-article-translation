import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  validateStagePayload,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerPublishCommand,
  type RuntimeHandlerResult,
  type RuntimeMessageContext,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { TranslationConfig } from "./config.js";
import {
  TranslationQwenError,
  type TranslationDependencies,
  type TranslationLanguagePolicySnapshot,
  type TranslationPrompt,
  type TranslationQwenRequest,
  type TranslationStoredLanguageResult,
  type TranslationWorkHandler,
  type TranslationWorkTools
} from "./dependencies.js";
import { stableUuid } from "./ids.js";

export interface ArticleTranslationWorkHandlerOptions {
  readonly config: TranslationConfig;
  readonly dependencies: TranslationDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
}

interface TranslationTaskInput {
  readonly articleId: string;
  readonly articleVersion: number;
  readonly sourceLanguage: string;
  readonly targetLanguages: readonly string[];
  readonly existingLanguageCodes: readonly string[];
  readonly pipelineRunId: string;
}

interface QwenTranslationDecision {
  readonly summary: string;
  readonly qualityScore: number;
  readonly latencyMs: number;
  readonly usage?: {
    readonly inputTokens: number;
    readonly outputTokens: number;
    readonly totalTokens: number;
  };
}

type TranslationOutcome = {
  readonly status: "success";
  readonly result: TranslationStoredLanguageResult;
} | {
  readonly status: "permanent_failure";
  readonly result: TranslationStoredLanguageResult;
} | {
  readonly status: "retry";
  readonly result: RuntimeHandlerResult;
};

const TRANSLATION_QUEUE = getWorkerRoute("translation").mainQueue.name;
const SUMMARY_UNSAFE_RE = /bearer |api_key=|apikey=|token=|secret=|password=|private_key|service_role/iu;

export function createArticleTranslationWorkHandler(options: ArticleTranslationWorkHandlerOptions): TranslationWorkHandler {
  return {
    name: "article-translation-work-handler",
    handle: (context, tools) => handleTranslation(context, tools, options)
  };
}

async function handleTranslation(
  context: RuntimeMessageContext,
  tools: TranslationWorkTools,
  options: ArticleTranslationWorkHandlerOptions
): Promise<RuntimeHandlerResult> {
  let input: TranslationTaskInput;

  try {
    input = translationInputFromContext(context);
  } catch (error: unknown) {
    return {
      status: "terminal-failure",
      reason: error instanceof Error ? normalizeReason(error.message) : "invalid-translation-input"
    };
  }

  const policy = await options.dependencies.languagePolicy.getPolicy();
  const prompt = await options.dependencies.promptRegistry.getPrompt(options.config.qwen.promptId);
  const requiredLanguages = policy.requiredLanguageCodes
    .filter((language) => input.targetLanguages.includes(language))
    .filter((language) => !input.existingLanguageCodes.includes(language));
  const completedLanguageCodes: string[] = [];
  const failedLanguageCodes: string[] = [];
  const summaryRefs: NonNullable<TranslationStoredLanguageResult["summaryRef"]>[] = [];

  for (const targetLanguage of requiredLanguages) {
    const existing = await tools.withTransaction((transaction) => options.dependencies.stateStore.findLanguageResult(languageKey(input, targetLanguage, prompt, options.config), transaction));

    if (existing?.status === "success") {
      completedLanguageCodes.push(targetLanguage);

      if (existing.summaryRef !== undefined) {
        summaryRefs.push(existing.summaryRef);
      }

      await publishPersistenceIfNeeded(context, input, existing, tools, options);
      await emitLanguageTelemetry(options, existing, true);
      continue;
    }

    const outcome = await translateLanguage(context, input, targetLanguage, prompt, policy, options);

    if (outcome.status === "retry") {
      return outcome.result;
    }

    const recorded = await tools.withTransaction((transaction) => options.dependencies.stateStore.recordLanguageResult(outcome.result, transaction));

    await emitLanguageTelemetry(options, recorded, false);

    if (recorded.status === "success") {
      completedLanguageCodes.push(targetLanguage);

      if (recorded.summaryRef !== undefined) {
        summaryRefs.push(recorded.summaryRef);
      }

      await publishPersistenceIfNeeded(context, input, recorded, tools, options);
    } else {
      failedLanguageCodes.push(targetLanguage);
    }
  }

  await publishTranslationStatus(context, input, requiredLanguages, completedLanguageCodes, failedLanguageCodes, summaryRefs, tools, options);

  return {
    status: "ok"
  };
}

async function translateLanguage(
  context: RuntimeMessageContext,
  input: TranslationTaskInput,
  targetLanguage: string,
  prompt: TranslationPrompt,
  policy: TranslationLanguagePolicySnapshot,
  options: ArticleTranslationWorkHandlerOptions
): Promise<TranslationOutcome> {
  void policy;
  const startedAtMs = options.dependencies.clock.now().getTime();
  const request = qwenRequest(input, targetLanguage, prompt, options.config);
  let raw: unknown;

  try {
    raw = await options.dependencies.qwenClient.translate(request);
  } catch (error: unknown) {
    if (isApprovedTransientQwenError(error)) {
      await emitLanguageRetryTelemetry(options, targetLanguage, prompt, error, elapsedMs(options, startedAtMs));

      return {
        status: "retry",
        result: error.retryAfterMs === undefined
          ? {
              status: "retry",
              reason: error.reason
            }
          : {
              status: "retry",
              reason: error.reason,
              retryAfterMs: error.retryAfterMs
            }
      };
    }

    return {
      status: "permanent_failure",
      result: storedLanguageResult(context, input, targetLanguage, prompt, options.config, options.dependencies.clock, {
        status: "permanent_failure",
        failureReason: error instanceof TranslationQwenError ? error.reason : "qwen-unauthorized",
        qualityScore: 0,
        latencyMs: elapsedMs(options, startedAtMs)
      })
    };
  }

  const validation = validateQwenTranslation(raw, options.config, elapsedMs(options, startedAtMs));

  if (validation.status !== "success") {
    return {
      status: "permanent_failure",
      result: storedLanguageResult(context, input, targetLanguage, prompt, options.config, options.dependencies.clock, {
        status: "permanent_failure",
        failureReason: validation.reason,
        qualityScore: validation.qualityScore,
        latencyMs: validation.latencyMs
      })
    };
  }

  return {
    status: "success",
    result: storedLanguageResult(context, input, targetLanguage, prompt, options.config, options.dependencies.clock, {
      status: "success",
      summary: validation.value.summary,
      qualityScore: validation.value.qualityScore,
      usage: validation.value.usage,
      latencyMs: validation.value.latencyMs
    })
  };
}

function qwenRequest(
  input: TranslationTaskInput,
  targetLanguage: string,
  prompt: TranslationPrompt,
  config: TranslationConfig
): TranslationQwenRequest {
  return {
    model: config.qwen.model,
    prompt,
    timeoutMs: config.qwen.totalTimeoutMs,
    maxInputBytes: config.qwen.maxInputBytes,
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
      articleId: input.articleId,
      articleVersion: input.articleVersion,
      sourceLanguage: input.sourceLanguage,
      targetLanguage
    }
  };
}

function storedLanguageResult(
  context: RuntimeMessageContext,
  input: TranslationTaskInput,
  targetLanguage: string,
  prompt: TranslationPrompt,
  config: TranslationConfig,
  clock: ArticleTranslationWorkHandlerOptions["dependencies"]["clock"],
  values: {
    readonly status: "success" | "permanent_failure";
    readonly failureReason?: string;
    readonly summary?: string;
    readonly qualityScore: number;
    readonly usage?: QwenTranslationDecision["usage"];
    readonly latencyMs: number;
  }
): TranslationStoredLanguageResult {
  const resultId = stableUuid([
    input.articleId,
    String(input.articleVersion),
    input.sourceLanguage,
    targetLanguage,
    prompt.id,
    prompt.version,
    config.qwen.model
  ]);
  const summaryRef = values.summary === undefined
    ? undefined
    : {
        kind: "backend-record",
        uri: `backend://worker-uplift/translation/${encodeURIComponent(input.articleId)}/${resultId}/${encodeURIComponent(targetLanguage)}/summary`,
        mediaType: "application/json",
        articleId: input.articleId,
        targetLanguage,
        resultId
      } as const;
  const qualityRef = {
    kind: "backend-record",
    uri: `backend://worker-uplift/translation/${encodeURIComponent(input.articleId)}/${resultId}/${encodeURIComponent(targetLanguage)}/quality`,
    mediaType: "application/json",
    qualityScore: values.qualityScore,
    resultId
  } as const;
  const aiUsageRef = values.usage === undefined
    ? undefined
    : {
        kind: "backend-record",
        uri: `backend://worker-uplift/translation/${encodeURIComponent(input.articleId)}/${resultId}/${encodeURIComponent(targetLanguage)}/ai-usage`,
        mediaType: "application/json",
        inputTokens: values.usage.inputTokens,
        outputTokens: values.usage.outputTokens,
        totalTokens: values.usage.totalTokens
      } as const;

  return {
    resultId,
    articleId: input.articleId,
    articleVersion: input.articleVersion,
    sourceLanguage: input.sourceLanguage,
    targetLanguage,
    promptId: prompt.id,
    promptVersion: prompt.version,
    model: config.qwen.model,
    status: values.status,
    ...(values.failureReason === undefined ? {} : {
      failureReason: values.failureReason
    }),
    ...(summaryRef === undefined ? {} : {
      summaryRef
    }),
    qualityRef,
    ...(aiUsageRef === undefined ? {} : {
      aiUsageRef
    }),
    sourceMessageId: context.envelope.messageId,
    correlationId: context.envelope.correlationId,
    traceparent: context.envelope.traceparent,
    latencyMs: values.latencyMs,
    translatedAt: runtimeNow(clock)
  };
}

async function publishPersistenceIfNeeded(
  context: RuntimeMessageContext,
  input: TranslationTaskInput,
  result: TranslationStoredLanguageResult,
  tools: TranslationWorkTools,
  options: ArticleTranslationWorkHandlerOptions
): Promise<void> {
  if (result.status !== "success" || result.persistencePublication !== undefined) {
    return;
  }

  const command = persistenceCommand(context, input, result, options.config);
  const receipt = await tools.publish(command);

  await tools.recordOutbox(command, receipt);
  await tools.withTransaction((transaction) => options.dependencies.stateStore.markPersistencePublished(result.resultId, {
    messageId: receipt.messageId,
    idempotencyKey: command.envelope.idempotencyKey,
    publishedAt: receipt.confirmedAt
  }, transaction));
}

async function publishTranslationStatus(
  context: RuntimeMessageContext,
  input: TranslationTaskInput,
  requiredLanguages: readonly string[],
  completedLanguageCodes: readonly string[],
  failedLanguageCodes: readonly string[],
  summaryRefs: readonly NonNullable<TranslationStoredLanguageResult["summaryRef"]>[],
  tools: TranslationWorkTools,
  options: ArticleTranslationWorkHandlerOptions
): Promise<void> {
  const missingLanguageCodes = requiredLanguages.filter((language) => !completedLanguageCodes.includes(language));
  const translationStatus = missingLanguageCodes.length === 0
    ? "complete"
    : failedLanguageCodes.length > 0
      ? "permanent_failure"
      : "partial";
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationResult,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: input.pipelineRunId,
    stageExecutionId: stableUuid([
      "translation-status",
      input.articleId,
      String(input.articleVersion),
      options.config.qwen.model
    ]),
    sourceMessageId: context.envelope.messageId,
    idempotencyKey: `translation:result:${input.articleId}:${String(input.articleVersion)}`,
    traceparent: context.envelope.traceparent,
    ...(context.envelope.tracestate === undefined ? {} : {
      tracestate: context.envelope.tracestate
    }),
    producedAt: runtimeNow(options.dependencies.clock),
    articleId: input.articleId,
    translationStatus,
    completedLanguageCodes,
    missingLanguageCodes,
    summaryRefs
  };
  const validation = validateStagePayload(payload);

  if (!validation.ok) {
    throw new Error(`Invalid translation result payload: ${validation.issues.map((issue) => `${issue.path}:${issue.code}`).join(", ")}`);
  }

  const command = commandForPayload(context, payload, "translation-status", options.config, options.dependencies.clock);
  const receipt = await tools.publish(command);

  await tools.recordOutbox(command, receipt);
}

function persistenceCommand(
  context: RuntimeMessageContext,
  input: TranslationTaskInput,
  result: TranslationStoredLanguageResult,
  config: TranslationConfig
): BrokerPublishCommand {
  const entityRef = {
    articleId: input.articleId,
    articleVersion: input.articleVersion,
    sourceLanguage: input.sourceLanguage,
    targetLanguage: result.targetLanguage,
    ...(result.summaryRef === undefined ? {} : {
      summaryRef: result.summaryRef
    }),
    ...(result.qualityRef === undefined ? {} : {
      qualityRef: result.qualityRef
    }),
    ...(result.aiUsageRef === undefined ? {} : {
      aiUsageRef: result.aiUsageRef
    })
  };
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: input.pipelineRunId,
    stageExecutionId: stableUuid([
      "persistence-command",
      result.resultId
    ]),
    sourceMessageId: context.envelope.messageId,
    idempotencyKey: `translation:persistence:${result.resultId}`,
    traceparent: context.envelope.traceparent,
    ...(context.envelope.tracestate === undefined ? {} : {
      tracestate: context.envelope.tracestate
    }),
    producedAt: result.translatedAt,
    commandId: result.resultId,
    commandKind: "save_summaries",
    backendOperation: "save-article-summaries-batch",
    entityRefs: [
      entityRef
    ],
    writeMode: "upsert",
    providerMode: "backend_postgres_primary"
  };
  const validation = validateStagePayload(payload);

  if (!validation.ok) {
    throw new Error(`Invalid persistence command payload: ${validation.issues.map((issue) => `${issue.path}:${issue.code}`).join(", ")}`);
  }

  return commandForPayload(context, payload, result.resultId, config, {
    now: () => new Date(result.translatedAt)
  });
}

function commandForPayload(
  context: RuntimeMessageContext,
  payload: Readonly<Record<string, unknown>>,
  suffix: string,
  config: TranslationConfig,
  clock: ArticleTranslationWorkHandlerOptions["dependencies"]["clock"]
): BrokerPublishCommand {
  const route = getWorkerRoute("persistence");
  const idempotencyKey = stringValue(payload.idempotencyKey, "idempotencyKey");
  const occurredAt = runtimeNow(clock);
  const envelope: WorkerMessageEnvelope = assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "persistence",
    messageId: stableUuid([
      "persistence-message",
      suffix
    ]),
    causationId: context.envelope.messageId,
    correlationId: context.envelope.correlationId,
    traceparent: context.envelope.traceparent,
    ...(context.envelope.tracestate === undefined ? {} : {
      tracestate: context.envelope.tracestate
    }),
    idempotencyKey,
    aggregate: {
      type: "article",
      id: context.envelope.aggregate.id,
      version: context.envelope.aggregate.version
    },
    occurredAt,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: occurredAt
    },
    producer: {
      name: config.serviceName,
      version: config.serviceVersion
    },
    payloadRef: {
      kind: "backend-record",
      uri: `backend://worker-uplift/translation/${encodeURIComponent(context.envelope.aggregate.id)}/${encodeURIComponent(suffix)}`,
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(payload)
    }
  });

  return {
    envelope,
    payload
  };
}

function validateQwenTranslation(
  raw: unknown,
  config: TranslationConfig,
  fallbackLatencyMs: number
): {
  readonly status: "success";
  readonly value: QwenTranslationDecision;
} | {
  readonly status: "permanent_failure";
  readonly reason: string;
  readonly qualityScore: number;
  readonly latencyMs: number;
} {
  if (!isRecord(raw)) {
    return invalidTranslation("invalid_ai_translation_schema", 0, fallbackLatencyMs);
  }

  const summary = raw.summary;
  const qualityScore = raw.qualityScore;
  const latencyMs = typeof raw.latencyMs === "number" && Number.isFinite(raw.latencyMs)
    ? Math.max(0, raw.latencyMs)
    : fallbackLatencyMs;

  if (typeof summary !== "string" || summary.trim().length === 0 || !score(qualityScore)) {
    return invalidTranslation("invalid_ai_translation_schema", 0, latencyMs);
  }

  const trimmed = summary.trim();

  if (SUMMARY_UNSAFE_RE.test(trimmed)) {
    return invalidTranslation("unsafe_summary_content", qualityScore, latencyMs);
  }

  if (qualityScore < config.quality.minScore) {
    return invalidTranslation("translation_quality_below_threshold", qualityScore, latencyMs);
  }

  const parsedUsage = usage(raw.usage);

  return {
    status: "success",
    value: {
      summary: trimmed,
      qualityScore,
      latencyMs,
      ...(parsedUsage === undefined ? {} : {
        usage: parsedUsage
      })
    }
  };
}

function invalidTranslation(reason: string, qualityScore: number, latencyMs: number) {
  return {
    status: "permanent_failure",
    reason,
    qualityScore,
    latencyMs
  } as const;
}

function translationInputFromContext(context: RuntimeMessageContext): TranslationTaskInput {
  return {
    articleId: stringValue(context.payload.articleId, "articleId"),
    articleVersion: context.envelope.aggregate.version,
    sourceLanguage: stringValue(context.payload.sourceLanguage, "sourceLanguage"),
    targetLanguages: stringArrayValue(context.payload.targetLanguages, "targetLanguages"),
    existingLanguageCodes: stringArrayValue(context.payload.existingLanguageCodes, "existingLanguageCodes"),
    pipelineRunId: stringValue(context.payload.pipelineRunId, "pipelineRunId")
  };
}

function languageKey(
  input: TranslationTaskInput,
  targetLanguage: string,
  prompt: TranslationPrompt,
  config: TranslationConfig
) {
  return {
    articleId: input.articleId,
    articleVersion: input.articleVersion,
    sourceLanguage: input.sourceLanguage,
    targetLanguage,
    promptId: prompt.id,
    promptVersion: prompt.version,
    model: config.qwen.model
  };
}

async function emitLanguageTelemetry(
  options: ArticleTranslationWorkHandlerOptions,
  result: TranslationStoredLanguageResult,
  reusedResult: boolean
): Promise<void> {
  await emitRuntimeTelemetry(options.telemetry, {
    name: "runtime.dependency.observed",
    level: result.status === "success" ? "info" : "warn",
    at: runtimeNow(options.dependencies.clock),
    stage: "translation",
    queue: TRANSLATION_QUEUE,
    durationMs: result.latencyMs,
    outcome: result.status === "success" ? "success" : "failure",
    attributes: {
      event: "translation.language.reviewed",
      dependency: "article-translation",
      articleId: result.articleId,
      articleVersion: result.articleVersion,
      targetLanguage: result.targetLanguage,
      provider: "local_ai",
      model: result.model,
      promptId: result.promptId,
      promptVersion: result.promptVersion,
      result: result.status,
      failureReason: result.failureReason,
      latencyMs: result.latencyMs,
      ...(result.aiUsageRef === undefined ? {} : {
        inputTokens: result.aiUsageRef.inputTokens,
        outputTokens: result.aiUsageRef.outputTokens,
        totalTokens: result.aiUsageRef.totalTokens
      }),
      reusedResult
    }
  });
}

async function emitLanguageRetryTelemetry(
  options: ArticleTranslationWorkHandlerOptions,
  targetLanguage: string,
  prompt: TranslationPrompt,
  error: TranslationQwenError,
  latencyMs: number
): Promise<void> {
  await emitRuntimeTelemetry(options.telemetry, {
    name: "runtime.dependency.observed",
    level: "warn",
    at: runtimeNow(options.dependencies.clock),
    stage: "translation",
    queue: TRANSLATION_QUEUE,
    durationMs: latencyMs,
    outcome: "retry",
    attributes: {
      event: "translation.language.reviewed",
      dependency: "article-translation",
      targetLanguage,
      provider: "local_ai",
      model: options.config.qwen.model,
      promptId: prompt.id,
      promptVersion: prompt.version,
      result: "retry",
      retryReason: error.reason,
      retryable: true,
      latencyMs,
      ...(error.retryAfterMs === undefined ? {} : {
        retryAfterMs: error.retryAfterMs
      }),
      reusedResult: false
    }
  });
}

function usage(value: unknown): QwenTranslationDecision["usage"] | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const inputTokens = value.inputTokens;
  const outputTokens = value.outputTokens;
  const totalTokens = value.totalTokens;

  if (!nonNegativeInteger(inputTokens) || !nonNegativeInteger(outputTokens) || !nonNegativeInteger(totalTokens)) {
    return undefined;
  }

  return {
    inputTokens,
    outputTokens,
    totalTokens
  };
}

function isApprovedTransientQwenError(error: unknown): error is TranslationQwenError {
  return error instanceof TranslationQwenError
    && error.retryable
    && (error.reason === "qwen-timeout" || error.reason === "qwen-rate-limited" || error.reason === "qwen-model-error");
}

function score(value: unknown): value is number {
  return Number.isInteger(value) && typeof value === "number" && value >= 0 && value <= 100;
}

function nonNegativeInteger(value: unknown): value is number {
  return Number.isInteger(value) && typeof value === "number" && value >= 0;
}

function stringValue(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`invalid-${field}`);
  }

  return value;
}

function stringArrayValue(value: unknown, field: string): readonly string[] {
  if (!isStringArray(value)) {
    throw new Error(`invalid-${field}`);
  }

  return value;
}

function isStringArray(value: unknown): value is readonly string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string" && item.length > 0);
}

function elapsedMs(options: ArticleTranslationWorkHandlerOptions, startedAtMs: number): number {
  return Math.max(0, options.dependencies.clock.now().getTime() - startedAtMs);
}

function normalizeReason(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 80) || "translation-error";
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
