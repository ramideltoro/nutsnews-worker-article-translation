import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createInMemoryIdempotencyStore,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport,
  type RuntimeClock,
  type RuntimeHandlerResult,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimReleaseResult,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  TranslationBrokerOutbox,
  TranslationDatabaseTransaction,
  TranslationDatabaseTransactionRunner,
  TranslationDependencies,
  TranslationDependencyProbe,
  TranslationLanguagePolicy,
  TranslationLanguagePolicySnapshot,
  TranslationLanguageResultKey,
  TranslationPersistencePublication,
  TranslationPrompt,
  TranslationPromptRegistry,
  TranslationQualityValidator,
  TranslationQualityValidationRequest,
  TranslationQualityValidationResult,
  TranslationQwenClient,
  TranslationQwenRequest,
  TranslationStateStore,
  TranslationStoredLanguageResult,
  TranslationWorkHandler,
  TranslationWorkTools
} from "./dependencies.js";

export class ManualTranslationClock implements RuntimeClock {
  private current: Date;

  constructor(initial = "2026-07-23T00:00:00.000Z") {
    this.current = new Date(initial);
  }

  now(): Date {
    return new Date(this.current.getTime());
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export class InMemoryTranslationStateStore implements TranslationStateStore {
  readonly name: string = "local-translation-state";
  status: TranslationDependencyProbe["status"] = "ok";
  readonly languageResults: TranslationStoredLanguageResult[] = [];
  private readonly store;

  constructor(clock: RuntimeClock = new ManualTranslationClock()) {
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): TranslationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local translation state ready" : "local translation state degraded"
    };
  }

  claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    return this.store.claim(idempotencyKey, context);
  }

  markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    return this.store.markCompleted(idempotencyKey, completion);
  }

  markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    return this.store.markFailed(idempotencyKey, failure);
  }

  releaseClaim(
    idempotencyKey: string,
    failure: RuntimeIdempotencyFailure
  ): Promise<RuntimeIdempotencyClaimReleaseResult> {
    return this.store.releaseClaim(idempotencyKey, failure);
  }

  findLanguageResult(key: TranslationLanguageResultKey, transaction: TranslationDatabaseTransaction): Promise<TranslationStoredLanguageResult | undefined> {
    void transaction;
    return Promise.resolve(this.languageResults.find((result) => languageResultMatches(result, key)));
  }

  recordLanguageResult(result: TranslationStoredLanguageResult, transaction: TranslationDatabaseTransaction): Promise<TranslationStoredLanguageResult> {
    void transaction;
    const existingIndex = this.languageResults.findIndex((stored) => stored.resultId === result.resultId);

    if (existingIndex >= 0) {
      this.languageResults[existingIndex] = result;
    } else {
      this.languageResults.push(result);
    }

    return Promise.resolve(result);
  }

  markPersistencePublished(
    resultId: string,
    publication: TranslationPersistencePublication,
    transaction: TranslationDatabaseTransaction
  ): Promise<TranslationStoredLanguageResult> {
    void transaction;
    const existingIndex = this.languageResults.findIndex((result) => result.resultId === resultId);
    const existing = this.languageResults[existingIndex];

    if (existingIndex < 0 || existing === undefined) {
      return Promise.reject(new Error(`No local translation result recorded for ${resultId}.`));
    }

    const updated = {
      ...existing,
      persistencePublication: publication
    };

    this.languageResults[existingIndex] = updated;

    return Promise.resolve(updated);
  }
}

export class LocalTranslationTransactionRunner implements TranslationDatabaseTransactionRunner {
  readonly name: string = "local-database-transactions";
  status: TranslationDependencyProbe["status"] = "ok";
  readonly transactions: TranslationDatabaseTransaction[] = [];

  probe(): TranslationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local transaction runner ready" : "local transaction runner degraded"
    };
  }

  async withTransaction<T>(
    operation: (transaction: TranslationDatabaseTransaction) => Promise<T>,
    signal?: AbortSignal
  ): Promise<T> {
    signal?.throwIfAborted();
    const transaction = {
      transactionId: `local-transaction-${String(this.transactions.length + 1)}`
    };

    this.transactions.push(transaction);

    const value = await operation(transaction);
    signal?.throwIfAborted();

    return value;
  }
}

export class LocalTranslationBrokerOutbox implements TranslationBrokerOutbox {
  readonly name: string = "local-broker-outbox";
  status: TranslationDependencyProbe["status"] = "ok";
  readonly records: { readonly command: BrokerPublishCommand; readonly receipt: BrokerPublishReceipt }[] = [];

  probe(): TranslationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local broker outbox ready" : "local broker outbox degraded"
    };
  }

  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    this.records.push({
      command,
      receipt
    });
    return Promise.resolve();
  }
}

export class LocalTranslationQwenClient implements TranslationQwenClient {
  readonly name: string = "local-qwen-client";
  status: TranslationDependencyProbe["status"] = "ok";
  readonly requests: TranslationQwenRequest[] = [];
  readonly responsesByLanguage = new Map<string, unknown>();
  readonly errorsByLanguage = new Map<string, unknown>();
  response: unknown = undefined;
  error: unknown = undefined;

  probe(): TranslationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local Qwen endpoint ready" : "local Qwen endpoint degraded"
    };
  }

  translate(request: TranslationQwenRequest): Promise<unknown> {
    request.signal?.throwIfAborted();
    this.requests.push(request);

    const languageError = this.errorsByLanguage.get(request.input.targetLanguage);

    if (languageError !== undefined) {
      return Promise.reject(toError(languageError));
    }

    if (this.error !== undefined) {
      return Promise.reject(toError(this.error));
    }

    return Promise.resolve(this.responsesByLanguage.get(request.input.targetLanguage) ?? this.response ?? defaultQwenResponse(request.input.targetLanguage));
  }
}

export class LocalTranslationPromptRegistry implements TranslationPromptRegistry {
  readonly name: string = "local-prompt-registry";
  status: TranslationDependencyProbe["status"] = "ok";
  readonly prompts = new Map<string, TranslationPrompt>([
    [
      "summary-translation-v1",
      {
        id: "summary-translation-v1",
        version: "0.1.0",
        purpose: "summary-translation",
        instructions: "Translate the approved summary into the requested target language and return a compact publication-ready summary."
      }
    ]
  ]);

  probe(): TranslationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local prompt registry ready" : "local prompt registry degraded"
    };
  }

  getPrompt(id: string): Promise<TranslationPrompt> {
    const prompt = this.prompts.get(id);

    if (prompt === undefined) {
      return Promise.reject(new Error(`Unknown local translation prompt ${id}.`));
    }

    return Promise.resolve(prompt);
  }
}

export class LocalTranslationLanguagePolicy implements TranslationLanguagePolicy {
  readonly name: string = "local-language-policy";
  status: TranslationDependencyProbe["status"] = "ok";
  policy: TranslationLanguagePolicySnapshot = {
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
  };

  probe(): TranslationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local language policy ready" : "local language policy degraded"
    };
  }

  getPolicy(): Promise<TranslationLanguagePolicySnapshot> {
    return Promise.resolve(this.policy);
  }
}

export class LocalTranslationQualityValidator implements TranslationQualityValidator {
  readonly name: string = "local-quality-validator";
  status: TranslationDependencyProbe["status"] = "ok";

  probe(): TranslationDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local quality validator ready" : "local quality validator degraded"
    };
  }

  validate(request: TranslationQualityValidationRequest): TranslationQualityValidationResult {
    const normalizedSummary = request.summary.trim().replace(/\s+/gu, " ");
    const length = Array.from(normalizedSummary).length;

    if (normalizedSummary.length === 0) {
      return invalidQuality("empty_summary", true);
    }

    if (length < request.minSummaryChars) {
      return invalidQuality("summary_too_short", true);
    }

    if (length > request.maxSummaryChars) {
      return invalidQuality("summary_too_long", true);
    }

    if (normalizedSummary.includes("\uFFFD") || hasDisallowedControlCharacter(normalizedSummary)) {
      return invalidQuality("encoding_error", true);
    }

    if (/^(as an ai|i cannot|sorry|translation:|here is|here's)/iu.test(normalizedSummary)) {
      return invalidQuality("prohibited_boilerplate", true);
    }

    if (/https?:\/\/|^#|\n[-*]\s/u.test(normalizedSummary)) {
      return invalidQuality("summary_policy_violation", true);
    }

    if (request.qualityScore < request.minQualityScore) {
      return invalidQuality("translation_quality_below_threshold", true);
    }

    if (looksLikeSourceCopy(normalizedSummary, request.sourceLanguage, request.targetLanguage)) {
      return invalidQuality("source_copy_leakage", true);
    }

    if (!matchesTargetScript(normalizedSummary, request.targetLanguage)) {
      return invalidQuality("target_language_script_mismatch", true);
    }

    return {
      ok: true,
      normalizedSummary,
      auditCodes: [
        "non_empty",
        "length_bounds",
        "encoding",
        "boilerplate",
        "summary_policy",
        "quality_score",
        "target_language"
      ]
    };
  }
}

export class LocalTranslationWorkHandler implements TranslationWorkHandler {
  readonly name: string = "local-translation-work-handler";
  readonly handled: RuntimeMessageContext[] = [];
  result: RuntimeHandlerResult = {
    status: "ok"
  };
  handleGate: Promise<unknown> | undefined;
  onHandleStart: (() => void) | undefined;

  async handle(
    context: RuntimeMessageContext,
    tools: TranslationWorkTools,
    signal: AbortSignal
  ): Promise<RuntimeHandlerResult> {
    void tools;
    signal.throwIfAborted();
    this.onHandleStart?.();
    await waitForAbortable(this.handleGate, signal);
    signal.throwIfAborted();
    this.handled.push(context);

    return this.result;
  }
}

async function waitForAbortable(operation: Promise<unknown> | undefined, signal: AbortSignal): Promise<void> {
  if (operation === undefined) {
    return;
  }

  let onAbort: (() => void) | undefined;

  try {
    await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        onAbort = () => {
          reject(signal.reason instanceof Error
            ? signal.reason
            : new Error("Translation operation aborted."));
        };
        signal.addEventListener("abort", onAbort, {
          once: true
        });
        signal.throwIfAborted();
      })
    ]);
  } finally {
    if (onAbort !== undefined) {
      signal.removeEventListener("abort", onAbort);
    }
  }
}

export class LocalBrokerTransport implements RuntimeBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly inFlightDeliveryCount = 0;
  readonly assertedRoutes: WorkerRoute[] = [];
  readonly published: BrokerPublishCommand[] = [];
  private connected = false;
  private readonly consumers = new Map<WorkerStage, BrokerDeliveryHandler>();

  connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.assertConnected();
    this.assertedRoutes.push(...routes);
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    this.assertConnected();
    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    this.assertConnected();
    this.consumers.set(stage, handler);

    return Promise.resolve({
      stage,
      cancel: () => {
        this.consumers.delete(stage);
        return Promise.resolve();
      }
    });
  }

  deliverTranslation(delivery: RuntimeMessageDelivery = createMinimalTranslationDelivery()): Promise<RuntimeMessageProcessingResult> {
    const handler = this.consumers.get("translation");

    if (handler === undefined) {
      return Promise.reject(new Error("No local consumer is registered for translation."));
    }

    return handler(delivery);
  }

  drain(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.connected = false;
    this.consumers.clear();
    return Promise.resolve();
  }

  private assertConnected(): void {
    if (!this.connected) {
      throw new Error("Local broker transport is not connected.");
    }
  }
}

export function createLocalTranslationDependencies(options: {
  readonly clock?: RuntimeClock;
  readonly workHandler?: TranslationWorkHandler;
} = {}): TranslationDependencies {
  const clock = options.clock ?? new ManualTranslationClock();

  return {
    clock,
    stateStore: new InMemoryTranslationStateStore(clock),
    transactionRunner: new LocalTranslationTransactionRunner(),
    brokerOutbox: new LocalTranslationBrokerOutbox(),
    brokerTransport: new LocalBrokerTransport(),
    qwenClient: new LocalTranslationQwenClient(),
    promptRegistry: new LocalTranslationPromptRegistry(),
    languagePolicy: new LocalTranslationLanguagePolicy(),
    qualityValidator: new LocalTranslationQualityValidator(),
    workHandler: options.workHandler ?? new LocalTranslationWorkHandler()
  };
}

function languageResultMatches(
  result: TranslationStoredLanguageResult,
  key: TranslationLanguageResultKey
): boolean {
  return result.articleId === key.articleId
    && result.articleVersion === key.articleVersion
    && result.sourceLanguage === key.sourceLanguage
    && result.targetLanguage === key.targetLanguage
    && result.promptId === key.promptId
    && result.promptVersion === key.promptVersion
    && result.model === key.model;
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

function defaultQwenResponse(targetLanguage: string): unknown {
  const summaries = new Map<string, string>([
    [
      "fr",
      "Le rapport decrit une avancee utile pour le public avec des details suffisants pour publication."
    ],
    [
      "ja",
      "この記事は、地域社会に役立つ進展を具体的に伝えています。"
    ],
    [
      "de-CH",
      "Der Bericht beschreibt eine konkrete Entwicklung mit klarem Nutzen fuer die Oeffentlichkeit."
    ],
    [
      "de",
      "Der Bericht beschreibt eine konkrete Entwicklung mit erkennbarem Nutzen fuer die Oeffentlichkeit."
    ],
    [
      "el",
      "Το άρθρο περιγράφει μια χρήσιμη εξέλιξη για το κοινό με σαφείς λεπτομέρειες."
    ]
  ]);

  return {
    title: `Localized ${targetLanguage} title`,
    summary: summaries.get(targetLanguage) ?? "Le rapport decrit une avancee utile avec des details suffisants pour publication.",
    qualityScore: 93,
    latencyMs: 41,
    usage: {
      inputTokens: 120,
      outputTokens: 38,
      totalTokens: 158
    }
  };
}

function invalidQuality(
  reason: Extract<TranslationQualityValidationResult, { readonly ok: false }>["reason"],
  retryable: boolean
): TranslationQualityValidationResult {
  return {
    ok: false,
    reason,
    retryable,
    auditCodes: [
      reason
    ]
  };
}

function looksLikeSourceCopy(summary: string, sourceLanguage: string, targetLanguage: string): boolean {
  if (sourceLanguage === targetLanguage) {
    return true;
  }

  if (sourceLanguage === "en" && targetLanguage !== "en") {
    const englishPhraseLeakage = /\b(the\s+article|this\s+article|that\s+article|article\s+(reports?|describes?|contains?|is|has)|public-interest)\b/iu.test(summary);
    const markerMatches = summary.match(/\b(the|and|with|summary|reporting)\b/giu) ?? [];

    return englishPhraseLeakage || new Set(markerMatches.map((match) => match.toLowerCase())).size >= 2;
  }

  return false;
}

function matchesTargetScript(summary: string, targetLanguage: string): boolean {
  if (targetLanguage === "ja") {
    return /[\p{Script=Hiragana}\p{Script=Katakana}\p{Script=Han}]/u.test(summary);
  }

  if (targetLanguage === "el") {
    return /\p{Script=Greek}/u.test(summary);
  }

  return true;
}

function hasDisallowedControlCharacter(value: string): boolean {
  for (const character of value) {
    const codePoint = character.codePointAt(0);

    if (codePoint !== undefined && ((codePoint >= 0 && codePoint <= 8) || codePoint === 11 || codePoint === 12 || (codePoint >= 14 && codePoint <= 31))) {
      return true;
    }
  }

  return false;
}

export function createMinimalTranslationEnvelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  const route = getWorkerRoute("translation");
  const occurredAt = "2026-07-23T00:00:00.000Z";
  const envelope = {
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "translation",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4801",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4701",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4601",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "approval:translation:article-001",
    aggregate: {
      type: "article",
      id: "article-001",
      version: 1
    },
    occurredAt,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: occurredAt
    },
    producer: {
      name: "approval",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/approval/article-001/translation-task",
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(createMinimalTranslationPayload())
    },
    ...overrides
  };

  return assertWorkerEnvelope(envelope);
}

export function createMinimalTranslationPayload(
  overrides: Readonly<Record<string, unknown>> = {}
): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationTask,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4701",
    idempotencyKey: "approval:translation:article-001",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: "2026-07-23T00:00:00.000Z",
    articleId: "article-001",
    sourceLanguage: "en",
    targetLanguages: [
      "fr",
      "ja",
      "de-CH",
      "de",
      "el"
    ],
    reason: "new_article",
    existingLanguageCodes: [],
    ...overrides
  };
}

export function createMinimalTranslationDelivery(): RuntimeMessageDelivery {
  return {
    envelope: createMinimalTranslationEnvelope(),
    payload: createMinimalTranslationPayload(),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}
