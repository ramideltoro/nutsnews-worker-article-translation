import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock,
  RuntimeHandlerResult,
  RuntimeIdempotencyStore,
  RuntimeMessageContext
} from "@ramideltoro/nutsnews-worker-runtime";

export interface TranslationDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export interface TranslationStateStore extends RuntimeIdempotencyStore {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  findLanguageResult(key: TranslationLanguageResultKey, transaction: TranslationDatabaseTransaction): Promise<TranslationStoredLanguageResult | undefined>;
  recordLanguageResult(result: TranslationStoredLanguageResult, transaction: TranslationDatabaseTransaction): Promise<TranslationStoredLanguageResult>;
  markPersistencePublished(resultId: string, publication: TranslationPersistencePublication, transaction: TranslationDatabaseTransaction): Promise<TranslationStoredLanguageResult>;
}

export interface TranslationDatabaseTransaction {
  readonly transactionId: string;
}

export interface TranslationDatabaseTransactionRunner {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  withTransaction<T>(
    operation: (transaction: TranslationDatabaseTransaction) => Promise<T>,
    signal?: AbortSignal
  ): Promise<T>;
}

export interface TranslationBrokerOutbox {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
}

export interface TranslationQwenClient {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  translate(request: TranslationQwenRequest): Promise<unknown>;
}

export interface TranslationPrompt {
  readonly id: string;
  readonly version: string;
  readonly purpose: "summary-translation";
  readonly instructions: string;
}

export interface TranslationPromptRegistry {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  getPrompt(id: string): Promise<TranslationPrompt>;
}

export interface TranslationLanguagePolicySnapshot {
  readonly policyId: string;
  readonly version: string;
  readonly requiredLanguageCodes: readonly string[];
  readonly perLanguageConcurrency: number;
}

export interface TranslationLanguagePolicy {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  getPolicy(): Promise<TranslationLanguagePolicySnapshot>;
}

export interface TranslationQualityValidator {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  validate(request: TranslationQualityValidationRequest): TranslationQualityValidationResult | Promise<TranslationQualityValidationResult>;
}

export interface TranslationWorkTools {
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: TranslationDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface TranslationWorkHandler {
  readonly name: string;
  handle(
    context: RuntimeMessageContext,
    tools: TranslationWorkTools,
    signal: AbortSignal
  ): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface TranslationDependencies {
  readonly clock: RuntimeClock;
  readonly stateStore: TranslationStateStore;
  readonly transactionRunner: TranslationDatabaseTransactionRunner;
  readonly brokerOutbox: TranslationBrokerOutbox;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly qwenClient: TranslationQwenClient;
  readonly promptRegistry: TranslationPromptRegistry;
  readonly languagePolicy: TranslationLanguagePolicy;
  readonly qualityValidator: TranslationQualityValidator;
  readonly workHandler: TranslationWorkHandler;
}

export interface TranslationLanguageResultKey {
  readonly articleId: string;
  readonly articleVersion: number;
  readonly sourceLanguage: string;
  readonly targetLanguage: string;
  readonly promptId: string;
  readonly promptVersion: string;
  readonly model: string;
}

export interface TranslationPersistencePublication {
  readonly messageId: string;
  readonly idempotencyKey: string;
  readonly publishedAt: string;
}

export interface TranslationStoredLanguageResult extends TranslationLanguageResultKey {
  readonly resultId: string;
  readonly status: "success" | "permanent_failure";
  readonly failureReason?: string;
  readonly summary?: string;
  readonly summaryRef?: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: "application/json";
    readonly articleId: string;
    readonly targetLanguage: string;
    readonly resultId: string;
  };
  readonly qualityRef?: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: "application/json";
    readonly qualityScore: number;
    readonly resultId: string;
  };
  readonly aiUsageRef?: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: "application/json";
    readonly inputTokens: number;
    readonly outputTokens: number;
    readonly totalTokens: number;
  };
  readonly sourceMessageId: string;
  readonly correlationId: string;
  readonly traceparent: string;
  readonly latencyMs: number;
  readonly translatedAt: string;
  readonly persistencePublication?: TranslationPersistencePublication;
}

export interface TranslationQualityValidationRequest {
  readonly sourceLanguage: string;
  readonly targetLanguage: string;
  readonly summary: string;
  readonly qualityScore: number;
  readonly minQualityScore: number;
  readonly minSummaryChars: number;
  readonly maxSummaryChars: number;
}

export type TranslationQualityValidationResult = {
  readonly ok: true;
  readonly normalizedSummary: string;
  readonly auditCodes: readonly string[];
} | {
  readonly ok: false;
  readonly reason:
    | "empty_summary"
    | "summary_too_short"
    | "summary_too_long"
    | "encoding_error"
    | "source_copy_leakage"
    | "target_language_script_mismatch"
    | "prohibited_boilerplate"
    | "summary_policy_violation"
    | "translation_quality_below_threshold";
  readonly retryable: boolean;
  readonly auditCodes: readonly string[];
};

export interface TranslationQwenRequest {
  readonly model: string;
  readonly prompt: TranslationPrompt;
  readonly timeoutMs: number;
  readonly maxInputBytes: number;
  readonly signal?: AbortSignal;
  readonly deterministic: {
    readonly temperature: 0;
    readonly topP: 1;
  };
  readonly responseSchema: {
    readonly name: "translation_result_v1";
    readonly requiredFields: readonly string[];
  };
  readonly input: {
    readonly articleId: string;
    readonly articleVersion: number;
    readonly sourceLanguage: string;
    readonly targetLanguage: string;
  };
}

export class TranslationQwenError extends Error {
  readonly reason: "qwen-timeout" | "qwen-rate-limited" | "qwen-unauthorized" | "qwen-model-error";
  readonly retryable: boolean;
  readonly retryAfterMs: number | undefined;

  constructor(
    reason: TranslationQwenError["reason"],
    options: {
      readonly retryable: boolean;
      readonly retryAfterMs?: number;
    }
  ) {
    super(reason);
    this.name = "TranslationQwenError";
    this.reason = reason;
    this.retryable = options.retryable;
    this.retryAfterMs = options.retryAfterMs;
  }
}
