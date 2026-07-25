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
}

export interface TranslationDatabaseTransaction {
  readonly transactionId: string;
}

export interface TranslationDatabaseTransactionRunner {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  withTransaction<T>(operation: (transaction: TranslationDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface TranslationBrokerOutbox {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
}

export interface TranslationQwenClient {
  readonly name: string;
  probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
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
}

export interface TranslationWorkTools {
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: TranslationDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface TranslationWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: TranslationWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface TranslationDependencies {
  readonly clock: RuntimeClock;
  readonly stateStore: TranslationStateStore;
  readonly transactionRunner: TranslationDatabaseTransactionRunner;
  readonly brokerOutbox: TranslationBrokerOutbox;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly qwenClient: TranslationQwenClient;
  readonly languagePolicy: TranslationLanguagePolicy;
  readonly qualityValidator: TranslationQualityValidator;
  readonly workHandler: TranslationWorkHandler;
}
