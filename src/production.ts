import { createHash, randomUUID } from "node:crypto";

import {
  WORKER_DELIVERY_BEHAVIOR,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  computeRetryJitterMs,
  createRetryEnvelope,
  randomUuidMessageIdFactory,
  runtimeNow,
  runtimeTraceHeadersFromEnvelope,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport,
  type RuntimeClock,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  connect as amqpConnect,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
  type Options
} from "amqplib";
import {
  Pool,
  type PoolClient,
  type QueryResultRow
} from "pg";

import type { TranslationConfig } from "./config.js";
import {
  TranslationQwenError,
  type TranslationBrokerOutbox,
  type TranslationDatabaseTransaction,
  type TranslationDatabaseTransactionRunner,
  type TranslationDependencies,
  type TranslationDependencyProbe,
  type TranslationLanguagePolicy,
  type TranslationLanguagePolicySnapshot,
  type TranslationLanguageResultKey,
  type TranslationPersistencePublication,
  type TranslationPrompt,
  type TranslationPromptRegistry,
  type TranslationQwenClient,
  type TranslationQwenRequest,
  type TranslationStateStore,
  type TranslationStoredLanguageResult,
  type TranslationWorkHandler
} from "./dependencies.js";
import { stableUuid } from "./ids.js";
import {
  LocalTranslationQualityValidator,
  LocalTranslationWorkHandler
} from "./test-doubles.js";

const TRANSLATION_SCHEMA = "worker_uplift_translation";
const DEFAULT_PROMPT_VERSION = "0.1.0";
const DEFAULT_LANGUAGE_POLICY_VERSION = "0.1.0";
const DEFAULT_CONFIRM_TIMEOUT_MS = WORKER_DELIVERY_BEHAVIOR.confirmTimeoutMs;
const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

export type ProductionTranslationDependencies = TranslationDependencies & {
  close(): Promise<void>;
};

interface ProductionTranslationDependencyOptions {
  readonly config: TranslationConfig;
  readonly clock: RuntimeClock;
  readonly env?: NodeJS.ProcessEnv;
  readonly workHandler?: TranslationWorkHandler;
}

interface PayloadCarrier {
  readonly envelope: WorkerMessageEnvelope;
  readonly payload: Readonly<Record<string, unknown>>;
}

interface PgTranslationTransaction extends TranslationDatabaseTransaction {
  readonly client: PoolClient;
}

interface LocalAiUsage {
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly totalTokens: number;
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export function createProductionTranslationDependencies(
  options: ProductionTranslationDependencyOptions
): ProductionTranslationDependencies {
  const env = options.env ?? process.env;
  const pool = new Pool({
    connectionString: requiredEnv(env, "NUTSNEWS_TRANSLATION_DATABASE_URL"),
    max: Math.max(2, options.config.concurrency + 1),
    application_name: options.config.serviceName
  });
  const brokerTransport = new PayloadRabbitMqTransport({
    url: requiredEnv(env, "NUTSNEWS_TRANSLATION_RABBITMQ_URL"),
    prefetch: options.config.prefetch,
    clock: options.clock
  });
  const stateStore = new PostgresTranslationStateStore(pool);
  const transactionRunner = new PostgresTranslationTransactionRunner(pool);
  const brokerOutbox = new PostgresTranslationBrokerOutbox(pool);
  const qwenClient = new LocalAiTranslationQwenClient({
    baseUrl: requiredEnv(env, "NUTSNEWS_TRANSLATION_QWEN_BASE_URL"),
    apiKey: requiredEnv(env, "NUTSNEWS_TRANSLATION_QWEN_API_KEY"),
    clock: options.clock
  });

  return {
    clock: options.clock,
    stateStore,
    transactionRunner,
    brokerOutbox,
    brokerTransport,
    qwenClient,
    promptRegistry: new StaticTranslationPromptRegistry(options.config.qwen.promptId),
    languagePolicy: new StaticTranslationLanguagePolicy(options.config),
    qualityValidator: new LocalTranslationQualityValidator(),
    workHandler: options.workHandler ?? new LocalTranslationWorkHandler(),
    async close(): Promise<void> {
      await brokerTransport.close();
      await pool.end();
    }
  };
}

export class PayloadRabbitMqTransport implements RuntimeBrokerTransport {
  readonly name = "rabbitmq-payload-transport";

  private readonly url: string;
  private readonly prefetchCount: number;
  private readonly clock: RuntimeClock;
  private readonly consumers = new Map<WorkerStage, { readonly consumerTag: string; readonly handler: BrokerDeliveryHandler }>();
  private readonly inFlight = new Set<Promise<void>>();
  private connection: ChannelModel | undefined;
  private channel: ConfirmChannel | undefined;
  private routes: readonly WorkerRoute[] = [];
  private closing = false;

  constructor(options: {
    readonly url: string;
    readonly prefetch: number;
    readonly clock: RuntimeClock;
  }) {
    this.url = options.url;
    this.prefetchCount = options.prefetch;
    this.clock = options.clock;
  }

  get inFlightDeliveryCount(): number {
    return this.inFlight.size;
  }

  async connect(): Promise<void> {
    this.closing = false;
    await this.ensureChannel();
  }

  async assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.routes = routes;
    await this.ensureChannel();
  }

  async publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    const route = getWorkerRoute(command.envelope.route);
    const channel = await this.ensureChannel();

    await publishCarrierWithConfirm(channel, {
      carrier: {
        envelope: command.envelope,
        payload: command.payload
      },
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS
    });

    return {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: runtimeNow(this.clock)
    };
  }

  async consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    const route = getWorkerRoute(stage);
    const channel = await this.ensureChannel();
    await channel.prefetch(this.prefetchCount);
    const reply = await channel.consume(route.mainQueue.name, (message) => {
      if (message === null) {
        this.consumers.delete(stage);
        return;
      }

      const tracked = this.handleDelivery(stage, handler, message);
      this.inFlight.add(tracked);
      void tracked.finally(() => {
        this.inFlight.delete(tracked);
      });
    }, {
      noAck: false
    });

    this.consumers.set(stage, {
      consumerTag: reply.consumerTag,
      handler
    });

    return {
      stage,
      cancel: async (): Promise<void> => {
        this.consumers.delete(stage);
        await channel.cancel(reply.consumerTag);
      }
    };
  }

  async drain(timeoutMs = DEFAULT_DRAIN_TIMEOUT_MS): Promise<void> {
    if (this.inFlight.size === 0) {
      return;
    }

    await Promise.race([
      Promise.all([...this.inFlight]).then(() => undefined),
      new Promise<void>((_, reject) => {
        setTimeout(() => {
          reject(new Error("Timed out waiting for RabbitMQ payload deliveries to drain."));
        }, timeoutMs);
      })
    ]);
  }

  async close(): Promise<void> {
    this.closing = true;
    const channel = this.channel;

    if (channel !== undefined) {
      for (const registration of this.consumers.values()) {
        await channel.cancel(registration.consumerTag);
      }
    }

    this.consumers.clear();
    await this.drain().catch(() => undefined);

    if (this.channel !== undefined) {
      await this.channel.close().catch(() => undefined);
      this.channel = undefined;
    }

    if (this.connection !== undefined) {
      await this.connection.close().catch(() => undefined);
      this.connection = undefined;
    }
  }

  private async ensureChannel(): Promise<ConfirmChannel> {
    if (this.channel !== undefined) {
      return this.channel;
    }

    if (this.closing) {
      throw new Error("RabbitMQ payload transport is closing.");
    }

    const connection = await amqpConnect(this.url);
    const channel = await connection.createConfirmChannel();
    this.connection = connection;
    this.channel = channel;

    connection.on("close", () => {
      this.connection = undefined;
      this.channel = undefined;
    });
    channel.on("close", () => {
      this.channel = undefined;
    });

    return channel;
  }

  private async handleDelivery(
    stage: WorkerStage,
    handler: BrokerDeliveryHandler,
    message: ConsumeMessage
  ): Promise<void> {
    const channel = this.channel;
    void stage;

    if (channel === undefined) {
      return;
    }

    try {
      const carrier = decodeCarrier(message);
      const result = await handler({
        envelope: carrier.envelope,
        payload: carrier.payload,
        receivedAt: runtimeNow(this.clock)
      });
      await this.settleDelivery(channel, message, carrier, result);
    } catch {
      channel.nack(message, false, false);
    }
  }

  private async settleDelivery(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    carrier: PayloadCarrier,
    result: RuntimeMessageProcessingResult
  ): Promise<void> {
    if (result.action === "ack") {
      channel.ack(message);
      return;
    }

    if (result.action === "retry") {
      const retryEnvelope = createRetryEnvelope(result.envelope, {
        now: runtimeNow(this.clock),
        messageIdFactory: randomUuidMessageIdFactory
      });
      const retryJitterMs = computeRetryJitterMs(result.destination.ttlMs, 0.1);
      await publishCarrierWithConfirm(channel, {
        carrier: {
          envelope: retryEnvelope,
          payload: carrier.payload
        },
        exchange: getWorkerRoute(result.envelope.route).retryExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS,
        retryJitterMs
      });
      channel.ack(message);
      return;
    }

    if (result.envelope !== undefined && result.destination !== undefined) {
      await publishCarrierWithConfirm(channel, {
        carrier: {
          envelope: result.envelope,
          payload: carrier.payload
        },
        exchange: getWorkerRoute(result.envelope.route).dlqExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS
      });
      channel.ack(message);
      return;
    }

    channel.nack(message, false, false);
  }
}

export class PostgresTranslationTransactionRunner implements TranslationDatabaseTransactionRunner {
  readonly name = "postgres-translation-transactions";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<TranslationDependencyProbe> {
    return probePool(this.pool, "translation transaction database ready");
  }

  async withTransaction<T>(operation: (transaction: TranslationDatabaseTransaction) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    const transaction: PgTranslationTransaction = {
      transactionId: randomUUID(),
      client
    };

    try {
      await client.query("BEGIN");
      const value = await operation(transaction);
      await client.query("COMMIT");
      return value;
    } catch (error: unknown) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}

export class PostgresTranslationStateStore implements TranslationStateStore {
  readonly name = "postgres-translation-state";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<TranslationDependencyProbe> {
    return probePool(this.pool, "translation state database ready");
  }

  async claim(
    idempotencyKey: string,
    context: RuntimeIdempotencyClaimContext
  ): Promise<RuntimeIdempotencyClaimResult> {
    const inserted = await this.pool.query<{ readonly received_at: Date }>(
      `INSERT INTO ${TRANSLATION_SCHEMA}.inbox (
        message_id, pipeline_run_id, stage_execution_id, source_stage, source_message_id,
        entity_kind, entity_id, schema_version, operation_version, idempotency_key,
        payload_ref, payload_digest, received_at, status, diagnostic_metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, 'processing', $14::jsonb)
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING received_at`,
      [
        context.envelope.messageId,
        context.envelope.correlationId,
        context.envelope.messageId,
        context.envelope.producer.name,
        context.envelope.causationId,
        context.envelope.aggregate.type,
        context.envelope.aggregate.id,
        context.envelope.schemaVersion,
        Math.max(1, context.envelope.aggregate.version),
        idempotencyKey,
        context.envelope.payloadRef.uri,
        context.envelope.payloadRef.digest ?? sha256Json(context.envelope.payloadRef),
        context.receivedAt,
        JSON.stringify({
          route: context.envelope.route,
          attempt: context.envelope.attempt
        })
      ]
    );

    if ((inserted.rowCount ?? 0) > 0) {
      return {
        status: "claimed",
        firstSeenAt: context.receivedAt,
        replay: false
      };
    }

    const existing = await this.pool.query<{
      readonly status: string;
      readonly received_at: Date;
      readonly processed_at: Date | null;
    }>(
      `SELECT status, received_at, processed_at
       FROM ${TRANSLATION_SCHEMA}.inbox
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );
    const row = existing.rows[0];

    if (row === undefined) {
      return {
        status: "in-progress",
        firstSeenAt: context.receivedAt
      };
    }

    const firstSeenAt = row.received_at.toISOString();

    if (row.status === "processed" || row.status === "duplicate") {
      return {
        status: "already-completed",
        firstSeenAt,
        completedAt: (row.processed_at ?? row.received_at).toISOString()
      };
    }

    if (row.status === "failed" || row.status === "parked") {
      await this.pool.query(
        `UPDATE ${TRANSLATION_SCHEMA}.inbox
         SET status = 'processing',
             sanitized_error_code = NULL,
             sanitized_error_message = NULL,
             diagnostic_metadata = diagnostic_metadata || $2::jsonb
         WHERE idempotency_key = $1`,
        [
          idempotencyKey,
          JSON.stringify({
            replayedAt: context.receivedAt,
            replayMessageId: context.envelope.messageId
          })
        ]
      );

      return {
        status: "claimed",
        firstSeenAt,
        replay: true
      };
    }

    return {
      status: "in-progress",
      firstSeenAt
    };
  }

  async markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    await this.pool.query(
      `UPDATE ${TRANSLATION_SCHEMA}.inbox
       SET status = 'processed',
           processed_at = $2::timestamptz,
           diagnostic_metadata = diagnostic_metadata || $3::jsonb
       WHERE idempotency_key = $1`,
      [
        idempotencyKey,
        completion.completedAt,
        JSON.stringify({
          completedMessageId: completion.messageId,
          completedStage: completion.stage
        })
      ]
    );
  }

  async markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    await this.pool.query(
      `UPDATE ${TRANSLATION_SCHEMA}.inbox
       SET status = 'failed',
           sanitized_error_code = $2,
           sanitized_error_message = $3,
           diagnostic_metadata = diagnostic_metadata || $4::jsonb
       WHERE idempotency_key = $1`,
      [
        idempotencyKey,
        sanitizeCode(failure.reason),
        sanitizeMessage(failure.reason),
        JSON.stringify({
          failedAt: failure.failedAt,
          failedMessageId: failure.messageId,
          retryable: failure.retryable
        })
      ]
    );
  }

  async findLanguageResult(
    key: TranslationLanguageResultKey,
    transaction: TranslationDatabaseTransaction
  ): Promise<TranslationStoredLanguageResult | undefined> {
    const result = await transactionClient(transaction).query<TranslationRecordRow>(
      `SELECT article_identity_hash, language_code, translation_version, summary_ref,
              quality_status, ai_provider, ai_model, model_metadata,
              diagnostic_metadata, translated_at
       FROM ${TRANSLATION_SCHEMA}.translation_records
       WHERE article_identity_hash = $1
         AND language_code = $2
         AND translation_version = $3
         AND model_metadata->>'promptId' = $4
         AND model_metadata->>'promptVersion' = $5
         AND ai_model = $6
       LIMIT 1`,
      [
        key.articleId,
        key.targetLanguage,
        key.articleVersion,
        key.promptId,
        key.promptVersion,
        key.model
      ]
    );

    return languageResultFromRow(result.rows[0]);
  }

  async recordLanguageResult(
    result: TranslationStoredLanguageResult,
    transaction: TranslationDatabaseTransaction
  ): Promise<TranslationStoredLanguageResult> {
    await transactionClient(transaction).query(
      `INSERT INTO ${TRANSLATION_SCHEMA}.translation_records (
        article_identity_hash, language_code, translation_version, summary_ref,
        quality_status, ai_provider, ai_model, model_metadata, diagnostic_metadata,
        translated_at
      ) VALUES ($1, $2, $3, $4, $5, 'local_ai', $6, $7::jsonb, $8::jsonb, $9::timestamptz)
      ON CONFLICT (article_identity_hash, language_code, translation_version)
      DO UPDATE SET summary_ref = EXCLUDED.summary_ref,
                    quality_status = EXCLUDED.quality_status,
                    ai_provider = EXCLUDED.ai_provider,
                    ai_model = EXCLUDED.ai_model,
                    model_metadata = EXCLUDED.model_metadata,
                    diagnostic_metadata = EXCLUDED.diagnostic_metadata,
                    translated_at = EXCLUDED.translated_at`,
      [
        result.articleId,
        result.targetLanguage,
        result.articleVersion,
        result.summaryRef?.uri ?? result.qualityRef?.uri ?? `backend://worker-uplift/translation/${encodeURIComponent(result.articleId)}/${result.resultId}/failure`,
        translationQualityStatus(result.status),
        result.model,
        JSON.stringify({
          promptId: result.promptId,
          promptVersion: result.promptVersion,
          sourceLanguage: result.sourceLanguage,
          qualityScore: result.qualityRef?.qualityScore ?? 0,
          latencyMs: result.latencyMs,
          summaryRef: result.summaryRef,
          qualityRef: result.qualityRef,
          aiUsageRef: result.aiUsageRef
        }),
        JSON.stringify({
          resultId: result.resultId,
          failureReason: result.failureReason,
          sourceMessageId: result.sourceMessageId,
          correlationId: result.correlationId,
          traceparent: result.traceparent,
          persistencePublication: result.persistencePublication,
          resultSnapshot: result
        }),
        result.translatedAt
      ]
    );

    return result;
  }

  async markPersistencePublished(
    resultId: string,
    publication: TranslationPersistencePublication,
    transaction: TranslationDatabaseTransaction
  ): Promise<TranslationStoredLanguageResult> {
    const result = await transactionClient(transaction).query<TranslationRecordRow>(
      `SELECT article_identity_hash, language_code, translation_version, summary_ref,
              quality_status, ai_provider, ai_model, model_metadata,
              diagnostic_metadata, translated_at
       FROM ${TRANSLATION_SCHEMA}.translation_records
       WHERE diagnostic_metadata->>'resultId' = $1
       LIMIT 1`,
      [resultId]
    );
    const existing = languageResultFromRow(result.rows[0]);

    if (existing === undefined) {
      throw new Error(`Translation result ${resultId} is not recorded.`);
    }

    const updated = {
      ...existing,
      persistencePublication: publication
    } satisfies TranslationStoredLanguageResult;

    return this.recordLanguageResult(updated, transaction);
  }
}

export class PostgresTranslationBrokerOutbox implements TranslationBrokerOutbox {
  readonly name = "postgres-translation-outbox";

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<TranslationDependencyProbe> {
    return probePool(this.pool, "translation outbox database ready");
  }

  async record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    const payload = command.payload;

    await this.pool.query(
      `INSERT INTO ${TRANSLATION_SCHEMA}.outbox (
        outbox_message_id, pipeline_run_id, stage_execution_id, destination_stage, routing_key,
        entity_kind, entity_id, schema_version, operation_version, idempotency_key,
        payload_ref, payload_digest, published_at, confirmed_at, status, diagnostic_metadata
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, $14::timestamptz, 'confirmed', $15::jsonb)
      ON CONFLICT (idempotency_key)
      DO UPDATE SET confirmed_at = EXCLUDED.confirmed_at,
                    status = 'confirmed',
                    diagnostic_metadata = ${TRANSLATION_SCHEMA}.outbox.diagnostic_metadata || EXCLUDED.diagnostic_metadata`,
      [
        receipt.messageId,
        stringFrom(payload.pipelineRunId, command.envelope.correlationId),
        stringFrom(payload.stageExecutionId, command.envelope.messageId),
        command.envelope.route,
        receipt.routingKey,
        command.envelope.aggregate.type,
        command.envelope.aggregate.id,
        command.envelope.schemaVersion,
        Math.max(1, command.envelope.aggregate.version),
        command.envelope.idempotencyKey,
        command.envelope.payloadRef.uri,
        command.envelope.payloadRef.digest ?? sha256Json(payload),
        receipt.confirmedAt,
        receipt.confirmedAt,
        JSON.stringify({
          exchange: receipt.exchange,
          payload,
          payloadSchemaId: payload.schemaId
        })
      ]
    );
  }
}

export class LocalAiTranslationQwenClient implements TranslationQwenClient {
  readonly name = "local-ai-translation-client";

  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly clock: RuntimeClock;
  private readonly fetcher: FetchLike;

  constructor(options: {
    readonly baseUrl: string;
    readonly apiKey: string;
    readonly clock: RuntimeClock;
    readonly fetcher?: FetchLike;
  }) {
    this.baseUrl = stripTrailingSlashes(options.baseUrl);
    this.apiKey = options.apiKey;
    this.clock = options.clock;
    this.fetcher = options.fetcher ?? fetch;
  }

  async probe(): Promise<TranslationDependencyProbe> {
    try {
      const response = await this.fetcher(`${this.baseUrl}/health`, {
        method: "GET",
        signal: AbortSignal.timeout(5_000)
      });

      return response.ok
        ? {
            status: "ok",
            summary: "local AI translation endpoint ready"
          }
        : {
            status: "unhealthy",
            summary: `local AI health returned ${String(response.status)}`
          };
    } catch (error: unknown) {
      return {
        status: "unhealthy",
        summary: error instanceof Error ? error.message : "local AI health failed"
      };
    }
  }

  async translate(request: TranslationQwenRequest): Promise<unknown> {
    const apiKey = safeHeaderValue(this.apiKey);

    if (apiKey === undefined) {
      throw new TranslationQwenError("qwen-unauthorized", {
        retryable: false
      });
    }

    const startedAtMs = this.clock.now().getTime();
    let response: Response;

    try {
      response = await this.fetcher(`${this.baseUrl}/translate`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-nutsnews-ai-key": apiKey
        },
        body: JSON.stringify({
          model: request.model,
          language_code: request.input.targetLanguage,
          language_name: languageName(request.input.targetLanguage),
          source: "NutsNews worker uplift",
          title: `NutsNews shadow article ${request.input.articleId}`,
          summary: shadowSourceSummary(request.input.articleId),
          category: "Uplifting"
        }),
        signal: AbortSignal.timeout(request.timeoutMs)
      });
    } catch (error: unknown) {
      if (error instanceof Error && error.name === "TimeoutError") {
        throw new TranslationQwenError("qwen-timeout", {
          retryable: true
        });
      }

      throw new TranslationQwenError("qwen-model-error", {
        retryable: true
      });
    }

    if (!response.ok) {
      throw translationErrorFromStatus(response);
    }

    const raw = await response.json();

    return mapLocalAiTranslation(raw, Math.max(0, this.clock.now().getTime() - startedAtMs));
  }
}

export class StaticTranslationPromptRegistry implements TranslationPromptRegistry {
  readonly name = "static-translation-prompt-registry";

  constructor(private readonly promptId: string) {}

  probe(): TranslationDependencyProbe {
    return {
      status: "ok",
      summary: "static translation prompt registry ready"
    };
  }

  getPrompt(id: string): Promise<TranslationPrompt> {
    if (id !== this.promptId) {
      return Promise.reject(new Error(`Unknown translation prompt ${id}.`));
    }

    return Promise.resolve({
      id,
      version: DEFAULT_PROMPT_VERSION,
      purpose: "summary-translation",
      instructions: "Translate a NutsNews article card summary with local AI while keeping output bounded and shadow-only."
    });
  }
}

export class StaticTranslationLanguagePolicy implements TranslationLanguagePolicy {
  readonly name = "static-translation-language-policy";

  private readonly policy: TranslationLanguagePolicySnapshot;

  constructor(config: TranslationConfig) {
    this.policy = {
      policyId: config.languagePolicy.policyId,
      version: DEFAULT_LANGUAGE_POLICY_VERSION,
      requiredLanguageCodes: config.languagePolicy.targetLanguages,
      perLanguageConcurrency: config.languagePolicy.perLanguageConcurrency
    };
  }

  probe(): TranslationDependencyProbe {
    return {
      status: "ok",
      summary: "static translation language policy ready"
    };
  }

  getPolicy(): Promise<TranslationLanguagePolicySnapshot> {
    return Promise.resolve(this.policy);
  }
}

interface TranslationRecordRow extends QueryResultRow {
  readonly article_identity_hash: string;
  readonly language_code: string;
  readonly translation_version: number;
  readonly summary_ref: string;
  readonly quality_status: string;
  readonly ai_provider: string | null;
  readonly ai_model: string | null;
  readonly model_metadata: unknown;
  readonly diagnostic_metadata: unknown;
  readonly translated_at: Date;
}

async function probePool(pool: Pool, summary: string): Promise<TranslationDependencyProbe> {
  try {
    await pool.query("SELECT 1");

    return {
      status: "ok",
      summary
    };
  } catch (error: unknown) {
    return {
      status: "unhealthy",
      summary: error instanceof Error ? error.message : "database probe failed"
    };
  }
}

function transactionClient(transaction: TranslationDatabaseTransaction): PoolClient {
  const client = (transaction as Partial<PgTranslationTransaction>).client;

  if (client === undefined) {
    throw new Error("Translation operation requires a Postgres transaction.");
  }

  return client;
}

async function publishCarrierWithConfirm(
  channel: ConfirmChannel,
  options: {
    readonly carrier: PayloadCarrier;
    readonly exchange: string;
    readonly routingKey: string;
    readonly confirmTimeoutMs: number;
    readonly retryJitterMs?: number;
  }
): Promise<void> {
  const content = Buffer.from(JSON.stringify(options.carrier), "utf8");
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let settled = false;

  await new Promise<void>((resolve, reject) => {
    const cleanup = (): void => {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }

      channel.off("return", onReturn);
      channel.off("close", onClose);
      channel.off("error", onChannelError);
      settled = true;
    };
    const fail = (error: Error): void => {
      if (settled) {
        return;
      }

      cleanup();
      reject(error);
    };
    const onReturn = (returned: unknown): void => {
      if (returnedMessageId(returned) === options.carrier.envelope.messageId) {
        fail(new Error(`RabbitMQ publish was returned for ${options.exchange}:${options.routingKey}.`));
      }
    };
    const onClose = (): void => {
      fail(new Error("RabbitMQ channel closed during publish."));
    };
    const onChannelError = (): void => {
      fail(new Error("RabbitMQ channel errored during publish."));
    };

    timeout = setTimeout(() => {
      fail(new Error("RabbitMQ publish confirm timed out."));
    }, options.confirmTimeoutMs);

    channel.on("return", onReturn);
    channel.on("close", onClose);
    channel.on("error", onChannelError);
    channel.publish(
      options.exchange,
      options.routingKey,
      content,
      publishOptions(options.carrier.envelope, options.retryJitterMs),
      (error: unknown) => {
        if (error !== null && error !== undefined) {
          fail(error instanceof Error ? error : new Error("RabbitMQ publish confirm failed."));
          return;
        }

        if (!settled) {
          cleanup();
          resolve();
        }
      }
    );
  });
}

function decodeCarrier(message: ConsumeMessage): PayloadCarrier {
  const parsed = JSON.parse(message.content.toString("utf8")) as unknown;

  if (isRecord(parsed) && isRecord(parsed.envelope)) {
    return {
      envelope: parsed.envelope as unknown as WorkerMessageEnvelope,
      payload: isRecord(parsed.payload) ? parsed.payload : {}
    };
  }

  if (!isRecord(parsed)) {
    throw new Error("RabbitMQ message body must be a JSON object.");
  }

  return {
    envelope: parsed as unknown as WorkerMessageEnvelope,
    payload: {}
  };
}

function publishOptions(envelope: WorkerMessageEnvelope, retryJitterMs: number | undefined): Options.Publish {
  return {
    persistent: true,
    mandatory: true,
    contentType: WORKER_DELIVERY_BEHAVIOR.contentType,
    contentEncoding: WORKER_DELIVERY_BEHAVIOR.contentEncoding,
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    timestamp: Math.floor(Date.parse(envelope.occurredAt) / 1_000),
    headers: {
      schemaId: envelope.schemaId,
      schemaVersion: envelope.schemaVersion,
      route: envelope.route,
      attemptCount: envelope.attempt.count,
      idempotencyKey: envelope.idempotencyKey,
      payloadCarrier: "envelope-plus-payload",
      ...runtimeTraceHeadersFromEnvelope(envelope),
      ...(retryJitterMs === undefined ? {} : {
        retryJitterMs
      })
    }
  };
}

function returnedMessageId(returned: unknown): string | undefined {
  if (!isRecord(returned) || !isRecord(returned.properties)) {
    return undefined;
  }

  const messageId = returned.properties.messageId;

  return typeof messageId === "string" ? messageId : undefined;
}

function languageResultFromRow(row: TranslationRecordRow | undefined): TranslationStoredLanguageResult | undefined {
  if (row === undefined) {
    return undefined;
  }

  const diagnostic = objectValue(row.diagnostic_metadata);
  const snapshot = diagnostic.resultSnapshot;

  if (isTranslationResultSnapshot(snapshot)) {
    return snapshot;
  }

  const modelMetadata = objectValue(row.model_metadata);
  const promptId = stringFrom(modelMetadata.promptId, "summary-translation-v1");
  const promptVersion = stringFrom(modelMetadata.promptVersion, DEFAULT_PROMPT_VERSION);
  const model = row.ai_model ?? "unknown";
  const resultId = stringFrom(diagnostic.resultId, stableUuid([
    row.article_identity_hash,
    String(row.translation_version),
    stringFrom(modelMetadata.sourceLanguage, "en"),
    row.language_code,
    promptId,
    promptVersion,
    model
  ]));
  const traceparent = stringFrom(diagnostic.traceparent, "00-00000000000000000000000000000000-0000000000000000-00");
  const sourceMessageId = stringFrom(diagnostic.sourceMessageId, resultId);
  const qualityRef = isTranslationQualityRef(modelMetadata.qualityRef)
    ? modelMetadata.qualityRef
    : {
        kind: "backend-record",
        uri: `backend://worker-uplift/translation/${encodeURIComponent(row.article_identity_hash)}/${resultId}/${encodeURIComponent(row.language_code)}/quality`,
        mediaType: "application/json",
        qualityScore: numberFrom(modelMetadata.qualityScore, 0),
        resultId
      } as const;

  return {
    resultId,
    articleId: row.article_identity_hash,
    articleVersion: row.translation_version,
    sourceLanguage: stringFrom(modelMetadata.sourceLanguage, "en"),
    targetLanguage: row.language_code,
    promptId,
    promptVersion,
    model,
    status: row.quality_status === "accepted" ? "success" : "permanent_failure",
    ...(typeof diagnostic.failureReason === "string" ? {
      failureReason: diagnostic.failureReason
    } : {}),
    ...(isTranslationSummaryRef(modelMetadata.summaryRef) ? {
      summaryRef: modelMetadata.summaryRef
    } : {}),
    qualityRef,
    ...(isTranslationAiUsageRef(modelMetadata.aiUsageRef) ? {
      aiUsageRef: modelMetadata.aiUsageRef
    } : {}),
    sourceMessageId,
    correlationId: stringFrom(diagnostic.correlationId, sourceMessageId),
    traceparent,
    latencyMs: numberFrom(modelMetadata.latencyMs, 0),
    translatedAt: row.translated_at.toISOString(),
    ...(isPersistencePublication(diagnostic.persistencePublication) ? {
      persistencePublication: diagnostic.persistencePublication
    } : {})
  };
}

function mapLocalAiTranslation(raw: unknown, latencyMs: number): unknown {
  if (!isRecord(raw)) {
    return raw;
  }

  const usage = usageFromLocalAi(raw);

  return {
    summary: stringFrom(raw.summary, ""),
    qualityScore: 92,
    latencyMs: numberFrom(raw.duration_ms, latencyMs),
    ...(usage === undefined ? {} : {
      usage
    })
  };
}

function translationErrorFromStatus(response: Response): TranslationQwenError {
  if (response.status === 401 || response.status === 403) {
    return new TranslationQwenError("qwen-unauthorized", {
      retryable: false
    });
  }

  if (response.status === 408) {
    return new TranslationQwenError("qwen-timeout", {
      retryable: true
    });
  }

  if (response.status === 429) {
    const retryAfter = retryAfterMs(response);

    return new TranslationQwenError("qwen-rate-limited", retryAfter === undefined
      ? {
          retryable: true
        }
      : {
          retryable: true,
          retryAfterMs: retryAfter
        });
  }

  return new TranslationQwenError("qwen-model-error", {
    retryable: response.status >= 500
  });
}

function retryAfterMs(response: Response): number | undefined {
  const value = response.headers.get("retry-after");

  if (value === null) {
    return undefined;
  }

  const seconds = Number(value);

  return Number.isFinite(seconds) && seconds > 0 ? Math.round(seconds * 1_000) : undefined;
}

function usageFromLocalAi(raw: Readonly<Record<string, unknown>>): LocalAiUsage | undefined {
  const inputTokens = nonNegativeInteger(raw.prompt_tokens) ? raw.prompt_tokens : 0;
  const outputTokens = nonNegativeInteger(raw.completion_tokens) ? raw.completion_tokens : 0;
  const totalTokens = nonNegativeInteger(raw.total_tokens) ? raw.total_tokens : inputTokens + outputTokens;

  if (inputTokens === 0 && outputTokens === 0 && totalTokens === 0) {
    return undefined;
  }

  return {
    inputTokens,
    outputTokens,
    totalTokens
  };
}

function translationQualityStatus(status: TranslationStoredLanguageResult["status"]): "accepted" | "failed" {
  return status === "success" ? "accepted" : "failed";
}

function languageName(languageCode: string): string {
  const names = new Map<string, string>([
    ["fr", "French"],
    ["ja", "Japanese"],
    ["de-CH", "Swiss German"],
    ["de", "German"],
    ["el", "Greek"]
  ]);

  return names.get(languageCode) ?? languageCode;
}

function shadowSourceSummary(articleId: string): string {
  return `Approved NutsNews article ${articleId} is running through shadow translation validation. This bounded summary proves local AI, queue, and stage-state behavior without publishing reader-facing content.`;
}

function sha256Json(value: unknown): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

function requiredEnv(env: NodeJS.ProcessEnv, key: string): string {
  const value = env[key]?.trim();

  if (value === undefined || value.length === 0) {
    throw new Error(`${key} is required for production translation dependencies.`);
  }

  return value;
}

function safeHeaderValue(value: string): string | undefined {
  const trimmed = value.trim();

  if (trimmed.length === 0 || /[\r\n]/u.test(trimmed)) {
    return undefined;
  }

  return trimmed;
}

function sanitizeCode(value: string): string {
  return boundedReasonCode(value, 80) || "translation-error";
}

function sanitizeMessage(value: string): string {
  return value.replace(/[\r\n\t]+/gu, " ").slice(0, 512);
}

function numberFrom(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function stringFrom(value: unknown, fallback: string): string {
  return typeof value === "string" && value.trim().length > 0 ? value : fallback;
}

function objectValue(value: unknown): Readonly<Record<string, unknown>> {
  return isRecord(value) ? value : {};
}

function nonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function stripTrailingSlashes(value: string): string {
  let end = value.length;

  while (end > 0 && value.charCodeAt(end - 1) === 47) {
    end -= 1;
  }

  return value.slice(0, end);
}

function boundedReasonCode(value: string, maxLength: number): string {
  let output = "";

  for (const character of value.trim().toLowerCase()) {
    if (output.length >= maxLength) {
      break;
    }

    const codePoint = character.codePointAt(0);

    if (codePoint === undefined) {
      continue;
    }

    if (isLowerAsciiLetter(codePoint) || isAsciiDigit(codePoint) || character === "_" || character === "-") {
      output += character;
      continue;
    }

    if (output.length > 0 && !output.endsWith("-")) {
      output += "-";
    }
  }

  while (output.startsWith("-")) {
    output = output.slice(1);
  }

  while (output.endsWith("-")) {
    output = output.slice(0, -1);
  }

  return output;
}

function isLowerAsciiLetter(codePoint: number): boolean {
  return codePoint >= 97 && codePoint <= 122;
}

function isAsciiDigit(codePoint: number): boolean {
  return codePoint >= 48 && codePoint <= 57;
}

function isTranslationResultSnapshot(value: unknown): value is TranslationStoredLanguageResult {
  return isRecord(value)
    && typeof value.resultId === "string"
    && typeof value.articleId === "string"
    && typeof value.articleVersion === "number"
    && typeof value.sourceLanguage === "string"
    && typeof value.targetLanguage === "string"
    && typeof value.status === "string"
    && typeof value.model === "string"
    && typeof value.promptId === "string"
    && typeof value.promptVersion === "string";
}

function isPersistencePublication(value: unknown): value is TranslationPersistencePublication {
  return isRecord(value)
    && typeof value.messageId === "string"
    && typeof value.idempotencyKey === "string"
    && typeof value.publishedAt === "string";
}

function isTranslationSummaryRef(value: unknown): value is NonNullable<TranslationStoredLanguageResult["summaryRef"]> {
  return isRecord(value)
    && value.kind === "backend-record"
    && typeof value.uri === "string"
    && value.mediaType === "application/json"
    && typeof value.articleId === "string"
    && typeof value.targetLanguage === "string"
    && typeof value.resultId === "string";
}

function isTranslationQualityRef(value: unknown): value is NonNullable<TranslationStoredLanguageResult["qualityRef"]> {
  return isRecord(value)
    && value.kind === "backend-record"
    && typeof value.uri === "string"
    && value.mediaType === "application/json"
    && typeof value.qualityScore === "number"
    && typeof value.resultId === "string";
}

function isTranslationAiUsageRef(value: unknown): value is NonNullable<TranslationStoredLanguageResult["aiUsageRef"]> {
  return isRecord(value)
    && value.kind === "backend-record"
    && typeof value.uri === "string"
    && value.mediaType === "application/json"
    && typeof value.inputTokens === "number"
    && typeof value.outputTokens === "number"
    && typeof value.totalTokens === "number";
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
