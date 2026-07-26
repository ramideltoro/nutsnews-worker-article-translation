import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";

import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  validateStagePayload,
  validateWorkerEnvelope,
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
  TRANSLATION_RECONCILIATION_CONFIRMATION,
  type TranslationReconciliationCandidate,
  type TranslationReconciliationReport,
  type TranslationReconciliationRequest,
  type TranslationReconciler
} from "./reconciliation.js";
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
  readonly reconciler: TranslationReconciler;
  readonly reconciliationToken?: string;
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

interface PayloadConsumerRegistration {
  readonly handler: BrokerDeliveryHandler;
  consumerTag: string | undefined;
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
type RabbitMqConnect = (url: string) => Promise<ChannelModel>;

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
  const reconciler = new PostgresTranslationOutboxReconciler({
    pool,
    brokerTransport,
    clock: options.clock,
    env,
    config: options.config
  });
  const reconciliationToken = reconciliationTokenFromEnv(env);
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
    reconciler,
    ...(reconciliationToken === undefined ? {} : {
      reconciliationToken
    }),
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
  private readonly connectToBroker: RabbitMqConnect;
  private readonly consumers = new Map<WorkerStage, PayloadConsumerRegistration>();
  private readonly inFlight = new Set<Promise<void>>();
  private connection: ChannelModel | undefined;
  private channel: ConfirmChannel | undefined;
  private routes: readonly WorkerRoute[] = [];
  private closing = false;
  private reconnecting: Promise<void> | undefined;
  private reconnectRetry: ReturnType<typeof setTimeout> | undefined;

  constructor(options: {
    readonly url: string;
    readonly prefetch: number;
    readonly clock: RuntimeClock;
    readonly connect?: RabbitMqConnect;
  }) {
    this.url = options.url;
    this.prefetchCount = options.prefetch;
    this.clock = options.clock;
    this.connectToBroker = options.connect ?? amqpConnect;
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
    const channel = await this.ensureChannel();
    const existing = this.consumers.get(stage);

    if (existing?.consumerTag !== undefined) {
      await channel.cancel(existing.consumerTag).catch(() => undefined);
    }

    const registration: PayloadConsumerRegistration = {
      handler,
      consumerTag: undefined
    };
    this.consumers.set(stage, registration);
    await this.activateConsumer(stage, registration, channel);

    return {
      stage,
      cancel: async (): Promise<void> => {
        const registered = this.consumers.get(stage);
        this.consumers.delete(stage);

        if (registered?.consumerTag !== undefined && this.channel !== undefined) {
          await this.channel.cancel(registered.consumerTag).catch(() => undefined);
        }
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
    this.clearReconnectRetry();
    const channel = this.channel;

    if (channel !== undefined) {
      for (const registration of this.consumers.values()) {
        if (registration.consumerTag !== undefined) {
          await channel.cancel(registration.consumerTag).catch(() => undefined);
        }
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

    const connection = await this.connectToBroker(this.url);
    const channel = await connection.createConfirmChannel();
    this.connection = connection;
    this.channel = channel;
    this.clearReconnectRetry();

    connection.on("close", () => {
      if (this.connection === connection) {
        this.connection = undefined;
      }

      this.markChannelClosed(channel);
    });
    channel.on("close", () => {
      this.markChannelClosed(channel);
    });

    await this.restoreConsumers(channel);

    return channel;
  }

  private async restoreConsumers(channel: ConfirmChannel): Promise<void> {
    for (const [stage, registration] of this.consumers) {
      await this.activateConsumer(stage, registration, channel);
    }
  }

  private async activateConsumer(
    stage: WorkerStage,
    registration: PayloadConsumerRegistration,
    channel: ConfirmChannel
  ): Promise<void> {
    if (registration.consumerTag !== undefined) {
      return;
    }

    const route = getWorkerRoute(stage);
    await channel.prefetch(this.prefetchCount);
    const reply = await channel.consume(route.mainQueue.name, (message) => {
      if (message === null) {
        registration.consumerTag = undefined;
        this.recoverCancelledConsumer(stage, registration, channel);
        return;
      }

      const tracked = this.handleDelivery(stage, registration.handler, message);
      this.inFlight.add(tracked);
      void tracked.finally(() => {
        this.inFlight.delete(tracked);
      });
    }, {
      noAck: false
    });

    registration.consumerTag = reply.consumerTag;
  }

  private markChannelClosed(channel: ConfirmChannel): void {
    if (this.channel !== channel) {
      return;
    }

    this.channel = undefined;

    for (const registration of this.consumers.values()) {
      registration.consumerTag = undefined;
    }

    this.recoverConsumersAfterDisconnect();
  }

  private recoverCancelledConsumer(
    stage: WorkerStage,
    registration: PayloadConsumerRegistration,
    channel: ConfirmChannel
  ): void {
    if (this.closing || this.channel !== channel || this.consumers.get(stage) !== registration) {
      return;
    }

    void this.activateConsumer(stage, registration, channel).catch(() => {
      this.markChannelClosed(channel);
    });
  }

  private recoverConsumersAfterDisconnect(): void {
    if (this.closing || this.consumers.size === 0 || this.reconnecting !== undefined) {
      return;
    }

    this.reconnecting = this.ensureChannel()
      .then(() => undefined)
      .catch(() => {
        this.scheduleReconnectRetry();
      })
      .finally(() => {
        this.reconnecting = undefined;
      });
  }

  private scheduleReconnectRetry(): void {
    if (this.closing || this.consumers.size === 0 || this.reconnectRetry !== undefined) {
      return;
    }

    this.reconnectRetry = setTimeout(() => {
      this.reconnectRetry = undefined;
      this.recoverConsumersAfterDisconnect();
    }, 1_000);
    this.reconnectRetry.unref();
  }

  private clearReconnectRetry(): void {
    if (this.reconnectRetry === undefined) {
      return;
    }

    clearTimeout(this.reconnectRetry);
    this.reconnectRetry = undefined;
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
          envelope: command.envelope,
          exchange: receipt.exchange,
          payload,
          payloadSchemaId: payload.schemaId
        })
      ]
    );
  }
}

interface TranslationOutboxReconcilerOptions {
  readonly pool: Pool;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly clock: RuntimeClock;
  readonly env: NodeJS.ProcessEnv;
  readonly config: TranslationConfig;
}

interface TranslationOutboxRow extends QueryResultRow {
  readonly id: string | number;
  readonly outbox_message_id: string;
  readonly pipeline_run_id: string;
  readonly stage_execution_id: string;
  readonly destination_stage: string;
  readonly routing_key: string;
  readonly entity_kind: string;
  readonly entity_id: string;
  readonly schema_version: number;
  readonly operation_version: number;
  readonly idempotency_key: string;
  readonly payload_ref: string;
  readonly payload_digest: string;
  readonly created_at: Date;
  readonly published_at: Date | null;
  readonly confirmed_at: Date | null;
  readonly status: string;
  readonly diagnostic_metadata: unknown;
}

interface HydratedReplay {
  readonly row: TranslationOutboxRow;
  readonly candidate: TranslationReconciliationCandidate;
  readonly command: BrokerPublishCommand;
}

interface TranslationResultSnapshotRow extends QueryResultRow {
  readonly result_snapshot: unknown;
}

type RecoveryResult = {
  readonly status: "not_applicable";
} | {
  readonly status: "failed";
  readonly reason: string;
} | {
  readonly status: "recovered";
  readonly payload: Readonly<Record<string, unknown>>;
  readonly envelope: WorkerMessageEnvelope;
};

export class PostgresTranslationOutboxReconciler implements TranslationReconciler {
  readonly name = "postgres-translation-outbox-reconciler";

  constructor(private readonly options: TranslationOutboxReconcilerOptions) {}

  async reconcile(request: TranslationReconciliationRequest): Promise<TranslationReconciliationReport> {
    const requestedAt = runtimeNow(this.options.clock);
    const mode = request.mode === "apply" ? "apply" : "dry-run";
    const maxItems = boundedInteger(request.maxItems, 100, 1, 100);
    const minAgeSeconds = boundedInteger(request.minAgeSeconds, 900, 0, 86_400);
    const reason = safeReason(request.reason);
    const runId = safeRunId(request.runId);

    if (this.killSwitchActive()) {
      return report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "kill_switch_active",
        errors: [
          "translation reconciliation stop switch is active"
        ],
        candidates: []
      });
    }

    if (mode === "apply") {
      const applyError = this.applyGateError(request, runId);

      if (applyError !== undefined) {
        return report({
          mode,
          requestedAt,
          runId,
          reason,
          maxItems,
          minAgeSeconds,
          status: "failed_closed",
          errors: [
            applyError
          ],
          candidates: []
        });
      }
    }

    const rows = await this.selectCandidates(maxItems, minAgeSeconds, runId);
    const hydrated = await Promise.all(rows.map((row) => this.hydrate(row, requestedAt)));
    const failed = hydrated.filter((candidate): candidate is TranslationReconciliationCandidate => "status" in candidate);

    if (failed.length > 0) {
      return report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "failed_closed",
        errors: failed.map((candidate) => `${candidate.outboxId}:${candidate.failedClosedReason ?? "unrecoverable-payload"}`),
        candidates: failed
      });
    }

    const replayable = hydrated as HydratedReplay[];

    if (mode === "dry-run") {
      const candidates = replayable.map((item) => item.candidate);

      return report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: "dry_run",
        candidates,
        errors: []
      });
    }

    const replayed: TranslationReconciliationCandidate[] = [];

    for (const item of replayable) {
      if (this.killSwitchActive()) {
        return report({
          mode,
          requestedAt,
          runId,
          reason,
          maxItems,
          minAgeSeconds,
          status: "kill_switch_active",
          candidates: replayed,
          errors: [
            "translation reconciliation stop switch became active"
          ]
        });
      }

      const receipt = await this.options.brokerTransport.publish(item.command);
      await this.recordReplay(item.row, item.command, receipt, requestedAt, runId ?? "untracked", reason);
      replayed.push({
        ...item.candidate,
        status: "replayed",
        replayMessageId: receipt.messageId
      });
    }

    return report({
      mode,
      requestedAt,
      runId,
      reason,
      maxItems,
      minAgeSeconds,
      status: "applied",
      candidates: replayed,
      errors: []
    });
  }

  private async selectCandidates(maxItems: number, minAgeSeconds: number, runId: string | undefined): Promise<readonly TranslationOutboxRow[]> {
    const result = await this.options.pool.query<TranslationOutboxRow>(
      `SELECT id, outbox_message_id, pipeline_run_id, stage_execution_id, destination_stage, routing_key,
              entity_kind, entity_id, schema_version, operation_version, idempotency_key,
              payload_ref, payload_digest, created_at, published_at, confirmed_at, status, diagnostic_metadata
       FROM ${TRANSLATION_SCHEMA}.outbox
       WHERE status = 'confirmed'
         AND confirmed_at IS NOT NULL
         AND created_at <= now() - ($2::integer * interval '1 second')
         AND ($3::text IS NULL OR diagnostic_metadata->>'lastReconciliationRunId' IS DISTINCT FROM $3::text)
       ORDER BY created_at ASC, id ASC
       LIMIT $1`,
      [
        maxItems,
        minAgeSeconds,
        runId ?? null
      ]
    );

    return result.rows;
  }

  private async hydrate(row: TranslationOutboxRow, requestedAt: string): Promise<HydratedReplay | TranslationReconciliationCandidate> {
    const baseCandidate = candidateFromRow(row, "confirmed-outbox-replay");
    const diagnostic = objectValue(row.diagnostic_metadata);
    const recovered = await this.recoverMissingCarrier(row, diagnostic);

    if (recovered.status === "failed") {
      return failedCandidate(baseCandidate, recovered.reason);
    }

    const payload = recovered.status === "recovered" ? recovered.payload : diagnostic.payload;
    const envelope = recovered.status === "recovered" ? recovered.envelope : diagnostic.envelope;

    if (!isRecord(payload)) {
      return failedCandidate(baseCandidate, "missing-stored-payload");
    }

    if (!isRecord(envelope)) {
      return failedCandidate(baseCandidate, "missing-stored-envelope");
    }

    if (row.payload_ref !== stringFrom(objectValue(envelope.payloadRef).uri, "")) {
      return failedCandidate(baseCandidate, "payload-ref-mismatch");
    }

    if (row.payload_digest !== sha256Json(payload)) {
      return failedCandidate(baseCandidate, "payload-digest-mismatch");
    }

    const payloadValidation = validateStagePayload(payload);

    if (!payloadValidation.ok) {
      return failedCandidate(baseCandidate, `invalid-stored-payload:${payloadValidation.issues[0]?.code ?? "unknown"}`);
    }

    const replayMessageId = randomUUID();
    const attempt = objectValue(envelope.attempt);
    const replayEnvelope = {
      ...envelope,
      messageId: replayMessageId,
      occurredAt: requestedAt,
      attempt: {
        ...attempt,
        lastAttemptAt: requestedAt
      }
    };
    const envelopeValidation = validateWorkerEnvelope(replayEnvelope);

    if (!envelopeValidation.ok) {
      return failedCandidate(baseCandidate, `invalid-stored-envelope:${envelopeValidation.issues[0]?.code ?? "unknown"}`);
    }

    if (envelopeValidation.value.route !== row.destination_stage) {
      return failedCandidate(baseCandidate, "destination-stage-mismatch");
    }

    return {
      row,
      candidate: {
        ...baseCandidate,
        replayMessageId
      },
      command: {
        envelope: envelopeValidation.value,
        payload
      }
    };
  }

  private async recoverMissingCarrier(
    row: TranslationOutboxRow,
    diagnostic: Readonly<Record<string, unknown>>
  ): Promise<RecoveryResult> {
    if (isRecord(diagnostic.envelope)) {
      return {
        status: "not_applicable"
      };
    }

    if (!isRecord(diagnostic.payload)) {
      return {
        status: "not_applicable"
      };
    }

    if (row.destination_stage !== "persistence") {
      return {
        status: "not_applicable"
      };
    }

    if (row.idempotency_key.startsWith("translation:persistence:")) {
      return this.recoverPersistenceCarrier(row, diagnostic.payload);
    }

    if (row.idempotency_key.startsWith("translation:result:")) {
      return this.recoverTranslationStatusCarrier(row, diagnostic.payload);
    }

    return {
      status: "not_applicable"
    };
  }

  private async recoverPersistenceCarrier(
    row: TranslationOutboxRow,
    diagnosticPayload: Readonly<Record<string, unknown>>
  ): Promise<RecoveryResult> {
    const resultId = row.idempotency_key.slice("translation:persistence:".length);

    if (resultId.length === 0) {
      return {
        status: "failed",
        reason: "invalid-legacy-result-id"
      };
    }

    const result = await this.findReplayableResultSnapshotById(resultId);

    if (result === undefined) {
      return {
        status: "failed",
        reason: "missing-result-snapshot"
      };
    }

    if (result.status !== "success") {
      return {
        status: "failed",
        reason: "non-success-result-not-persistence-replayable"
      };
    }

    const payload = this.persistencePayloadFromResult(row, result, diagnosticPayload);

    if (payload === undefined) {
      return {
        status: "failed",
        reason: "legacy-persistence-metadata-mismatch"
      };
    }

    if (row.payload_digest !== sha256Json(payload)) {
      return {
        status: "failed",
        reason: "payload-digest-mismatch"
      };
    }

    const envelope = this.envelopeForRecoveredPersistencePayload(row, payload, result);

    if (envelope === undefined) {
      return {
        status: "failed",
        reason: "legacy-envelope-metadata-mismatch"
      };
    }

    return {
      status: "recovered",
      payload,
      envelope
    };
  }

  private async recoverTranslationStatusCarrier(
    row: TranslationOutboxRow,
    diagnosticPayload: Readonly<Record<string, unknown>>
  ): Promise<RecoveryResult> {
    const payloadArticleId = stringFrom(diagnosticPayload.articleId, "");
    const payloadVersion = row.operation_version;

    if (payloadArticleId.length === 0 || payloadArticleId !== row.entity_id || !Number.isInteger(payloadVersion) || payloadVersion < 1) {
      return {
        status: "failed",
        reason: "legacy-status-article-metadata-mismatch"
      };
    }

    const results = await this.findReplayableResultSnapshotsForArticle(payloadArticleId, payloadVersion);

    if (results.length === 0) {
      return {
        status: "failed",
        reason: "missing-result-snapshot"
      };
    }

    const payload = this.translationStatusPayloadFromResults(row, diagnosticPayload, results);

    if (payload === undefined) {
      return {
        status: "failed",
        reason: "legacy-status-metadata-mismatch"
      };
    }

    if (row.payload_digest !== sha256Json(payload)) {
      return {
        status: "failed",
        reason: "payload-digest-mismatch"
      };
    }

    const envelope = this.envelopeForRecoveredStatusPayload(row, payload, results);

    if (envelope === undefined) {
      return {
        status: "failed",
        reason: "legacy-envelope-metadata-mismatch"
      };
    }

    return {
      status: "recovered",
      payload,
      envelope
    };
  }

  private async findReplayableResultSnapshotById(resultId: string): Promise<TranslationStoredLanguageResult | undefined> {
    const result = await this.options.pool.query<TranslationResultSnapshotRow>(
      `SELECT diagnostic_metadata->'resultSnapshot' AS result_snapshot
       FROM ${TRANSLATION_SCHEMA}.translation_records
       WHERE diagnostic_metadata->>'resultId' = $1
       ORDER BY translated_at ASC, id ASC
       LIMIT 2`,
      [
        resultId
      ]
    );

    if (result.rows.length !== 1) {
      return undefined;
    }

    const snapshot = result.rows[0]?.result_snapshot;

    return isReplayableTranslationResultSnapshot(snapshot) ? snapshot : undefined;
  }

  private async findReplayableResultSnapshotsForArticle(articleId: string, articleVersion: number): Promise<readonly TranslationStoredLanguageResult[]> {
    const result = await this.options.pool.query<TranslationResultSnapshotRow>(
      `SELECT diagnostic_metadata->'resultSnapshot' AS result_snapshot
       FROM ${TRANSLATION_SCHEMA}.translation_records
       WHERE article_identity_hash = $1
         AND translation_version = $2
       ORDER BY translated_at ASC, id ASC`,
      [
        articleId,
        articleVersion
      ]
    );

    const snapshots = result.rows.map((row) => row.result_snapshot);

    return snapshots.every(isReplayableTranslationResultSnapshot) ? snapshots : [];
  }

  private persistencePayloadFromResult(
    row: TranslationOutboxRow,
    result: TranslationStoredLanguageResult,
    diagnosticPayload: Readonly<Record<string, unknown>>
  ): Readonly<Record<string, unknown>> | undefined {
    const payloadRef = `backend://worker-uplift/translation/${encodeURIComponent(result.articleId)}/${encodeURIComponent(result.resultId)}`;
    const stageExecutionId = stableUuid([
      "persistence-command",
      result.resultId
    ]);

    if (row.payload_ref !== payloadRef
      || row.stage_execution_id !== stageExecutionId
      || row.entity_kind !== "article"
      || row.entity_id !== result.articleId
      || row.operation_version !== result.articleVersion
      || row.idempotency_key !== `translation:persistence:${result.resultId}`
      || result.model !== this.options.config.qwen.model
      || result.promptId !== this.options.config.qwen.promptId
      || result.summaryRef === undefined
      || result.qualityRef === undefined) {
      return undefined;
    }

    const entityRef = {
      articleId: result.articleId,
      articleVersion: result.articleVersion,
      sourceLanguage: result.sourceLanguage,
      targetLanguage: result.targetLanguage,
      summaryRef: orderedSummaryRef(result.summaryRef),
      qualityRef: orderedQualityRef(result.qualityRef),
      ...(result.aiUsageRef === undefined ? {} : {
        aiUsageRef: orderedAiUsageRef(result.aiUsageRef)
      })
    };
    const tracestate = optionalString(diagnosticPayload.tracestate);
    const payload = {
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand,
      schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
      pipelineRunId: row.pipeline_run_id,
      stageExecutionId,
      sourceMessageId: result.sourceMessageId,
      idempotencyKey: row.idempotency_key,
      traceparent: result.traceparent,
      ...(tracestate === undefined ? {} : {
        tracestate
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

    return validateStagePayload(payload).ok ? payload : undefined;
  }

  private translationStatusPayloadFromResults(
    row: TranslationOutboxRow,
    diagnosticPayload: Readonly<Record<string, unknown>>,
    results: readonly TranslationStoredLanguageResult[]
  ): Readonly<Record<string, unknown>> | undefined {
    const articleId = stringFrom(diagnosticPayload.articleId, "");
    const producedAt = optionalString(diagnosticPayload.producedAt);
    const sourceMessageId = optionalString(diagnosticPayload.sourceMessageId);
    const traceparent = optionalString(diagnosticPayload.traceparent);
    const tracestate = optionalString(diagnosticPayload.tracestate);
    const completedLanguageCodes = stringArray(diagnosticPayload.completedLanguageCodes);
    const missingLanguageCodes = stringArray(diagnosticPayload.missingLanguageCodes);
    const status = optionalString(diagnosticPayload.translationStatus);

    if (producedAt === undefined
      || sourceMessageId === undefined
      || traceparent === undefined
      || completedLanguageCodes === undefined
      || missingLanguageCodes === undefined
      || status === undefined
      || articleId !== row.entity_id
      || row.entity_kind !== "article"
      || row.payload_ref !== `backend://worker-uplift/translation/${encodeURIComponent(articleId)}/translation-status`
      || row.stage_execution_id !== stableUuid([
        "translation-status",
        articleId,
        String(row.operation_version),
        this.options.config.qwen.model
      ])) {
      return undefined;
    }

    const allLanguageCodes = [
      ...completedLanguageCodes,
      ...missingLanguageCodes
    ];

    if (new Set(allLanguageCodes).size !== allLanguageCodes.length
      || allLanguageCodes.some((language) => !this.options.config.languagePolicy.targetLanguages.includes(language))) {
      return undefined;
    }

    const matchingResults = results.filter((result) => allLanguageCodes.includes(result.targetLanguage));
    const successfulLanguages = results
      .filter((result) => this.options.config.languagePolicy.targetLanguages.includes(result.targetLanguage))
      .filter((result) => result.status === "success")
      .map((result) => result.targetLanguage);

    if (!sameStringSet(successfulLanguages, completedLanguageCodes)
      || !matchingResults.every((result) => result.articleId === articleId
        && result.articleVersion === row.operation_version
        && result.traceparent === traceparent
        && result.model === this.options.config.qwen.model
        && result.promptId === this.options.config.qwen.promptId)) {
      return undefined;
    }

    const expectedStatus = missingLanguageCodes.length === 0
      ? "complete"
      : matchingResults.some((result) => missingLanguageCodes.includes(result.targetLanguage) && result.status === "permanent_failure")
        ? "permanent_failure"
        : "partial";

    if (status !== expectedStatus) {
      return undefined;
    }

    const summaryRefs: NonNullable<TranslationStoredLanguageResult["summaryRef"]>[] = [];

    for (const language of completedLanguageCodes) {
      const result = matchingResults.find((item) => item.targetLanguage === language);

      if (result?.summaryRef === undefined) {
        return undefined;
      }

      summaryRefs.push(orderedSummaryRef(result.summaryRef));
    }

    const payload = this.translationStatusPayload(row, {
      articleId,
      producedAt,
      sourceMessageId,
      traceparent,
      ...(tracestate === undefined ? {} : {
        tracestate
      }),
      status,
      completedLanguageCodes,
      missingLanguageCodes,
      summaryRefs
    });

    if (row.payload_digest === sha256Json(payload)) {
      return validateStagePayload(payload).ok ? payload : undefined;
    }

    const diagnosticSummaryRefs = summaryRefArray(diagnosticPayload.summaryRefs);

    if (diagnosticSummaryRefs === undefined || !sameSummaryRefs(summaryRefs, diagnosticSummaryRefs)) {
      return validateStagePayload(payload).ok ? payload : undefined;
    }

    const diagnosticPayloadCandidate = this.translationStatusPayload(row, {
      articleId,
      producedAt,
      sourceMessageId,
      traceparent,
      ...(tracestate === undefined ? {} : {
        tracestate
      }),
      status,
      completedLanguageCodes,
      missingLanguageCodes,
      summaryRefs: diagnosticSummaryRefs
    });

    return validateStagePayload(diagnosticPayloadCandidate).ok ? diagnosticPayloadCandidate : undefined;
  }

  private translationStatusPayload(
    row: TranslationOutboxRow,
    input: {
      readonly articleId: string;
      readonly producedAt: string;
      readonly sourceMessageId: string;
      readonly traceparent: string;
      readonly tracestate?: string;
      readonly status: string;
      readonly completedLanguageCodes: readonly string[];
      readonly missingLanguageCodes: readonly string[];
      readonly summaryRefs: readonly NonNullable<TranslationStoredLanguageResult["summaryRef"]>[];
    }
  ): Readonly<Record<string, unknown>> {
    return {
      schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationResult,
      schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
      pipelineRunId: row.pipeline_run_id,
      stageExecutionId: row.stage_execution_id,
      sourceMessageId: input.sourceMessageId,
      idempotencyKey: row.idempotency_key,
      traceparent: input.traceparent,
      ...(input.tracestate === undefined ? {} : {
        tracestate: input.tracestate
      }),
      producedAt: input.producedAt,
      articleId: input.articleId,
      translationStatus: input.status,
      completedLanguageCodes: input.completedLanguageCodes,
      missingLanguageCodes: input.missingLanguageCodes,
      summaryRefs: input.summaryRefs
    };
  }

  private envelopeForRecoveredPersistencePayload(
    row: TranslationOutboxRow,
    payload: Readonly<Record<string, unknown>>,
    result: TranslationStoredLanguageResult
  ): WorkerMessageEnvelope | undefined {
    const tracestate = optionalString(payload.tracestate);

    return this.envelopeForRecoveredPayload(row, payload, {
      causationId: result.sourceMessageId,
      correlationId: result.correlationId,
      traceparent: result.traceparent,
      ...(tracestate === undefined ? {} : {
        tracestate
      }),
      occurredAt: result.translatedAt
    });
  }

  private envelopeForRecoveredStatusPayload(
    row: TranslationOutboxRow,
    payload: Readonly<Record<string, unknown>>,
    results: readonly TranslationStoredLanguageResult[]
  ): WorkerMessageEnvelope | undefined {
    const sourceMessageId = optionalString(payload.sourceMessageId);
    const traceparent = optionalString(payload.traceparent);
    const correlations = new Set(results.map((result) => result.correlationId));

    if (sourceMessageId === undefined || traceparent === undefined || correlations.size !== 1) {
      return undefined;
    }

    const correlationId = results[0]?.correlationId;

    if (correlationId === undefined) {
      return undefined;
    }

    const tracestate = optionalString(payload.tracestate);

    return this.envelopeForRecoveredPayload(row, payload, {
      causationId: sourceMessageId,
      correlationId,
      traceparent,
      ...(tracestate === undefined ? {} : {
        tracestate
      }),
      occurredAt: optionalString(payload.producedAt) ?? row.confirmed_at?.toISOString() ?? row.created_at.toISOString()
    });
  }

  private envelopeForRecoveredPayload(
    row: TranslationOutboxRow,
    payload: Readonly<Record<string, unknown>>,
    metadata: {
      readonly causationId: string;
      readonly correlationId: string;
      readonly traceparent: string;
      readonly tracestate?: string;
      readonly occurredAt: string;
    }
  ): WorkerMessageEnvelope | undefined {
    const route = getWorkerRoute("persistence");
    const envelope = {
      schemaId: route.schemaId,
      schemaVersion: row.schema_version,
      route: "persistence",
      messageId: row.outbox_message_id,
      causationId: metadata.causationId,
      correlationId: metadata.correlationId,
      traceparent: metadata.traceparent,
      ...(metadata.tracestate === undefined ? {} : {
        tracestate: metadata.tracestate
      }),
      idempotencyKey: row.idempotency_key,
      aggregate: {
        type: row.entity_kind,
        id: row.entity_id,
        version: row.operation_version
      },
      occurredAt: metadata.occurredAt,
      attempt: {
        count: 1,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: metadata.occurredAt
      },
      producer: {
        name: this.options.config.serviceName,
        version: this.options.config.serviceVersion
      },
      payloadRef: {
        kind: "backend-record",
        uri: row.payload_ref,
        mediaType: "application/json",
        sizeBytes: getStagePayloadSizeBytes(payload)
      }
    };
    const validation = validateWorkerEnvelope(envelope);

    return validation.ok && validation.value.route === row.destination_stage ? validation.value : undefined;
  }

  private async recordReplay(
    row: TranslationOutboxRow,
    command: BrokerPublishCommand,
    receipt: BrokerPublishReceipt,
    requestedAt: string,
    runId: string,
    reason: string | undefined
  ): Promise<void> {
    const audit = {
      event: "translation.outbox.replayed",
      runId,
      reason: reason ?? "unspecified",
      originalMessageId: row.outbox_message_id,
      replayMessageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      correlationId: command.envelope.correlationId,
      causationId: command.envelope.causationId,
      articleId: command.envelope.aggregate.id,
      articleVersion: command.envelope.aggregate.version,
      replayedAt: requestedAt,
      exchange: receipt.exchange,
      routingKey: receipt.routingKey
    };

    await this.options.pool.query(
      `UPDATE ${TRANSLATION_SCHEMA}.outbox
       SET diagnostic_metadata =
         jsonb_set(
           diagnostic_metadata || $2::jsonb,
           '{reconciliationAuditHistory}',
           coalesce(diagnostic_metadata->'reconciliationAuditHistory', '[]'::jsonb) || $3::jsonb,
           true
         )
       WHERE id = $1`,
      [
        row.id,
        JSON.stringify({
          lastReconciliationRunId: runId,
          reconciliationLastReplayAt: requestedAt,
          reconciliationLastReplayMessageId: command.envelope.messageId
        }),
        JSON.stringify([
          audit
        ])
      ]
    );
  }

  private applyGateError(request: TranslationReconciliationRequest, runId: string | undefined): string | undefined {
    if (!this.applyEnabled()) {
      return "translation reconciliation apply is disabled by configuration";
    }

    if (request.protectedConfirmation !== TRANSLATION_RECONCILIATION_CONFIRMATION) {
      return `protectedConfirmation must be ${TRANSLATION_RECONCILIATION_CONFIRMATION}`;
    }

    if (runId === undefined) {
      return "runId is required for apply";
    }

    return undefined;
  }

  private applyEnabled(): boolean {
    return flagEnabled(this.options.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_APPLY_ENABLED)
      || flagEnabled(this.options.env.NUTSNEWS_TRANSLATION_RECONCILIATION_APPLY_ENABLED);
  }

  private killSwitchActive(): boolean {
    return flagEnabled(this.options.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_STOP)
      || flagEnabled(this.options.env.NUTSNEWS_TRANSLATION_RECONCILIATION_STOP);
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

function reconciliationTokenFromEnv(env: NodeJS.ProcessEnv): string | undefined {
  const directToken = optionalEnv(env, "NUTSNEWS_TRANSLATION_RECONCILIATION_TOKEN")
    ?? optionalEnv(env, "NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN");

  if (directToken !== undefined) {
    return directToken;
  }

  const tokenFile = optionalEnv(env, "NUTSNEWS_TRANSLATION_RECONCILIATION_TOKEN_FILE")
    ?? optionalEnv(env, "NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN_FILE");

  if (tokenFile === undefined) {
    return undefined;
  }

  const value = readFileSync(tokenFile, "utf8").trim();

  return value.length > 0 ? value : undefined;
}

function optionalEnv(env: NodeJS.ProcessEnv, key: string): string | undefined {
  const value = env[key]?.trim();

  return value === undefined || value.length === 0 ? undefined : value;
}

function report(input: {
  readonly mode: TranslationReconciliationRequest["mode"];
  readonly requestedAt: string;
  readonly runId?: string | undefined;
  readonly reason?: string | undefined;
  readonly maxItems: number;
  readonly minAgeSeconds: number;
  readonly status: TranslationReconciliationReport["status"];
  readonly candidates: readonly TranslationReconciliationCandidate[];
  readonly errors: readonly string[];
}): TranslationReconciliationReport {
  const replayedCount = input.candidates.filter((candidate) => candidate.status === "replayed").length;
  const failedClosedCount = input.candidates.filter((candidate) => candidate.status === "failed_closed").length;
  const skippedCount = input.status === "failed_closed"
    ? Math.max(0, input.candidates.length - failedClosedCount)
    : 0;
  const base = {
    service: "translation",
    mode: input.mode,
    status: input.status,
    requestedAt: input.requestedAt,
    maxItems: input.maxItems,
    minAgeSeconds: input.minAgeSeconds,
    selectedCount: input.candidates.length,
    replayedCount,
    failedClosedCount,
    skippedCount,
    writesPerformed: replayedCount > 0,
    dryRun: input.mode === "dry-run",
    productionVisibilityEnabled: false,
    legacyRuntimeRequired: false,
    protectedApplyRequired: true,
    candidates: input.candidates,
    errors: input.errors,
    metrics: {
      candidateCount: input.candidates.length,
      replayedCount,
      failedClosedCount,
      skippedCount
    }
  } satisfies Omit<TranslationReconciliationReport, "runId" | "reason">;

  return {
    ...base,
    ...(input.runId === undefined ? {} : {
      runId: input.runId
    }),
    ...(input.reason === undefined ? {} : {
      reason: input.reason
    })
  };
}

function candidateFromRow(row: TranslationOutboxRow, selectedReason: string): TranslationReconciliationCandidate {
  return {
    outboxId: String(row.id),
    idempotencyKey: row.idempotency_key,
    destinationStage: row.destination_stage,
    routingKey: row.routing_key,
    entityKind: row.entity_kind,
    entityId: row.entity_id,
    payloadRef: row.payload_ref,
    payloadDigest: row.payload_digest,
    selectedReason,
    status: "selected"
  };
}

function failedCandidate(
  candidate: TranslationReconciliationCandidate,
  failedClosedReason: string
): TranslationReconciliationCandidate {
  return {
    ...candidate,
    status: "failed_closed",
    failedClosedReason
  };
}

function boundedInteger(value: unknown, defaultValue: number, min: number, max: number): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    return defaultValue;
  }

  return Math.max(min, Math.min(max, value));
}

function safeRunId(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();

  return /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/u.test(trimmed) ? trimmed : undefined;
}

function safeReason(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.replace(/[\r\n\t]+/gu, " ").trim();

  return trimmed.length === 0 ? undefined : trimmed.slice(0, 160);
}

function flagEnabled(value: string | undefined): boolean {
  return value?.trim().toLowerCase() === "true";
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
    && (value.status === "success" || value.status === "permanent_failure")
    && typeof value.model === "string"
    && typeof value.promptId === "string"
    && typeof value.promptVersion === "string";
}

function isReplayableTranslationResultSnapshot(value: unknown): value is TranslationStoredLanguageResult {
  if (!isTranslationResultSnapshot(value)
    || typeof value.sourceMessageId !== "string"
    || typeof value.correlationId !== "string"
    || typeof value.traceparent !== "string"
    || typeof value.latencyMs !== "number"
    || typeof value.translatedAt !== "string"
    || (value.failureReason !== undefined && typeof value.failureReason !== "string")
    || (value.summaryRef !== undefined && !isTranslationSummaryRef(value.summaryRef))
    || !isTranslationQualityRef(value.qualityRef)
    || (value.aiUsageRef !== undefined && !isTranslationAiUsageRef(value.aiUsageRef))
    || (value.persistencePublication !== undefined && !isPersistencePublication(value.persistencePublication))) {
    return false;
  }

  return value.status === "success" ? value.summaryRef !== undefined : true;
}

function orderedSummaryRef(
  ref: NonNullable<TranslationStoredLanguageResult["summaryRef"]>
): NonNullable<TranslationStoredLanguageResult["summaryRef"]> {
  return {
    kind: "backend-record",
    uri: ref.uri,
    mediaType: "application/json",
    articleId: ref.articleId,
    targetLanguage: ref.targetLanguage,
    resultId: ref.resultId
  };
}

function orderedQualityRef(
  ref: NonNullable<TranslationStoredLanguageResult["qualityRef"]>
): NonNullable<TranslationStoredLanguageResult["qualityRef"]> {
  return {
    kind: "backend-record",
    uri: ref.uri,
    mediaType: "application/json",
    qualityScore: ref.qualityScore,
    resultId: ref.resultId
  };
}

function orderedAiUsageRef(
  ref: NonNullable<TranslationStoredLanguageResult["aiUsageRef"]>
): NonNullable<TranslationStoredLanguageResult["aiUsageRef"]> {
  return {
    kind: "backend-record",
    uri: ref.uri,
    mediaType: "application/json",
    inputTokens: ref.inputTokens,
    outputTokens: ref.outputTokens,
    totalTokens: ref.totalTokens
  };
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function stringArray(value: unknown): readonly string[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }

  const output: string[] = [];

  for (const item of value) {
    if (typeof item !== "string" || item.length === 0) {
      return undefined;
    }

    output.push(item);
  }

  return output;
}

function summaryRefArray(value: unknown): readonly NonNullable<TranslationStoredLanguageResult["summaryRef"]>[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }

  const output: NonNullable<TranslationStoredLanguageResult["summaryRef"]>[] = [];

  for (const item of value) {
    if (!isTranslationSummaryRef(item)) {
      return undefined;
    }

    output.push(item);
  }

  return output;
}

function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length) {
    return false;
  }

  const rightSet = new Set(right);

  return left.every((value) => rightSet.has(value));
}

function sameSummaryRefs(
  left: readonly NonNullable<TranslationStoredLanguageResult["summaryRef"]>[],
  right: readonly NonNullable<TranslationStoredLanguageResult["summaryRef"]>[]
): boolean {
  if (left.length !== right.length) {
    return false;
  }

  return left.every((ref, index) => {
    const other = right[index];

    if (other === undefined) {
      return false;
    }

    return ref.uri === other.uri
      && ref.articleId === other.articleId
      && ref.targetLanguage === other.targetLanguage
      && ref.resultId === other.resultId;
  });
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
