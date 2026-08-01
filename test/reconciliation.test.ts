import { createHash } from "node:crypto";

import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import type {
  BrokerConsumerHandle,
  BrokerDeliveryHandler,
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport
} from "@ramideltoro/nutsnews-worker-runtime";
import type { QueryResultRow } from "pg";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  PostgresTranslationBrokerOutbox,
  PostgresTranslationOutboxReconciler
} from "../src/production.js";
import { TRANSLATION_RECONCILIATION_CONFIRMATION } from "../src/reconciliation.js";
import { stableUuid } from "../src/ids.js";
import type { TranslationConfig } from "../src/config.js";
import type { TranslationStoredLanguageResult } from "../src/dependencies.js";

const now = "2026-07-23T00:00:00.000Z";
const clock = {
  now: () => new Date(now)
};

describe("translation outbox reconciliation", () => {
  it("records full envelope and payload so service-owned replay can hydrate payload_ref", async () => {
    const pool = new FakePool([]);
    const outbox = new PostgresTranslationBrokerOutbox(pool.asPool());
    const command = persistenceCommand();

    await outbox.record(command, {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: getWorkerRoute("persistence").exchange,
      routingKey: getWorkerRoute("persistence").routingKey,
      confirmed: true,
      confirmedAt: now
    });

    const insert = firstQuery(pool);
    const diagnostic = JSON.parse(String(insert.values[14])) as Readonly<Record<string, unknown>>;

    expect(diagnostic).toMatchObject({
      envelope: {
        messageId: command.envelope.messageId,
        correlationId: command.envelope.correlationId,
        causationId: command.envelope.causationId,
        idempotencyKey: command.envelope.idempotencyKey
      },
      payload: {
        schemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand,
        idempotencyKey: command.envelope.idempotencyKey
      },
      payloadSchemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand
    });
  });

  it("dry-runs deterministic candidates without publishing", async () => {
    const command = persistenceCommand();
    const pool = new FakePool([
      outboxRow(command)
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresTranslationOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {},
      config: testConfig
    });

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723",
      maxItems: 10,
      minAgeSeconds: 900
    });

    expect(report).toMatchObject({
      status: "dry_run",
      selectedCount: 1,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.candidates[0]).toMatchObject({
      idempotencyKey: command.envelope.idempotencyKey,
      destinationStage: "persistence",
      status: "selected"
    });
    expect(transport.published).toHaveLength(0);
  });

  it("applies replay with a new message ID while preserving routing metadata and audit history", async () => {
    const command = persistenceCommand();
    const pool = new FakePool([
      outboxRow(command)
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresTranslationOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {
        NUTSNEWS_TRANSLATION_RECONCILIATION_APPLY_ENABLED: "true"
      },
      config: testConfig
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723",
      reason: "empty broker recovery",
      protectedConfirmation: TRANSLATION_RECONCILIATION_CONFIRMATION
    });

    expect(report).toMatchObject({
      status: "applied",
      selectedCount: 1,
      replayedCount: 1,
      writesPerformed: true,
      productionVisibilityEnabled: false
    });
    expect(transport.published).toHaveLength(1);
    const replay = transport.published[0];
    expect(replay?.envelope.messageId).not.toBe(command.envelope.messageId);
    expect(replay?.envelope.idempotencyKey).toBe(command.envelope.idempotencyKey);
    expect(replay?.envelope.correlationId).toBe(command.envelope.correlationId);
    expect(replay?.envelope.causationId).toBe(command.envelope.causationId);
    expect(replay?.envelope.aggregate).toEqual(command.envelope.aggregate);
    expect(pool.queries.some((query) => query.sql.includes("reconciliationAuditHistory"))).toBe(true);
  });

  it("fails closed without publishing when a legacy envelope cannot be recovered from service storage", async () => {
    const command = persistenceCommand();
    const row = {
      ...outboxRow(command),
      diagnostic_metadata: {
        payload: command.payload,
        payloadSchemaId: command.payload.schemaId
      }
    };
    const pool = new FakePool([
      row
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresTranslationOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {
        NUTSNEWS_TRANSLATION_RECONCILIATION_APPLY_ENABLED: "true"
      },
      config: testConfig
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723",
      protectedConfirmation: TRANSLATION_RECONCILIATION_CONFIRMATION
    });

    expect(report.status).toBe("failed_closed");
    expect(report.errors).toContain("1:missing-result-snapshot");
    expect(report.writesPerformed).toBe(false);
    expect(transport.published).toHaveLength(0);
  });

  it("recovers a legacy persistence row from translation_records without reusing the original message ID", async () => {
    const result = storedResultSnapshot();
    const command = legacyPersistenceCommand(result);
    const pool = new FakePool([
      {
        ...outboxRow(command),
        diagnostic_metadata: {
          payload: jsonbLikePersistencePayload(command.payload),
          payloadSchemaId: command.payload.schemaId,
          exchange: getWorkerRoute("persistence").exchange
        }
      }
    ], [
      {
        result_snapshot: result
      }
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresTranslationOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {
        NUTSNEWS_TRANSLATION_RECONCILIATION_APPLY_ENABLED: "true"
      },
      config: testConfig
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-legacy-translation",
      reason: "empty broker recovery",
      protectedConfirmation: TRANSLATION_RECONCILIATION_CONFIRMATION
    });

    expect(report).toMatchObject({
      status: "applied",
      selectedCount: 1,
      replayedCount: 1,
      writesPerformed: true,
      productionVisibilityEnabled: false
    });
    expect(transport.published).toHaveLength(1);
    const replay = transport.published[0];
    expect(replay?.payload).toEqual(command.payload);
    expect(replay?.envelope.messageId).not.toBe(command.envelope.messageId);
    expect(replay?.envelope.idempotencyKey).toBe(command.envelope.idempotencyKey);
    expect(replay?.envelope.correlationId).toBe(command.envelope.correlationId);
    expect(replay?.envelope.causationId).toBe(command.envelope.causationId);
    expect(replay?.envelope.aggregate).toEqual(command.envelope.aggregate);
  });

  it("recovers a legacy translation-status row when the status source differs from language result sources", async () => {
    const result = storedResultSnapshot();
    const command = legacyTranslationStatusCommand(
      result,
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3511",
      jsonbOrderedSummaryRef(result.summaryRef)
    );
    const pool = new FakePool([
      {
        ...outboxRow(command),
        diagnostic_metadata: {
          payload: jsonbLikeStatusPayload(command.payload),
          payloadSchemaId: command.payload.schemaId,
          exchange: getWorkerRoute("persistence").exchange
        }
      }
    ], [
      {
        result_snapshot: result
      }
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresTranslationOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {
        NUTSNEWS_TRANSLATION_RECONCILIATION_APPLY_ENABLED: "true"
      },
      config: testConfig
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-legacy-translation-status",
      reason: "empty broker recovery",
      protectedConfirmation: TRANSLATION_RECONCILIATION_CONFIRMATION
    });

    expect(report.status).toBe("applied");
    expect(transport.published).toHaveLength(1);
    const replay = transport.published[0];
    expect(replay?.payload).toEqual(command.payload);
    expect(replay?.envelope.messageId).not.toBe(command.envelope.messageId);
    expect(replay?.envelope.idempotencyKey).toBe(command.envelope.idempotencyKey);
    expect(replay?.envelope.correlationId).toBe(command.envelope.correlationId);
    expect(replay?.envelope.causationId).toBe(command.envelope.causationId);
    expect(replay?.envelope.aggregate).toEqual(command.envelope.aggregate);
  });

  it("reconstructs an authoritative persistence payload after JSONB reorders a complete carrier", async () => {
    const result = storedResultSnapshot();
    const command = legacyPersistenceCommand(result);
    const pool = new FakePool([
      {
        ...outboxRow(command),
        diagnostic_metadata: {
          envelope: command.envelope,
          payload: jsonbLikePersistencePayload(command.payload),
          payloadSchemaId: command.payload.schemaId
        }
      }
    ], [
      {
        result_snapshot: result
      }
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresTranslationOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {},
      config: testConfig
    });

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-jsonb-order-persistence"
    });

    expect(report).toMatchObject({
      status: "dry_run",
      selectedCount: 1,
      failedClosedCount: 0,
      writesPerformed: false
    });
    expect(transport.published).toHaveLength(0);
  });

  it("reconstructs an authoritative status payload after JSONB reorders a complete carrier", async () => {
    const result = storedResultSnapshot();
    const command = legacyTranslationStatusCommand(
      result,
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3511",
      jsonbOrderedSummaryRef(result.summaryRef)
    );
    const pool = new FakePool([
      {
        ...outboxRow(command),
        diagnostic_metadata: {
          envelope: command.envelope,
          payload: jsonbLikeStatusPayload(command.payload),
          payloadSchemaId: command.payload.schemaId
        }
      }
    ], [
      {
        result_snapshot: result
      }
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresTranslationOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {},
      config: testConfig
    });

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-jsonb-order-status"
    });

    expect(report).toMatchObject({
      status: "dry_run",
      selectedCount: 1,
      failedClosedCount: 0,
      writesPerformed: false
    });
    expect(transport.published).toHaveLength(0);
  });

  it("still fails closed when the authoritative translation payload digest is tampered", async () => {
    const result = storedResultSnapshot();
    const command = legacyPersistenceCommand(result);
    const pool = new FakePool([
      {
        ...outboxRow(command),
        payload_digest: `sha256:${"0".repeat(64)}`,
        diagnostic_metadata: {
          envelope: command.envelope,
          payload: jsonbLikePersistencePayload(command.payload),
          payloadSchemaId: command.payload.schemaId
        }
      }
    ], [
      {
        result_snapshot: result
      }
    ]);
    const transport = new FakeBrokerTransport();
    const reconciler = new PostgresTranslationOutboxReconciler({
      pool: pool.asPool(),
      brokerTransport: transport,
      clock,
      env: {},
      config: testConfig
    });

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-tampered-digest"
    });

    expect(report.status).toBe("failed_closed");
    expect(report.errors).toContain("1:payload-digest-mismatch");
    expect(report.writesPerformed).toBe(false);
    expect(transport.published).toHaveLength(0);
  });
});

const testConfig: TranslationConfig = {
  serviceName: "nutsnews-worker-article-translation",
  serviceVersion: "0.1.0",
  environment: "test",
  host: "test-host",
  http: {
    host: "127.0.0.1",
    port: 0
  },
  dependencyMode: "test",
  dependencies: {
    databaseConfigured: true,
    rabbitmqConfigured: true,
    qwenEndpointConfigured: true,
    qwenCredentialConfigured: true
  },
  qwen: {
    model: "qwen2.5:3b",
    promptId: "summary-translation-v1",
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
    minScore: 80,
    minSummaryChars: 24,
    maxSummaryChars: 420,
    repromptMaxAttempts: 2
  },
  concurrency: 2,
  prefetch: 4,
  shutdownTimeoutMs: 30_000,
  shadowMode: true,
  telemetryLogs: "silent",
  metricsEnabled: true
};

function persistenceCommand(): BrokerPublishCommand {
  const route = getWorkerRoute("persistence");
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3501",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3502",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3503",
    idempotencyKey: "translation:persistence:018f1598-2dd5-7c4f-9f92-8f7a7f8b3504",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: now,
    commandId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3504",
    commandKind: "save_summaries",
    backendOperation: "save-article-summaries-batch",
    entityRefs: [
      {
        articleId: "article-001",
        articleVersion: 1,
        targetLanguage: "fr"
      }
    ],
    writeMode: "upsert",
    providerMode: "backend_postgres_primary"
  };
  const envelope = assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "persistence",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3510",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3503",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3500",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: payload.idempotencyKey,
    aggregate: {
      type: "article",
      id: "article-001",
      version: 1
    },
    occurredAt: now,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: now
    },
    producer: {
      name: "translation",
      version: "0.1.0",
      instanceId: "test-host"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/translation/article-001/fr",
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(payload),
      digest: sha256Json(payload)
    }
  });

  return {
    envelope,
    payload
  };
}

function outboxRow(command: BrokerPublishCommand): QueryResultRow {
  return {
    id: "1",
    outbox_message_id: command.envelope.messageId,
    pipeline_run_id: command.payload.pipelineRunId,
    stage_execution_id: command.payload.stageExecutionId,
    destination_stage: command.envelope.route,
    routing_key: getWorkerRoute(command.envelope.route).routingKey,
    entity_kind: command.envelope.aggregate.type,
    entity_id: command.envelope.aggregate.id,
    schema_version: command.envelope.schemaVersion,
    operation_version: command.envelope.aggregate.version,
    idempotency_key: command.envelope.idempotencyKey,
    payload_ref: command.envelope.payloadRef.uri,
    payload_digest: sha256Json(command.payload),
    created_at: new Date("2026-07-22T23:00:00.000Z"),
    published_at: new Date("2026-07-22T23:00:01.000Z"),
    confirmed_at: new Date("2026-07-22T23:00:02.000Z"),
    status: "confirmed",
    diagnostic_metadata: {
      envelope: command.envelope,
      payload: command.payload,
      payloadSchemaId: command.payload.schemaId
    }
  };
}

function storedResultSnapshot(): TranslationStoredLanguageResult {
  const resultId = stableUuid([
    "article-001",
    "1",
    "en",
    "fr",
    "summary-translation-v1",
    "0.1.0",
    "qwen2.5:3b"
  ]);

  return {
    resultId,
    articleId: "article-001",
    articleVersion: 1,
    sourceLanguage: "en",
    targetLanguage: "fr",
    promptId: "summary-translation-v1",
    promptVersion: "0.1.0",
    model: "qwen2.5:3b",
    status: "success",
    summaryRef: {
      kind: "backend-record",
      uri: `backend://worker-uplift/translation/article-001/${resultId}/fr/summary`,
      mediaType: "application/json",
      articleId: "article-001",
      targetLanguage: "fr",
      resultId
    },
    qualityRef: {
      kind: "backend-record",
      uri: `backend://worker-uplift/translation/article-001/${resultId}/fr/quality`,
      mediaType: "application/json",
      qualityScore: 92,
      resultId
    },
    aiUsageRef: {
      kind: "backend-record",
      uri: `backend://worker-uplift/translation/article-001/${resultId}/fr/ai-usage`,
      mediaType: "application/json",
      inputTokens: 20,
      outputTokens: 12,
      totalTokens: 32
    },
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3503",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3500",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    latencyMs: 100,
    translatedAt: now
  };
}

function legacyPersistenceCommand(result: TranslationStoredLanguageResult): BrokerPublishCommand {
  const route = getWorkerRoute("persistence");
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.persistenceCommand,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3501",
    stageExecutionId: stableUuid([
      "persistence-command",
      result.resultId
    ]),
    sourceMessageId: result.sourceMessageId,
    idempotencyKey: `translation:persistence:${result.resultId}`,
    traceparent: result.traceparent,
    producedAt: result.translatedAt,
    commandId: result.resultId,
    commandKind: "save_summaries",
    backendOperation: "save-article-summaries-batch",
    entityRefs: [
      {
        articleId: result.articleId,
        articleVersion: result.articleVersion,
        sourceLanguage: result.sourceLanguage,
        targetLanguage: result.targetLanguage,
        summaryRef: result.summaryRef,
        qualityRef: result.qualityRef,
        aiUsageRef: result.aiUsageRef
      }
    ],
    writeMode: "upsert",
    providerMode: "backend_postgres_primary"
  };
  const envelope = assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "persistence",
    messageId: stableUuid([
      "persistence-message",
      payload.idempotencyKey
    ]),
    causationId: result.sourceMessageId,
    correlationId: result.correlationId,
    traceparent: result.traceparent,
    idempotencyKey: payload.idempotencyKey,
    aggregate: {
      type: "article",
      id: result.articleId,
      version: result.articleVersion
    },
    occurredAt: result.translatedAt,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: result.translatedAt
    },
    producer: {
      name: "nutsnews-worker-article-translation",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: `backend://worker-uplift/translation/${encodeURIComponent(result.articleId)}/${encodeURIComponent(result.resultId)}`,
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(payload)
    }
  });

  return {
    envelope,
    payload
  };
}

function legacyTranslationStatusCommand(
  result: TranslationStoredLanguageResult,
  sourceMessageId: string,
  summaryRef: NonNullable<TranslationStoredLanguageResult["summaryRef"]>
): BrokerPublishCommand {
  const route = getWorkerRoute("persistence");
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.translationResult,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3501",
    stageExecutionId: stableUuid([
      "translation-status",
      result.articleId,
      String(result.articleVersion),
      result.model
    ]),
    sourceMessageId,
    idempotencyKey: `translation:result:${result.articleId}:${String(result.articleVersion)}`,
    traceparent: result.traceparent,
    producedAt: now,
    articleId: result.articleId,
    translationStatus: "partial",
    completedLanguageCodes: [
      result.targetLanguage
    ],
    missingLanguageCodes: [
      "ja",
      "de-CH",
      "de",
      "el"
    ],
    summaryRefs: [
      summaryRef
    ]
  };
  const envelope = assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "persistence",
    messageId: stableUuid([
      "persistence-message",
      payload.idempotencyKey
    ]),
    causationId: sourceMessageId,
    correlationId: result.correlationId,
    traceparent: result.traceparent,
    idempotencyKey: payload.idempotencyKey,
    aggregate: {
      type: "article",
      id: result.articleId,
      version: result.articleVersion
    },
    occurredAt: now,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: now
    },
    producer: {
      name: "nutsnews-worker-article-translation",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: `backend://worker-uplift/translation/${encodeURIComponent(result.articleId)}/translation-status`,
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(payload)
    }
  });

  return {
    envelope,
    payload
  };
}

function jsonbOrderedSummaryRef(
  ref: NonNullable<TranslationStoredLanguageResult["summaryRef"]> | undefined
): NonNullable<TranslationStoredLanguageResult["summaryRef"]> {
  if (ref === undefined) {
    throw new Error("expected summary ref");
  }

  return {
    uri: ref.uri,
    kind: "backend-record",
    resultId: ref.resultId,
    articleId: ref.articleId,
    mediaType: "application/json",
    targetLanguage: ref.targetLanguage
  };
}

function jsonbLikePersistencePayload(payload: BrokerPublishCommand["payload"]): Readonly<Record<string, unknown>> {
  return {
    schemaId: payload.schemaId,
    commandId: payload.commandId,
    writeMode: payload.writeMode,
    entityRefs: payload.entityRefs,
    producedAt: payload.producedAt,
    commandKind: payload.commandKind,
    traceparent: payload.traceparent,
    providerMode: payload.providerMode,
    pipelineRunId: payload.pipelineRunId,
    schemaVersion: payload.schemaVersion,
    idempotencyKey: payload.idempotencyKey,
    sourceMessageId: payload.sourceMessageId,
    backendOperation: payload.backendOperation,
    stageExecutionId: payload.stageExecutionId
  };
}

function jsonbLikeStatusPayload(payload: BrokerPublishCommand["payload"]): Readonly<Record<string, unknown>> {
  return {
    schemaId: payload.schemaId,
    articleId: payload.articleId,
    producedAt: payload.producedAt,
    summaryRefs: payload.summaryRefs,
    traceparent: payload.traceparent,
    pipelineRunId: payload.pipelineRunId,
    schemaVersion: payload.schemaVersion,
    idempotencyKey: payload.idempotencyKey,
    sourceMessageId: payload.sourceMessageId,
    stageExecutionId: payload.stageExecutionId,
    translationStatus: payload.translationStatus,
    missingLanguageCodes: payload.missingLanguageCodes,
    completedLanguageCodes: payload.completedLanguageCodes
  };
}

function sha256Json(value: unknown): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

function firstQuery(pool: FakePool): { readonly sql: string; readonly values: readonly unknown[] } {
  const query = pool.queries[0];

  if (query === undefined) {
    throw new Error("expected a captured query");
  }

  return query;
}

class FakePool {
  readonly queries: { readonly sql: string; readonly values: readonly unknown[] }[] = [];

  constructor(
    private readonly rows: readonly QueryResultRow[],
    private readonly snapshotRows: readonly QueryResultRow[] = []
  ) {}

  asPool() {
    return this as never;
  }

  query(sql: string, values: readonly unknown[] = []): Promise<{ readonly rows: QueryResultRow[]; readonly rowCount: number }> {
    this.queries.push({
      sql,
      values
    });

    if (sql.includes("worker_uplift_translation.translation_records")) {
      return Promise.resolve({
        rows: [...this.snapshotRows],
        rowCount: this.snapshotRows.length
      });
    }

    if (sql.trimStart().startsWith("SELECT")) {
      return Promise.resolve({
        rows: [...this.rows],
        rowCount: this.rows.length
      });
    }

    return Promise.resolve({
      rows: [],
      rowCount: 1
    });
  }
}

class FakeBrokerTransport implements RuntimeBrokerTransport {
  readonly name = "fake-broker";
  readonly published: BrokerPublishCommand[] = [];

  connect(): Promise<void> {
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    void routes;
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: now
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    void stage;
    void handler;
    throw new Error("consume is not supported in fake transport");
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}
