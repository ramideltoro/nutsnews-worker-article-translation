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
      env: {}
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
      }
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

  it("fails closed without publishing when the authoritative envelope is missing", async () => {
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
      }
    });

    const report = await reconciler.reconcile({
      mode: "apply",
      runId: "recovery-20260723",
      protectedConfirmation: TRANSLATION_RECONCILIATION_CONFIRMATION
    });

    expect(report.status).toBe("failed_closed");
    expect(report.errors).toContain("1:missing-stored-envelope");
    expect(report.writesPerformed).toBe(false);
    expect(transport.published).toHaveLength(0);
  });
});

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

  constructor(private readonly rows: readonly QueryResultRow[]) {}

  asPool() {
    return this as never;
  }

  query(sql: string, values: readonly unknown[] = []): Promise<{ readonly rows: QueryResultRow[]; readonly rowCount: number }> {
    this.queries.push({
      sql,
      values
    });

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
