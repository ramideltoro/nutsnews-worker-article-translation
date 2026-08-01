import type { Pool } from "pg";
import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

import type {
  RuntimeIdempotencyCompletion,
  RuntimeIdempotencyFailure
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  PostgresTranslationStateStore,
  TRANSLATION_IDEMPOTENCY_LEASE_MS,
  TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS
} from "../src/production.js";
import {
  ManualTranslationClock,
  createMinimalTranslationEnvelope
} from "../src/test-doubles.js";

interface FakeInboxRow {
  readonly idempotencyKey: string;
  readonly receivedAt: Date;
  status: "processing" | "processed" | "duplicate" | "failed" | "parked";
  processedAt: Date | null;
  claimToken: string | undefined;
  claimExpiresAt: string | undefined;
}

interface FakeQueryResult {
  readonly rowCount: number;
  readonly rows: Record<string, unknown>[];
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
}

class IdempotencyPoolDouble {
  readonly rows = new Map<string, FakeInboxRow>();
  readonly queries: string[] = [];
  rejectNextClaimAfterCommit = false;
  completionFailure: "before-commit" | "after-commit" | undefined;
  renewalCalls = 0;
  private renewalGate: Promise<void> | undefined;

  constructor(private readonly clock: ManualTranslationClock) {}

  asPool(): Pool {
    return this as unknown as Pool;
  }

  async query(sql: string, values: readonly unknown[] = []): Promise<FakeQueryResult> {
    const normalized = sql.replace(/\s+/gu, " ").trim();
    this.queries.push(normalized);

    if (normalized.startsWith("INSERT INTO worker_uplift_translation.inbox")) {
      return this.insert(values);
    }

    if (normalized.includes("SET status = 'processing'")) {
      return Promise.resolve(this.reclaim(values));
    }

    if (normalized.includes("SET status = 'processed'")) {
      return this.complete(values);
    }

    if (normalized.includes("coalesce(diagnostic_metadata, '{}'::jsonb)")) {
      return Promise.resolve(this.seedServerLease(values));
    }

    if (normalized.includes("jsonb_set(")) {
      return this.renew(values);
    }

    if (normalized.includes("SET status = 'failed'")) {
      return Promise.resolve(this.fail(values));
    }

    if (normalized.startsWith("SELECT status, received_at, processed_at")) {
      const row = this.rows.get(stringValue(values[0]));

      return Promise.resolve(result(row === undefined
        ? []
        : [{
            status: row.status,
            received_at: row.receivedAt,
            processed_at: row.processedAt
          }]));
    }

    if (normalized.startsWith("SELECT status FROM worker_uplift_translation.inbox")) {
      const row = this.rows.get(stringValue(values[0]));

      return Promise.resolve(result(row === undefined ? [] : [{ status: row.status }]));
    }

    throw new Error(`Unexpected test query: ${normalized}`);
  }

  holdNextRenewal(): Deferred<undefined> {
    const gate = deferred<undefined>();
    this.renewalGate = gate.promise;

    return gate;
  }

  private insert(values: readonly unknown[]): FakeQueryResult {
    const idempotencyKey = stringValue(values[9]);
    const existing = this.rows.get(idempotencyKey);

    if (existing !== undefined) {
      return result([]);
    }

    const row: FakeInboxRow = {
      idempotencyKey,
      receivedAt: new Date(stringValue(values[12])),
      status: "processing",
      processedAt: null,
      claimToken: stringValue(values[14]),
      claimExpiresAt: this.leaseExpiresAt(numberValue(values[15]))
    };
    this.rows.set(idempotencyKey, row);

    if (this.rejectNextClaimAfterCommit) {
      this.rejectNextClaimAfterCommit = false;
      throw new Error("claim response lost after commit");
    }

    return result([{ received_at: row.receivedAt }]);
  }

  private reclaim(values: readonly unknown[]): FakeQueryResult {
    const row = this.rows.get(stringValue(values[0]));

    if (row === undefined) {
      return result([]);
    }

    const observedAt = this.clock.now().getTime();
    const fallbackExpiry = row.receivedAt.getTime() + TRANSLATION_IDEMPOTENCY_LEASE_MS;
    const parsedExpiry = row.claimExpiresAt === undefined
      ? Number.NaN
      : Date.parse(row.claimExpiresAt);
    const expiry = Number.isFinite(parsedExpiry) ? parsedExpiry : fallbackExpiry;
    const reclaimable = row.status === "failed"
      || row.status === "parked"
      || (row.status === "processing" && expiry <= observedAt);

    if (!reclaimable) {
      return result([]);
    }

    const metadata = objectValue(values[1]);
    row.status = "processing";
    row.processedAt = null;
    row.claimToken = stringValue(metadata.idempotencyClaimToken);
    row.claimExpiresAt = this.leaseExpiresAt(numberValue(values[2]));

    return result([{ received_at: row.receivedAt }]);
  }

  private complete(values: readonly unknown[]): FakeQueryResult {
    const row = this.rows.get(stringValue(values[0]));

    if (this.completionFailure === "before-commit") {
      this.completionFailure = undefined;
      throw new Error("completion rejected before commit");
    }

    if (
      row?.status !== "processing"
      || row.claimToken !== stringValue(values[3])
    ) {
      return result([]);
    }

    row.status = "processed";
    row.processedAt = new Date(stringValue(values[1]));
    row.claimToken = undefined;
    row.claimExpiresAt = undefined;

    if (this.completionFailure === "after-commit") {
      this.completionFailure = undefined;
      throw new Error("completion response lost after commit");
    }

    return result([{ status: row.status }]);
  }

  private seedServerLease(values: readonly unknown[]): FakeQueryResult {
    const row = this.rows.get(stringValue(values[0]));
    const expiry = row?.claimExpiresAt;
    const parsedExpiry = expiry === undefined ? Number.NaN : Date.parse(expiry);
    const canonicalExpiry = expiry === undefined
      ? false
      : /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?Z$/u.test(expiry);
    const boundedExpiry = canonicalExpiry
      && Number.isFinite(parsedExpiry)
      && parsedExpiry <= this.clock.now().getTime() + numberValue(values[1]);

    if (
      row?.status !== "processing"
      || boundedExpiry
    ) {
      return result([]);
    }

    row.claimExpiresAt = this.leaseExpiresAt(numberValue(values[1]));
    return result([{ status: row.status }]);
  }

  private fail(values: readonly unknown[]): FakeQueryResult {
    const row = this.rows.get(stringValue(values[0]));

    if (
      row?.status !== "processing"
      || row.claimToken !== stringValue(values[4])
    ) {
      return result([]);
    }

    row.status = "failed";
    row.claimToken = undefined;
    row.claimExpiresAt = undefined;

    return result([{ status: row.status }]);
  }

  private async renew(values: readonly unknown[]): Promise<FakeQueryResult> {
    this.renewalCalls += 1;
    const gate = this.renewalGate;
    this.renewalGate = undefined;

    if (gate !== undefined) {
      await gate;
    }

    const row = this.rows.get(stringValue(values[0]));

    if (
      row?.status !== "processing"
      || row.claimToken !== stringValue(values[1])
      || row.claimExpiresAt === undefined
      || !Number.isFinite(Date.parse(row.claimExpiresAt))
      || Date.parse(row.claimExpiresAt) <= this.clock.now().getTime()
    ) {
      return result([]);
    }

    const nextExpiry = this.leaseExpiresAt(numberValue(values[2]));
    row.claimExpiresAt = nextExpiry;
    return result([{ status: row.status }]);
  }

  private leaseExpiresAt(leaseMs: number): string {
    return new Date(this.clock.now().getTime() + leaseMs).toISOString();
  }
}

const stores: PostgresTranslationStateStore[] = [];

afterEach(() => {
  for (const store of stores.splice(0)) {
    store.close();
  }

  vi.useRealTimers();
});

describe("PostgresTranslationStateStore Runtime 1 conformance", () => {
  it("never releases a claim whose commit response was lost and reclaims it only after the lease expires", async () => {
    const clock = new ManualTranslationClock();
    const pool = new IdempotencyPoolDouble(clock);
    const store = createStore(pool);
    pool.rejectNextClaimAfterCommit = true;

    await expect(store.claim("translation:test:claim-lost", claimContext())).rejects.toThrow(
      "claim response lost"
    );

    clock.advance(TRANSLATION_IDEMPOTENCY_LEASE_MS - 1);
    await expect(store.claim("translation:test:claim-lost", claimContext())).resolves.toMatchObject({
      status: "in-progress"
    });

    clock.advance(2);
    const reclaimed = await store.claim("translation:test:claim-lost", claimContext());
    expect(reclaimed).toMatchObject({
      status: "claimed",
      replay: true
    });
    expect(reclaimed.status === "claimed" && reclaimed.claimToken.length > 0).toBe(true);
  });

  it("releases reject-before-commit work only with the owning token", async () => {
    const clock = new ManualTranslationClock();
    const pool = new IdempotencyPoolDouble(clock);
    const store = createStore(pool);
    const claim = await claimed(store, "translation:test:before-commit");
    pool.completionFailure = "before-commit";

    await expect(store.markCompleted(
      "translation:test:before-commit",
      completion(claim.claimToken)
    )).rejects.toThrow("before commit");
    await expect(store.releaseClaim(
      "translation:test:before-commit",
      failure(claim.claimToken)
    )).resolves.toEqual({ status: "released" });

    await expect(store.claim(
      "translation:test:before-commit",
      claimContext()
    )).resolves.toMatchObject({
      status: "claimed",
      replay: true
    });
  });

  it("gives legacy in-progress rows a server-timed grace lease before reclaim", async () => {
    const clock = new ManualTranslationClock("2026-08-01T12:00:00.000Z");
    const pool = new IdempotencyPoolDouble(clock);
    const store = createStore(pool);
    pool.rows.set("translation:test:legacy", {
      idempotencyKey: "translation:test:legacy",
      receivedAt: new Date("2020-01-01T00:00:00.000Z"),
      status: "processing",
      processedAt: null,
      claimToken: undefined,
      claimExpiresAt: undefined
    });

    await expect(store.claim("translation:test:legacy", claimContext())).resolves.toMatchObject({
      status: "in-progress"
    });
    expect(pool.rows.get("translation:test:legacy")?.claimExpiresAt).toBe(
      "2026-08-01T12:05:00.000Z"
    );

    clock.advance(TRANSLATION_IDEMPOTENCY_LEASE_MS + 1);
    await expect(store.claim("translation:test:legacy", claimContext())).resolves.toMatchObject({
      status: "claimed",
      replay: true
    });
    const reclaimSql = pool.queries.find((query) => query.includes("SET status = 'processing'"));
    const seedSql = pool.queries.find((query) => query.includes(
      "coalesce(diagnostic_metadata, '{}'::jsonb)"
    ));
    expect(seedSql).toContain("pg_input_is_valid(");
    expect(reclaimSql).toContain("END <= clock_timestamp()");
    expect(reclaimSql).toContain("ELSE 'infinity'::timestamptz");
    expect(reclaimSql).toContain("pg_input_is_valid(");
  });

  it.each([
    ["regex-shaped impossible", "2026-99-99T99:99:99Z"],
    ["non-canonical infinity", "infinity"],
    ["far-future", "2030-01-01T00:00:00.000Z"]
  ])("normalizes a %s expiry with a server-timed grace lease", async (_case, expiry) => {
    const clock = new ManualTranslationClock("2026-08-01T12:00:00.000Z");
    const pool = new IdempotencyPoolDouble(clock);
    const store = createStore(pool);
    pool.rows.set("translation:test:unsafe-expiry", {
      idempotencyKey: "translation:test:unsafe-expiry",
      receivedAt: new Date("2026-08-01T11:00:00.000Z"),
      status: "processing",
      processedAt: null,
      claimToken: "legacy-owner",
      claimExpiresAt: expiry
    });

    await expect(store.claim(
      "translation:test:unsafe-expiry",
      claimContext()
    )).resolves.toMatchObject({
      status: "in-progress"
    });
    expect(pool.rows.get("translation:test:unsafe-expiry")?.claimExpiresAt).toBe(
      "2026-08-01T12:05:00.000Z"
    );

    const seedSql = pool.queries.find((query) => query.includes(
      "coalesce(diagnostic_metadata, '{}'::jsonb)"
    ));
    const reclaimSql = pool.queries.find((query) => query.includes("SET status = 'processing'"));
    expect(seedSql).toContain("pg_input_is_valid(");
    expect(seedSql).toContain("~ '^[0-9]{4}");
    expect(seedSql).toContain("> clock_timestamp() +");
    expect(reclaimSql).toContain("pg_input_is_valid(");
    expect(reclaimSql).toContain("~ '^[0-9]{4}");

    clock.advance(TRANSLATION_IDEMPOTENCY_LEASE_MS);
    await expect(store.claim(
      "translation:test:unsafe-expiry",
      claimContext()
    )).resolves.toMatchObject({
      status: "claimed",
      replay: true
    });
  });

  it("preserves commit-then-reject completion and rejects stale-token mutation", async () => {
    const clock = new ManualTranslationClock();
    const pool = new IdempotencyPoolDouble(clock);
    const store = createStore(pool);
    const first = await claimed(store, "translation:test:ownership");

    clock.advance(TRANSLATION_IDEMPOTENCY_LEASE_MS + 1);
    const second = await claimed(store, "translation:test:ownership");
    expect(second.claimToken).not.toBe(first.claimToken);
    await expect(store.releaseClaim(
      "translation:test:ownership",
      failure(first.claimToken)
    )).resolves.toEqual({ status: "not-owned" });

    pool.completionFailure = "after-commit";
    await expect(store.markCompleted(
      "translation:test:ownership",
      completion(second.claimToken)
    )).rejects.toThrow("after commit");
    await expect(store.releaseClaim(
      "translation:test:ownership",
      failure(second.claimToken)
    )).resolves.toEqual({ status: "preserved-completed" });
    await expect(store.markFailed(
      "translation:test:ownership",
      failure(first.claimToken)
    )).rejects.toThrow("another delivery");
    expect(pool.rows.get("translation:test:ownership")?.status).toBe("processed");
  });

  it("uses server-authoritative, strictly bounded, single-flight lease renewal", async () => {
    vi.useFakeTimers();
    const clock = new ManualTranslationClock();
    const pool = new IdempotencyPoolDouble(clock);
    const store = createStore(pool);
    const claim = await claimed(store, "translation:test:renewal");
    const row = pool.rows.get("translation:test:renewal");
    const initialExpiry = row?.claimExpiresAt;

    expect(claim.claimToken).toBe(row?.claimToken);
    expect(initialExpiry).toBe("2026-07-23T00:05:00.000Z");
    const renewalGate = pool.holdNextRenewal();

    clock.advance(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    vi.advanceTimersByTime(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    await settleMicrotasks();
    expect(pool.renewalCalls).toBe(1);

    clock.advance(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    vi.advanceTimersByTime(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    await settleMicrotasks();
    expect(pool.renewalCalls).toBe(1);

    renewalGate.resolve(undefined);
    await settleMicrotasks();
    expect(row?.claimExpiresAt).toBe("2026-07-23T00:07:00.000Z");

    clock.advance(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    vi.advanceTimersByTime(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    await settleMicrotasks();
    expect(pool.renewalCalls).toBe(2);
    expect(row?.claimExpiresAt).toBe("2026-07-23T00:08:00.000Z");

    if (row !== undefined) {
      row.claimExpiresAt = "2030-01-01T00:00:00.000Z";
    }
    clock.advance(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    vi.advanceTimersByTime(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    await settleMicrotasks();
    expect(pool.renewalCalls).toBe(3);
    expect(row?.claimExpiresAt).toBe("2026-07-23T00:09:00.000Z");

    const insertSql = pool.queries.find((query) => query.startsWith(
      "INSERT INTO worker_uplift_translation.inbox"
    ));
    const renewalSql = pool.queries.find((query) => query.includes("jsonb_set("));
    expect(insertSql).toContain("clock_timestamp()");
    expect(renewalSql).toContain("clock_timestamp()");
    expect(renewalSql).not.toContain("GREATEST(");
  });

  it("cannot renew a token-owned lease after its server-authoritative expiry", async () => {
    vi.useFakeTimers();
    const clock = new ManualTranslationClock();
    const pool = new IdempotencyPoolDouble(clock);
    const store = createStore(pool);
    const claim = await claimed(store, "translation:test:expired-renewal");
    const row = pool.rows.get("translation:test:expired-renewal");

    expect(row?.claimToken).toBe(claim.claimToken);
    clock.advance(TRANSLATION_IDEMPOTENCY_LEASE_MS);
    vi.advanceTimersByTime(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    await settleMicrotasks();

    expect(pool.renewalCalls).toBe(1);
    expect(row?.claimExpiresAt).toBe("2026-07-23T00:05:00.000Z");

    vi.advanceTimersByTime(TRANSLATION_IDEMPOTENCY_RENEWAL_INTERVAL_MS);
    await settleMicrotasks();
    expect(pool.renewalCalls).toBe(1);

    const renewalSql = pool.queries.find((query) => query.includes("jsonb_set("));
    expect(renewalSql).toContain("pg_input_is_valid(");
    expect(renewalSql).toContain("::timestamptz > clock_timestamp()");
    expect(renewalSql).toContain("ELSE false");
  });
});

function createStore(pool: IdempotencyPoolDouble): PostgresTranslationStateStore {
  const store = new PostgresTranslationStateStore(pool.asPool());
  stores.push(store);

  return store;
}

async function claimed(
  store: PostgresTranslationStateStore,
  idempotencyKey: string
): Promise<Extract<Awaited<ReturnType<PostgresTranslationStateStore["claim"]>>, { status: "claimed" }>> {
  const claim = await store.claim(idempotencyKey, claimContext());

  if (claim.status !== "claimed") {
    throw new Error(`Expected claimed result, received ${claim.status}.`);
  }

  return claim;
}

function claimContext() {
  return {
    envelope: createMinimalTranslationEnvelope(),
    stage: "translation" as const,
    receivedAt: "2026-07-23T00:00:00.000Z"
  };
}

function completion(claimToken: string): RuntimeIdempotencyCompletion {
  return {
    completedAt: "2026-07-23T00:01:00.000Z",
    messageId: createMinimalTranslationEnvelope().messageId,
    claimToken,
    stage: "translation"
  };
}

function failure(claimToken: string): RuntimeIdempotencyFailure {
  return {
    failedAt: "2026-07-23T00:01:00.000Z",
    messageId: createMinimalTranslationEnvelope().messageId,
    claimToken,
    stage: "translation",
    reason: "idempotency-completion-error",
    retryable: true
  };
}

function result(rows: Record<string, unknown>[]): FakeQueryResult {
  return {
    rowCount: rows.length,
    rows
  };
}

function objectValue(value: unknown): Record<string, unknown> {
  const parsed: unknown = JSON.parse(stringValue(value));

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("Expected JSON object query value.");
  }

  return parsed as Record<string, unknown>;
}

function stringValue(value: unknown): string {
  if (typeof value !== "string") {
    throw new Error("Expected string query value.");
  }

  return value;
}

function numberValue(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error("Expected finite number query value.");
  }

  return value;
}

function deferred<T>(): Deferred<T> {
  let resolvePromise: ((value: T) => void) | undefined;
  const promise = new Promise<T>((resolve) => {
    resolvePromise = resolve;
  });

  return {
    promise,
    resolve(value: T): void {
      if (resolvePromise === undefined) {
        throw new Error("Deferred promise resolver is unavailable.");
      }

      resolvePromise(value);
    }
  };
}

async function settleMicrotasks(): Promise<void> {
  for (let index = 0; index < 5; index += 1) {
    await Promise.resolve();
  }
}
