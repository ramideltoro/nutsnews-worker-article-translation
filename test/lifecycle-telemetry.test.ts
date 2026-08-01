import {
  STAGE_PAYLOAD_FIXTURES,
  WORKER_DELIVERY_BEHAVIOR,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  RUNTIME_ALLOWED_METRIC_LABELS,
  RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS,
  createBufferedRuntimeTelemetrySink,
  type RuntimeMessageDelivery,
  type RuntimeTelemetryEvent
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadTranslationConfig } from "../src/config.js";
import {
  TRANSLATION_DURATION_BUCKETS_SECONDS,
  TRANSLATION_RUNTIME_HEALTH_CHECKS,
  createTranslationPrometheusMetricsSink,
  type TranslationPrometheusMetricsSink
} from "../src/metrics.js";
import { createTranslationService } from "../src/service.js";
import {
  LocalBrokerTransport,
  LocalTranslationQwenClient,
  LocalTranslationWorkHandler,
  createLocalTranslationDependencies,
  createMinimalTranslationDelivery,
  createMinimalTranslationEnvelope,
  createMinimalTranslationPayload
} from "../src/test-doubles.js";

const COMPLETING_MESSAGE_EVENTS = new Set([
  "runtime.message.accepted",
  "runtime.message.duplicate",
  "runtime.message.invalid",
  "runtime.message.retry",
  "runtime.message.dlq"
]);

describe("translation lifecycle telemetry", () => {
  it("exports a deterministic bounded zero-valued stage family before traffic", async () => {
    const context = createTelemetryContext();
    const initial = context.metrics.collect();
    const initialStageSamples = canonicalStageSampleLines(initial);

    expect(context.metrics.collect()).toBe(initial);
    expect(initialStageSamples).toHaveLength(6 + TRANSLATION_DURATION_BUCKETS_SECONDS.length + 3);
    expect(initialStageSamples.every((line) => line.endsWith(" 0"))).toBe(true);
    expect(new Set(stageOutcomesFromMetrics(initial))).toEqual(new Set([
      "success",
      "duplicate",
      "invalid",
      "retry",
      "dlq",
      "failure"
    ]));
    expect(histogramBoundaries(initial)).toEqual([
      ...TRANSLATION_DURATION_BUCKETS_SECONDS.map(String),
      "+Inf"
    ]);
    expect(TRANSLATION_DURATION_BUCKETS_SECONDS).toContain(0.005);
    expect(TRANSLATION_DURATION_BUCKETS_SECONDS).toContain(0.025);
    expect(sampleValue(initial, "nutsnews_worker_last_success_timestamp_seconds")).toBe(0);

    await context.metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "translation",
      queue: "nutsnews.worker.translation.v1",
      outcome: "success",
      durationMs: 25
    });

    const afterTraffic = context.metrics.collect();
    expect(canonicalStageSeriesKeys(afterTraffic)).toEqual(canonicalStageSeriesKeys(initial));
    expect(metricValue(afterTraffic, "nutsnews_worker_uplift_stage_events_total", "success")).toBe(1);
    expect(sampleValue(afterTraffic, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(1);
    expect(sampleValue(afterTraffic, "nutsnews_worker_last_success_timestamp_seconds")).toBe(
      Date.parse("2026-07-23T00:00:00.000Z") / 1_000
    );
  });

  it("advances Runtime last-success monotonically for accepted and already-completed duplicate outcomes", async () => {
    const metrics = createTelemetryContext().metrics;
    const acceptedAt = "2026-08-01T03:00:00.900Z";
    const olderDuplicateAt = "2026-08-01T02:00:00.000Z";
    const newerDuplicateAt = "2026-08-01T04:00:00.100Z";

    await metrics.emit(translationCompletionEvent("runtime.message.accepted", acceptedAt));
    expect(sampleValue(metrics.collect(), "nutsnews_worker_last_success_timestamp_seconds")).toBe(
      Math.floor(Date.parse(acceptedAt) / 1_000)
    );

    await metrics.emit(translationCompletionEvent("runtime.message.duplicate", olderDuplicateAt, {
      completedAt: "2026-08-01T01:00:00.000Z"
    }));
    expect(sampleValue(metrics.collect(), "nutsnews_worker_last_success_timestamp_seconds")).toBe(
      Math.floor(Date.parse(acceptedAt) / 1_000)
    );

    await metrics.emit(translationCompletionEvent("runtime.message.duplicate", newerDuplicateAt, {
      completedAt: acceptedAt
    }));
    expect(sampleValue(metrics.collect(), "nutsnews_worker_last_success_timestamp_seconds")).toBe(
      Math.floor(Date.parse(newerDuplicateAt) / 1_000)
    );

    await metrics.emit(translationCompletionEvent("runtime.message.duplicate", "2026-08-01T05:00:00.000Z"));
    expect(sampleValue(metrics.collect(), "nutsnews_worker_last_success_timestamp_seconds")).toBe(
      Math.floor(Date.parse(newerDuplicateAt) / 1_000)
    );
  });

  it("keeps Runtime in-flight truthful while concurrent deliveries complete independently", async () => {
    const context = createTelemetryContext();
    const gates = [
      deferred<undefined>(),
      deferred<undefined>()
    ];
    const starts = [
      deferred<undefined>(),
      deferred<undefined>()
    ];
    let invocation = 0;

    vi.spyOn(context.workHandler, "handle").mockImplementation(async (_message, _tools, signal) => {
      const gate = gates[invocation];
      const started = starts[invocation];
      invocation += 1;

      if (gate === undefined || started === undefined) {
        throw new Error("Unexpected translation handler invocation.");
      }

      started.resolve(undefined);
      await gate.promise;
      signal.throwIfAborted();

      return {
        status: "ok"
      };
    });

    await context.service.start();
    const first = context.broker.deliverTranslation(translationDelivery(31));
    await starts[0]?.promise;
    const second = context.broker.deliverTranslation(translationDelivery(32));
    await starts[1]?.promise;

    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_inflight", {
      queue: "nutsnews.worker.translation.v1"
    })).toBe(2);

    gates[0]?.resolve(undefined);
    await expect(first).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_inflight", {
      queue: "nutsnews.worker.translation.v1"
    })).toBe(1);

    gates[1]?.resolve(undefined);
    await expect(second).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_inflight", {
      queue: "nutsnews.worker.translation.v1"
    })).toBe(0);

    await context.service.stop();
  });

  it("emits one completing event for accepted, duplicate, invalid, retry, retry-exhausted, and terminal deliveries", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    context.telemetry.clear();
    await exerciseLifecycleOutcomes(context);

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate",
      "runtime.message.started",
      "runtime.message.invalid",
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);
    expect(messageEvents.filter((event) => event.name === "runtime.message.started")).toHaveLength(6);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(6);
    expect(context.workHandler.handled).toHaveLength(4);

    const completions = messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name));
    expect(completions[0]).toMatchObject({
      name: "runtime.message.accepted",
      outcome: "success",
      messageId: messageId(1),
      idempotencyKey: idempotencyKey(1)
    });
    expect(completions[1]).toMatchObject({
      name: "runtime.message.duplicate",
      outcome: "duplicate"
    });
    expect(completions[2]).toMatchObject({
      name: "runtime.message.invalid",
      outcome: "invalid",
      attributes: {
        issueCode: "payload-consumer-mismatch",
        issuePath: "$.schemaId"
      }
    });
    expect(completions[3]).toMatchObject({
      name: "runtime.message.retry",
      outcome: "retry",
      attributes: {
        reason: "transient-translation-error"
      }
    });
    expect(completions[4]).toMatchObject({
      name: "runtime.message.dlq",
      outcome: "dlq",
      attributes: {
        reason: "retry-exhausted"
      }
    });
    expect(completions[5]).toMatchObject({
      name: "runtime.message.dlq",
      outcome: "dlq",
      attributes: {
        reason: "terminal-translation-error"
      }
    });

    await context.service.stop();
  });

  it.each([
    {
      operation: "claim",
      action: "retry",
      completion: "runtime.message.retry",
      reason: "idempotency-claim-error",
      attemptCount: 1
    },
    {
      operation: "claim",
      action: "dlq",
      completion: "runtime.message.dlq",
      reason: "idempotency-claim-error",
      attemptCount: WORKER_DELIVERY_BEHAVIOR.maxAttempts
    },
    {
      operation: "markCompleted",
      action: "retry",
      completion: "runtime.message.retry",
      reason: "idempotency-completion-error",
      attemptCount: 1
    },
    {
      operation: "markFailed",
      action: "retry",
      completion: "runtime.message.retry",
      reason: "idempotency-failure-record-error",
      attemptCount: 1
    }
  ] as const)("contains $operation failure as one $action completion", async ({
    operation,
    action,
    completion,
    reason,
    attemptCount
  }) => {
    const context = createTelemetryContext();

    if (operation === "claim") {
      vi.spyOn(context.dependencies.stateStore, "claim").mockRejectedValue(new Error("claim unavailable"));
    } else if (operation === "markCompleted") {
      vi.spyOn(context.dependencies.stateStore, "markCompleted").mockRejectedValue(new Error("completion unavailable"));
    } else {
      context.workHandler.result = {
        status: "retry",
        reason: "transient-translation-error"
      };
      vi.spyOn(context.dependencies.stateStore, "markFailed").mockRejectedValue(new Error("failure state unavailable"));
    }

    await context.service.start();
    context.telemetry.clear();

    const delivery = translationDelivery(9, {
      attempt: {
        count: attemptCount,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T00:00:00.000Z"
      }
    });
    await expect(context.broker.deliverTranslation(delivery)).resolves.toMatchObject({
      action,
      reason
    });

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      completion
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(1);
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_inflight", {
      queue: "nutsnews.worker.translation.v1"
    })).toBe(0);
    expect(metricValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_events_total", action)).toBe(1);
    expect(context.workHandler.handled).toHaveLength(operation === "claim" ? 0 : 1);

    await context.service.stop();
  });

  it("does not release an ambiguously committed claim without an ownership token", async () => {
    const context = createTelemetryContext();
    const store = context.dependencies.stateStore;
    const originalClaim = store.claim.bind(store);
    const releaseClaim = vi.spyOn(store, "releaseClaim");
    vi.spyOn(store, "claim").mockImplementationOnce(async (key, claimContext) => {
      await originalClaim(key, claimContext);
      throw new Error("claim response lost after commit");
    });

    await context.service.start();
    const delivery = translationDelivery(10);

    await expect(context.broker.deliverTranslation(delivery)).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-claim-error"
    });
    await expect(context.broker.deliverTranslation(delivery)).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-in-progress"
    });
    expect(releaseClaim).not.toHaveBeenCalled();
    expect(context.workHandler.handled).toHaveLength(0);
    await context.service.stop();
  });

  it("conditionally releases completion rejected before commit and succeeds on replay", async () => {
    const context = createTelemetryContext();
    const store = context.dependencies.stateStore;
    const releaseClaim = vi.spyOn(store, "releaseClaim");
    vi.spyOn(store, "markCompleted").mockRejectedValueOnce(
      new Error("completion rejected before commit")
    );

    await context.service.start();
    const delivery = translationDelivery(11);

    await expect(context.broker.deliverTranslation(delivery)).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-completion-error"
    });
    expect(releaseClaim).toHaveBeenCalledTimes(1);
    await expect(context.broker.deliverTranslation(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(context.workHandler.handled).toHaveLength(2);
    await context.service.stop();
  });

  it("acknowledges commit-then-reject completion on the final attempt", async () => {
    const context = createTelemetryContext();
    const store = context.dependencies.stateStore;
    const originalMarkCompleted = store.markCompleted.bind(store);
    const releaseClaim = vi.spyOn(store, "releaseClaim");
    vi.spyOn(store, "markCompleted").mockImplementationOnce(
      async (key, completion) => {
        await originalMarkCompleted(key, completion);
        throw new Error("completion response lost after commit");
      }
    );

    await context.service.start();
    context.telemetry.clear();
    const delivery = translationDelivery(12, {
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T00:00:00.000Z",
        lastAttemptAt: "2026-07-23T00:05:00.000Z"
      }
    });

    await expect(context.broker.deliverTranslation(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(releaseClaim).toHaveBeenCalledTimes(1);
    expect(context.telemetry.events.filter(
      (event) => event.name === "runtime.message.accepted"
    )).toHaveLength(1);
    expect(context.telemetry.events.some(
      (event) => event.name === "runtime.message.dlq"
    )).toBe(false);
    await context.service.stop();
  });

  it("preserves consumer-aware acceptance for approval decisions from the previous stage", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    await expect(context.broker.deliverTranslation({
      envelope: createMinimalTranslationEnvelope({
        messageId: messageId(6),
        idempotencyKey: idempotencyKey(6)
      }),
      payload: fixturePayload("approval-decision-rejected")
    })).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.workHandler.handled[0]?.payload.schemaId).toBe("nutsnews.worker.payload.approval-decision.v1");
    await context.service.stop();
  });

  it("counts bounded stage outcomes once and keeps identifiers out of Prometheus series", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    await exerciseLifecycleOutcomes(context);

    const output = context.metrics.collect();
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "success")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "duplicate")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "invalid")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "retry")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "dlq")).toBe(2);
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="test",service="translation",le="30"} 6');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="test",service="translation",le="+Inf"} 6');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_sum{environment="test",service="translation"} 0');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="test",service="translation"} 6');
    expect(context.metrics.allowedLabels).toEqual(RUNTIME_ALLOWED_METRIC_LABELS);

    for (const line of output.split("\n").filter((value) => value.startsWith("nutsnews_worker_uplift_stage_events_total{"))) {
      expect(metricLabelNames(line)).toEqual([
        "environment",
        "outcome",
        "service"
      ]);
    }

    for (const forbidden of RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS) {
      expect(output).not.toContain(`${forbidden}=`);
    }

    for (const identifier of [
      messageId(1),
      idempotencyKey(1),
      "article-001",
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    ]) {
      expect(output).not.toContain(identifier);
    }

    await context.service.stop();
  });

  it("exports truthful one-hot liveness, startup, readiness, and consumer state across the lifecycle", async () => {
    const context = createTelemetryContext();

    const initial = context.metrics.collect();
    expect(initial).not.toContain("nutsnews_worker_health_probe{");
    expect(initial).not.toContain("nutsnews_worker_consumers{");

    await expect(context.service.health.liveness()).resolves.toMatchObject({
      status: "ok"
    });
    await expect(context.service.health.startup()).resolves.toMatchObject({
      status: "unhealthy"
    });
    await expect(context.service.health.readiness()).resolves.toMatchObject({
      status: "unhealthy"
    });
    const beforeStart = context.metrics.collect();
    expectHealthOneHot(beforeStart, "liveness", "ok");
    expectHealthOneHot(beforeStart, "startup", "unhealthy");
    expectHealthOneHot(beforeStart, "readiness", "unhealthy");

    await context.service.start();
    const started = context.metrics.collect();
    expectHealthOneHot(started, "liveness", "ok");
    expectHealthOneHot(started, "startup", "ok");
    expectHealthOneHot(started, "readiness", "ok");
    expect(sampleValue(started, "nutsnews_worker_consumers", {
      queue: "nutsnews.worker.translation.v1"
    })).toBe(1);
    expect(started).not.toContain("nutsnews_worker_dependency_duration_ms");
    const healthDurationCountBeforeChannelDrop = sampleValue(
      started,
      "nutsnews_worker_health_check_duration_seconds_count",
      {
        check: "rabbitmq-consumer",
        probe: "readiness"
      }
    );

    await context.metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "error",
      at: "2026-07-31T00:00:01.000Z",
      stage: "translation",
      queue: "nutsnews.worker.translation.v1",
      outcome: "channel-dropped",
      attributes: {
        activeConsumers: 0,
        state: "channel-dropped"
      }
    });
    const channelDropped = context.metrics.collect();
    expect(sampleValue(channelDropped, "nutsnews_worker_consumers", {
      queue: "nutsnews.worker.translation.v1"
    })).toBe(0);
    expectHealthOneHot(channelDropped, "readiness", "unhealthy");
    expect(sampleValue(channelDropped, "nutsnews_worker_health_check", {
      check: "rabbitmq-consumer",
      outcome: "unhealthy",
      probe: "readiness"
    })).toBe(1);
    expect(sampleValue(channelDropped, "nutsnews_worker_health_check", {
      check: "rabbitmq-consumer",
      outcome: "ok",
      probe: "readiness"
    })).toBe(0);
    expect(sampleValue(channelDropped, "nutsnews_worker_health_check_duration_seconds_count", {
      check: "rabbitmq-consumer",
      probe: "readiness"
    })).toBe(healthDurationCountBeforeChannelDrop);

    await context.metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "info",
      at: "2026-07-31T00:00:02.000Z",
      stage: "translation",
      queue: "nutsnews.worker.translation.v1",
      outcome: "active",
      attributes: {
        activeConsumers: 1,
        state: "active"
      }
    });
    const consumerRestored = context.metrics.collect();
    expect(sampleValue(consumerRestored, "nutsnews_worker_consumers", {
      queue: "nutsnews.worker.translation.v1"
    })).toBe(1);
    expectHealthOneHot(consumerRestored, "readiness", "unhealthy");

    await expect(context.service.health.liveness()).resolves.toMatchObject({
      status: "ok"
    });
    await expect(context.service.health.startup()).resolves.toMatchObject({
      status: "ok"
    });
    await expect(context.service.health.readiness()).resolves.toMatchObject({
      status: "ok"
    });
    const healthy = context.metrics.collect();
    expectHealthOneHot(healthy, "readiness", "ok");
    expect(healthy.match(/^# HELP nutsnews_worker_health_check /gmu)).toHaveLength(1);
    expect(healthy.match(/^# TYPE nutsnews_worker_health_check gauge$/gmu)).toHaveLength(1);
    expect(healthy.match(/^# HELP nutsnews_worker_health_check_duration_seconds /gmu)).toHaveLength(1);
    expect(healthy.match(/^# TYPE nutsnews_worker_health_check_duration_seconds histogram$/gmu)).toHaveLength(1);
    expect(healthy.split("\n").some((line) => line.startsWith("nutsnews_worker_health_check{")
      && line.includes('check="broker-lifecycle"'))).toBe(true);
    expect(healthy.split("\n").some((line) => line.startsWith("nutsnews_worker_health_check_duration_seconds_bucket{")
      && line.includes('check="broker-lifecycle"'))).toBe(true);
    expect(healthy).not.toContain('check="other"');
    expect(healthChecksByProbe(healthy, "liveness")).toEqual(new Set([
      "process"
    ]));
    expect(healthChecksByProbe(healthy, "startup")).toEqual(new Set([
      "service-started"
    ]));
    expect(healthChecksByProbe(healthy, "readiness")).toEqual(new Set(
      TRANSLATION_RUNTIME_HEALTH_CHECKS.filter((check) => check !== "process" && check !== "service-started")
    ));
    expect(allHealthChecks(healthy)).toEqual(new Set(TRANSLATION_RUNTIME_HEALTH_CHECKS));

    const qwenClient = context.dependencies.qwenClient as LocalTranslationQwenClient;
    qwenClient.status = "unhealthy";
    await expect(context.service.health.readiness()).resolves.toMatchObject({
      status: "unhealthy"
    });
    const dependencyFailure = context.metrics.collect();
    expectHealthOneHot(dependencyFailure, "readiness", "unhealthy");
    expect(sampleValue(dependencyFailure, "nutsnews_worker_health_check", {
      check: "qwen-client",
      outcome: "unhealthy",
      probe: "readiness"
    })).toBe(1);
    expect(sampleValue(dependencyFailure, "nutsnews_worker_health_check", {
      check: "rabbitmq-consumer",
      outcome: "ok",
      probe: "readiness"
    })).toBe(1);

    qwenClient.status = "ok";
    await expect(context.service.health.readiness()).resolves.toMatchObject({
      status: "ok"
    });
    const dependencyRecovered = context.metrics.collect();
    expectHealthOneHot(dependencyRecovered, "readiness", "ok");
    expect(sampleValue(dependencyRecovered, "nutsnews_worker_health_check", {
      check: "qwen-client",
      outcome: "ok",
      probe: "readiness"
    })).toBe(1);
    expect(sampleValue(dependencyRecovered, "nutsnews_worker_health_check", {
      check: "qwen-client",
      outcome: "unhealthy",
      probe: "readiness"
    })).toBe(0);

    await context.service.consumer?.cancel();
    const cancelled = context.metrics.collect();
    expectHealthOneHot(cancelled, "readiness", "unhealthy");
    expect(sampleValue(cancelled, "nutsnews_worker_consumers", {
      queue: "nutsnews.worker.translation.v1"
    })).toBe(0);
    expect(sampleValue(cancelled, "nutsnews_worker_consumer_events_total", {
      outcome: "inactive",
      queue: "nutsnews.worker.translation.v1"
    })).toBe(1);
    expect(sampleValue(cancelled, "nutsnews_worker_health_check", {
      check: "rabbitmq-consumer",
      outcome: "unhealthy",
      probe: "readiness"
    })).toBe(1);
    expect(sampleValue(cancelled, "nutsnews_worker_health_check", {
      check: "rabbitmq-consumer",
      outcome: "ok",
      probe: "readiness"
    })).toBe(0);

    await context.service.stop();
    const stopped = context.metrics.collect();
    expectHealthOneHot(stopped, "liveness", "ok");
    expectHealthOneHot(stopped, "startup", "unhealthy");
    expectHealthOneHot(stopped, "readiness", "unhealthy");
    expect(sampleValue(stopped, "nutsnews_worker_health_check", {
      check: "service-started",
      outcome: "unhealthy",
      probe: "startup"
    })).toBe(1);
    expect(sampleValue(stopped, "nutsnews_worker_health_check", {
      check: "broker-lifecycle",
      outcome: "unhealthy",
      probe: "readiness"
    })).toBe(1);
    expect(sampleValue(stopped, "nutsnews_worker_consumer_events_total", {
      outcome: "inactive",
      queue: "nutsnews.worker.translation.v1"
    })).toBe(1);
  });

  it("keeps synchronous and rejected telemetry best-effort without changing exact-one delivery outcomes", async () => {
    const config = loadTranslationConfig({
      HOSTNAME: "translation-rejecting-telemetry-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_TRANSLATION_HTTP_PORT: "0",
      NUTSNEWS_TRANSLATION_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalTranslationDependencies();
    const events: RuntimeTelemetryEvent[] = [];
    let emissionCount = 0;
    const rejectingSink: TranslationPrometheusMetricsSink = {
      allowedLabels: RUNTIME_ALLOWED_METRIC_LABELS,
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
      emit: (event) => {
        events.push(event);
        emissionCount += 1;

        if (emissionCount % 2 === 1) {
          throw new Error("telemetry synchronous failure");
        }

        return Promise.reject(new Error("telemetry asynchronous failure"));
      },
      collect: () => {
        throw new Error("metrics unavailable");
      },
      setInFlight: () => {
        throw new Error("metrics unavailable");
      },
      setShutdownDraining: () => {
        throw new Error("metrics unavailable");
      },
      setExpectedActive: () => {
        throw new Error("metrics unavailable");
      },
      setLastSuccessTimestamp: () => {
        throw new Error("metrics unavailable");
      }
    };
    const service = createTranslationService({
      config,
      dependencies,
      telemetry: rejectingSink,
      metrics: rejectingSink
    });
    const context = {
      broker: dependencies.brokerTransport as LocalBrokerTransport,
      service,
      workHandler: dependencies.workHandler as LocalTranslationWorkHandler
    };

    await expect(service.start()).resolves.toBeUndefined();
    events.length = 0;
    await exerciseLifecycleOutcomes(context);

    const messageEvents = events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate",
      "runtime.message.started",
      "runtime.message.invalid",
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(6);
    expect(context.workHandler.handled).toHaveLength(4);
    await expect(service.stop()).resolves.toBeUndefined();
  });

  it("exports language latency as fixed seconds buckets and bounds language, provider, outcome, and token dimensions", async () => {
    const untrustedLanguages = Array.from({
      length: 1_000
    }, (_, index) => `article-${String(index)}`);
    const metrics = createTranslationPrometheusMetricsSink({
      identity: {
        service: "nutsnews-worker-article-translation",
        version: "0.1.0",
        environment: "test",
        host: "translation-test"
      },
      expectedActive: false,
      allowedLanguages: [
        "fr",
        ...untrustedLanguages
      ]
    });

    await metrics.emit(languageEvent({
      durationMs: 41,
      outcome: "success",
      language: "fr",
      provider: "local_ai",
      inputTokens: 10,
      outputTokens: 4,
      totalTokens: 14
    }));
    await metrics.emit(languageEvent({
      durationMs: 31_000,
      outcome: "retry",
      language: "article-001",
      provider: "provider-per-request"
    }));
    for (const [index, language] of untrustedLanguages.entries()) {
      await metrics.emit(languageEvent({
        durationMs: index,
        outcome: "success",
        language,
        provider: `provider-${String(index)}`
      }));
    }

    const output = metrics.collect();
    expect(output).toContain("# TYPE nutsnews_translation_language_duration_seconds histogram");
    expect(output).toContain('nutsnews_translation_language_duration_seconds_bucket{environment="test",language="fr",outcome="success",provider="local_ai",service="translation",stage="translation",le="0.05"} 1');
    expect(output).toContain('nutsnews_translation_language_duration_seconds_bucket{environment="test",language="unknown",outcome="retry",provider="unknown",service="translation",stage="translation",le="+Inf"} 1');
    expect(output).toContain('nutsnews_translation_language_duration_seconds_sum{environment="test",language="unknown",outcome="retry",provider="unknown",service="translation",stage="translation"} 31');
    expect(output).toContain('nutsnews_translation_language_duration_seconds_count{environment="test",language="unknown",outcome="retry",provider="unknown",service="translation",stage="translation"} 1');
    expect(output).toContain("nutsnews_translation_language_input_tokens_total");
    expect(output).toContain("nutsnews_translation_language_output_tokens_total");
    expect(output).toContain("nutsnews_translation_language_total_tokens_total");
    expect(output).not.toContain("nutsnews_translation_language_latency_ms");
    expect(output).not.toContain("token_kind=");
    expect(output).not.toContain("provider-per-request");
    expect(output).not.toContain("article-001");
    expect(output.split("\n").filter((line) => line.startsWith("nutsnews_translation_language_results_total{")).length).toBe(3);
    expect(metrics.translationMetricLabels).toEqual([
      "environment",
      "service",
      "stage",
      "outcome",
      "language",
      "provider"
    ]);
  });

  it("delegates measured zero-duration dependencies to Runtime without manufacturing duration-less samples", async () => {
    const metrics = createTranslationPrometheusMetricsSink({
      identity: {
        service: "nutsnews-worker-article-translation",
        version: "0.1.0",
        environment: "test",
        host: "translation-test"
      },
      expectedActive: false
    });

    await metrics.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-31T00:00:00.000Z",
      stage: "translation",
      queue: "nutsnews.worker.translation.v1",
      outcome: "success",
      attributes: {
        event: "translation.configuration",
        dependency: "translation-shell"
      }
    });
    await metrics.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-31T00:00:00.000Z",
      stage: "translation",
      queue: "nutsnews.worker.translation.v1",
      durationMs: 0,
      outcome: "success",
      attributes: {
        event: "translation.startup",
        dependency: "translation-shell"
      }
    });

    const output = metrics.collect();

    expect(sampleValue(output, "nutsnews_worker_dependency_duration_seconds_count", {
      dependency: "translation-shell"
    })).toBe(1);
    expect(sampleValue(output, "nutsnews_worker_dependency_duration_seconds_sum", {
      dependency: "translation-shell"
    })).toBe(0);
    expect(output).not.toContain("nutsnews_worker_dependency_duration_ms");
  });
});

function createTelemetryContext() {
  const config = loadTranslationConfig({
    HOSTNAME: "translation-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_TRANSLATION_HTTP_PORT: "0",
    NUTSNEWS_TRANSLATION_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalTranslationDependencies();
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createTranslationPrometheusMetricsSink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    },
    expectedActive: !config.shadowMode,
    allowedLanguages: config.languagePolicy.targetLanguages
  });
  const service = createTranslationService({
    config,
    dependencies,
    telemetry: {
      emit: async (event) => {
        await telemetry.emit(event);
        await metrics.emit(event);
      }
    },
    metrics
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    dependencies,
    metrics,
    service,
    telemetry,
    workHandler: dependencies.workHandler as LocalTranslationWorkHandler
  };
}

interface LifecycleContext {
  readonly broker: LocalBrokerTransport;
  readonly service: ReturnType<typeof createTranslationService>;
  readonly workHandler: LocalTranslationWorkHandler;
}

async function exerciseLifecycleOutcomes(context: LifecycleContext): Promise<void> {
  const accepted = translationDelivery(1);
  await expect(context.broker.deliverTranslation(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "handled"
  });
  await expect(context.broker.deliverTranslation(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "duplicate"
  });
  await expect(context.broker.deliverTranslation({
    ...translationDelivery(2),
    payload: fixturePayload("translation-result-partial")
  })).resolves.toMatchObject({
    action: "dlq",
    reason: "payload-consumer-mismatch"
  });

  context.workHandler.result = {
    status: "retry",
    reason: "transient-translation-error",
    retryAfterMs: 2_000
  };
  await expect(context.broker.deliverTranslation(translationDelivery(3))).resolves.toMatchObject({
    action: "retry",
    reason: "transient-translation-error"
  });

  context.workHandler.result = {
    status: "retry",
    reason: "retry-exhausted"
  };
  await expect(context.broker.deliverTranslation(translationDelivery(4, {
    attempt: {
      count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T00:00:00.000Z",
      lastAttemptAt: "2026-07-23T00:05:00.000Z"
    }
  }))).resolves.toMatchObject({
    action: "dlq",
    reason: "retry-exhausted"
  });

  context.workHandler.result = {
    status: "terminal-failure",
    reason: "terminal-translation-error"
  };
  await expect(context.broker.deliverTranslation(translationDelivery(5))).resolves.toMatchObject({
    action: "dlq",
    reason: "terminal-translation-error"
  });
}

function translationDelivery(
  sequence: number,
  envelopeOverrides: Partial<WorkerMessageEnvelope> = {}
): RuntimeMessageDelivery {
  return {
    ...createMinimalTranslationDelivery(),
    envelope: createMinimalTranslationEnvelope({
      messageId: messageId(sequence),
      idempotencyKey: idempotencyKey(sequence),
      ...envelopeOverrides
    }),
    payload: createMinimalTranslationPayload({
      idempotencyKey: idempotencyKey(sequence)
    })
  };
}

function fixturePayload(name: string): Readonly<Record<string, unknown>> {
  const fixture = STAGE_PAYLOAD_FIXTURES.find((candidate) => candidate.name === name);

  if (fixture === undefined) {
    throw new Error(`Missing worker contract fixture ${name}.`);
  }

  return fixture.payload;
}

function messageId(sequence: number): string {
  return `018f1598-2dd5-7c4f-9f92-8f7a7f8b58${String(sequence).padStart(2, "0")}`;
}

function idempotencyKey(sequence: number): string {
  return `approval:translation:telemetry-${String(sequence)}`;
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
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

function translationCompletionEvent(
  name: "runtime.message.accepted" | "runtime.message.duplicate",
  at: string,
  attributes?: Readonly<Record<string, unknown>>
): RuntimeTelemetryEvent {
  return {
    name,
    level: "info",
    at,
    stage: "translation",
    queue: "nutsnews.worker.translation.v1",
    outcome: name === "runtime.message.accepted" ? "success" : "duplicate",
    ...(attributes === undefined ? {} : {
      attributes
    })
  };
}

function metricValue(output: string, metric: string, outcome: string): number {
  const matches = output
    .split("\n")
    .filter((line) => line.startsWith(`${metric}{`) && line.includes(`outcome="${outcome}"`));

  expect(matches).toHaveLength(1);

  return Number(matches[0]?.split(" ").at(-1));
}

function canonicalStageSampleLines(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_events_total{")
      || line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_"));
}

function canonicalStageSeriesKeys(output: string): readonly string[] {
  return canonicalStageSampleLines(output).map((line) => line.slice(0, line.lastIndexOf(" ")));
}

function stageOutcomesFromMetrics(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_events_total{"))
    .map((line) => /outcome="([^"]+)"/u.exec(line)?.[1] ?? "");
}

function histogramBoundaries(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_bucket{"))
    .map((line) => /le="([^"]+)"/u.exec(line)?.[1] ?? "");
}

function allHealthChecks(output: string): ReadonlySet<string> {
  return healthChecks(output);
}

function healthChecksByProbe(output: string, probe: "liveness" | "startup" | "readiness"): ReadonlySet<string> {
  return healthChecks(output, probe);
}

function healthChecks(output: string, probe?: "liveness" | "startup" | "readiness"): ReadonlySet<string> {
  return new Set(output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_health_check{"))
    .filter((line) => probe === undefined || line.includes(`probe="${probe}"`))
    .map((line) => /check="([^"]+)"/u.exec(line)?.[1] ?? ""));
}

function expectHealthOneHot(
  output: string,
  probe: "liveness" | "startup" | "readiness",
  expected: "ok" | "degraded" | "unhealthy"
): void {
  const outcomes = [
    "ok",
    "degraded",
    "unhealthy"
  ] as const;
  const values = outcomes.map((outcome) => sampleValue(output, "nutsnews_worker_health_probe", {
    probe,
    outcome
  }));

  expect(values.reduce((sum, value) => sum + value, 0)).toBe(1);
  expect(values[outcomes.indexOf(expected)]).toBe(1);
}

function sampleValue(
  output: string,
  metric: string,
  labels: Readonly<Record<string, string>> = {}
): number {
  const matches = output
    .split("\n")
    .filter((line) => line.startsWith(`${metric}{`))
    .filter((line) => Object.entries(labels).every(([name, value]) => line.includes(`${name}="${value}"`)));

  expect(matches).toHaveLength(1);

  return Number(matches[0]?.split(" ").at(-1));
}

function metricLabelNames(line: string): readonly string[] {
  const start = line.indexOf("{");
  const end = line.indexOf("}", start);

  return line
    .slice(start + 1, end)
    .split(",")
    .map((label) => label.slice(0, label.indexOf("=")));
}

function languageEvent(options: {
  readonly durationMs: number;
  readonly outcome: "success" | "retry";
  readonly language: string;
  readonly provider: string;
  readonly inputTokens?: number;
  readonly outputTokens?: number;
  readonly totalTokens?: number;
}): RuntimeTelemetryEvent {
  return {
    name: "runtime.dependency.observed",
    level: options.outcome === "success" ? "info" : "warn",
    at: "2026-07-31T00:00:00.000Z",
    stage: "translation",
    queue: "nutsnews.worker.translation.v1",
    outcome: options.outcome,
    durationMs: options.durationMs,
    attributes: {
      event: "translation.language.reviewed",
      dependency: "article-translation",
      targetLanguage: options.language,
      provider: options.provider,
      ...(options.inputTokens === undefined ? {} : {
        inputTokens: options.inputTokens
      }),
      ...(options.outputTokens === undefined ? {} : {
        outputTokens: options.outputTokens
      }),
      ...(options.totalTokens === undefined ? {} : {
        totalTokens: options.totalTokens
      })
    }
  };
}
