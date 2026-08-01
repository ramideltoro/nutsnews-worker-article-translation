import {
  getRetryDestination,
  getWorkerRoute,
  validateStagePayload,
  validateWorkerEnvelope,
  type StagePayloadValidationIssue,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createBrokerConsumerReadinessCheck,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type RuntimeHealthCheck,
  type RuntimeHealthReport,
  type RuntimeHealthProbeSet,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyStore,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetrySink,
  type RuntimeValidationIssue
} from "@ramideltoro/nutsnews-worker-runtime";

import type { TranslationConfig } from "./config.js";
import type {
  TranslationDependencies,
  TranslationDependencyProbe
} from "./dependencies.js";
import type {
  TranslationHealthOutcome,
  TranslationHealthProbe,
  TranslationPrometheusMetricsSink,
  TranslationRuntimeMetricsSink
} from "./metrics.js";
import {
  bestEffortTelemetrySink,
  runTelemetryBestEffort
} from "./telemetry.js";

export interface TranslationServiceOptions {
  readonly config: TranslationConfig;
  readonly dependencies: TranslationDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: TranslationRuntimeMetricsSink;
}

export interface TranslationService {
  readonly broker: BrokerLifecycle;
  readonly health: RuntimeHealthProbeSet;
  readonly isStarted: boolean;
  readonly isDraining: boolean;
  readonly consumer: BrokerConsumerHandle | undefined;
  start(): Promise<void>;
  stop(): Promise<void>;
  processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult>;
}

export function createTranslationService(options: TranslationServiceOptions): TranslationService {
  const translationRoute = getWorkerRoute("translation");
  const persistenceRoute = getWorkerRoute("persistence");
  const telemetry = bestEffortTelemetrySink(options.telemetry);
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      translationRoute,
      persistenceRoute
    ],
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const processor = createTranslationInputProcessor({
    dependencies: options.dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    handler: async (context) => {
      try {
        return await drain.track(async () => {
          setInFlight(options.metrics, translationRoute.mainQueue.name, drain.inFlight);
          const dependencyStartedAtMs = options.dependencies.clock.now().getTime();
          const result = await options.dependencies.workHandler.handle(context, {
            publish: (command) => broker.publish(command),
            recordOutbox: (command, receipt) => options.dependencies.brokerOutbox.record(command, receipt),
            withTransaction: (operation) => options.dependencies.transactionRunner.withTransaction(operation)
          });

          await emitRuntimeTelemetry(telemetry, {
            name: "runtime.dependency.observed",
            level: result.status === "ok" ? "info" : "warn",
            at: runtimeNow(options.dependencies.clock),
            stage: "translation",
            queue: translationRoute.mainQueue.name,
            durationMs: elapsedMs(options.dependencies.clock, dependencyStartedAtMs),
            outcome: result.status === "ok" ? "success" : result.status === "retry" ? "retry" : "failure",
            attributes: {
              event: "translation.message.delegated",
              dependency: options.dependencies.workHandler.name,
              shadowMode: options.config.shadowMode
            }
          });

          return result;
        });
      } finally {
        setInFlight(options.metrics, translationRoute.mainQueue.name, drain.inFlight);
      }
    }
  });
  let started = false;
  let consumer: BrokerConsumerHandle | undefined;

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      const probes = createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started)
        ],
        readinessChecks: [
          brokerReadinessCheck(broker),
          createBrokerConsumerReadinessCheck(broker, "translation"),
          dependencyReadinessCheck("translation-state", options.dependencies.stateStore),
          dependencyReadinessCheck("database-transactions", options.dependencies.transactionRunner),
          dependencyReadinessCheck("broker-outbox", options.dependencies.brokerOutbox),
          dependencyReadinessCheck("qwen-client", options.dependencies.qwenClient),
          dependencyReadinessCheck("prompt-registry", options.dependencies.promptRegistry),
          dependencyReadinessCheck("language-policy", options.dependencies.languagePolicy),
          dependencyReadinessCheck("quality-validator", options.dependencies.qualityValidator),
          shadowModeCheck(options.config)
        ],
        clock: options.dependencies.clock,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      });

      return observeHealthProbes(probes, options.metrics);
    },
    get isStarted(): boolean {
      return started;
    },
    get isDraining(): boolean {
      return drain.isDraining;
    },
    get consumer(): BrokerConsumerHandle | undefined {
      return consumer;
    },
    async start(): Promise<void> {
      if (started) {
        return;
      }

      await broker.start();
      const brokerConsumer = await broker.consume("translation", processor);
      consumer = {
        stage: brokerConsumer.stage,
        cancel: async () => {
          await brokerConsumer.cancel();
          setConsumerActive(options.metrics, 0);
          setHealthProbe(options.metrics, "readiness", "unhealthy");
        }
      };
      started = true;
      setConsumerActive(options.metrics, 1);
      setHealthProbe(options.metrics, "startup", "ok");
      setInFlight(options.metrics, translationRoute.mainQueue.name, drain.inFlight);
      await refreshReadinessBestEffort(
        () => service.health.readiness(),
        options.metrics
      );
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      setShutdownDraining(options.metrics, true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      setShutdownDraining(options.metrics, false);
      setInFlight(options.metrics, translationRoute.mainQueue.name, drain.inFlight);
      setConsumerActive(options.metrics, 0);
      setHealthProbe(options.metrics, "startup", "unhealthy");
      setHealthProbe(options.metrics, "readiness", "unhealthy");
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies TranslationService;

  return service;
}

function setConsumerActive(
  metrics: TranslationRuntimeMetricsSink | undefined,
  activeConsumers: number
): void {
  if (isTranslationMetrics(metrics)) {
    runTelemetryBestEffort(() => metrics.setConsumerActive(activeConsumers));
  }
}

function setHealthProbe(
  metrics: TranslationRuntimeMetricsSink | undefined,
  probe: TranslationHealthProbe,
  outcome: TranslationHealthOutcome
): void {
  if (isTranslationMetrics(metrics)) {
    runTelemetryBestEffort(() => metrics.setHealthProbe(probe, outcome));
  }
}

function setInFlight(
  metrics: TranslationRuntimeMetricsSink | undefined,
  queue: string,
  value: number
): void {
  runTelemetryBestEffort(() => metrics?.setInFlight(queue, value));
}

function setShutdownDraining(
  metrics: TranslationRuntimeMetricsSink | undefined,
  draining: boolean
): void {
  runTelemetryBestEffort(() => metrics?.setShutdownDraining(draining));
}

function observeHealthProbes(
  probes: RuntimeHealthProbeSet,
  metrics: TranslationRuntimeMetricsSink | undefined
): RuntimeHealthProbeSet {
  const observe = async <T extends RuntimeHealthReport>(
    probe: TranslationHealthProbe,
    operation: () => Promise<T>
  ): Promise<T> => {
    const report = await operation();
    setHealthProbe(metrics, probe, report.status);

    return report;
  };

  return {
    liveness: () => observe("liveness", () => probes.liveness()),
    startup: () => observe("startup", () => probes.startup()),
    readiness: () => observe("readiness", () => probes.readiness())
  };
}

async function refreshReadinessBestEffort(
  operation: () => Promise<RuntimeHealthReport>,
  metrics: TranslationRuntimeMetricsSink | undefined
): Promise<void> {
  try {
    await operation();
  } catch {
    setHealthProbe(metrics, "readiness", "unhealthy");
  }
}

function isTranslationMetrics(
  metrics: TranslationRuntimeMetricsSink | undefined
): metrics is TranslationPrometheusMetricsSink {
  return metrics !== undefined
    && "setConsumerActive" in metrics
    && typeof metrics.setConsumerActive === "function"
    && "setHealthProbe" in metrics
    && typeof metrics.setHealthProbe === "function";
}

interface TranslationInputProcessorOptions {
  readonly dependencies: TranslationDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  handler(context: RuntimeMessageContext): Promise<{ readonly status: "ok" } | { readonly status: "retry"; readonly reason: string; readonly retryAfterMs?: number } | { readonly status: "terminal-failure"; readonly reason: string }>;
}

function createTranslationInputProcessor(options: TranslationInputProcessorOptions) {
  return async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const receivedAt = delivery.receivedAt ?? runtimeNow(options.dependencies.clock);
    const startedAtMs = options.dependencies.clock.now().getTime();
    const queue = getWorkerRoute("translation").mainQueue.name;
    await emitRuntimeTelemetry(options.telemetry, {
      name: "runtime.message.started",
      level: "info",
      at: runtimeNow(options.dependencies.clock),
      stage: "translation",
      queue,
      outcome: "started"
    });

    const envelopeResult = validateWorkerEnvelope(delivery.envelope);

    if (!envelopeResult.ok) {
      const issues = envelopeResult.issues.map(toRuntimeValidationIssue);
      await emitInvalid(
        options.telemetry,
        undefined,
        issues,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return {
        action: "dlq",
        reason: "invalid-envelope",
        issues
      };
    }

    const envelope = envelopeResult.value;

    if (envelope.route !== "translation") {
      const issues = [
        {
          path: "$.route",
          code: "stage-mismatch",
          message: `Envelope route ${envelope.route} does not match processor stage translation.`
        }
      ];
      await emitInvalid(
        options.telemetry,
        envelope,
        issues,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return terminalResult(envelope, "stage-mismatch", issues);
    }

    const payloadResult = validateStagePayload(delivery.payload);

    if (!payloadResult.ok) {
      const issues = payloadResult.issues.map(toRuntimeValidationIssue);
      await emitInvalid(
        options.telemetry,
        envelope,
        issues,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return terminalResult(envelope, "invalid-payload", issues);
    }

    if (payloadResult.definition.consumer !== "translation") {
      const issues = [
        {
          path: "$.schemaId",
          code: "payload-consumer-mismatch",
          message: `Payload schema consumer ${payloadResult.definition.consumer} does not match translation.`
        }
      ];
      await emitInvalid(
        options.telemetry,
        envelope,
        issues,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return terminalResult(envelope, "payload-consumer-mismatch", issues);
    }

    let claim: RuntimeIdempotencyClaimResult;

    try {
      claim = await options.dependencies.stateStore.claim(envelope.idempotencyKey, {
        envelope,
        stage: "translation",
        receivedAt
      });
    } catch {
      return completeWithRetryOrDlq(
        options.telemetry,
        envelope,
        "idempotency-claim-error",
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );
    }

    if (claim.status === "already-completed") {
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.message.duplicate",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "translation",
        ...envelopeTelemetryFields(envelope, queue, elapsedMs(options.dependencies.clock, startedAtMs)),
        outcome: "duplicate",
        attributes: {
          firstSeenAt: claim.firstSeenAt,
          completedAt: claim.completedAt
        }
      });

      return {
        action: "ack",
        reason: "duplicate",
        envelope
      };
    }

    if (claim.status === "in-progress") {
      const result = retryOrDlq(envelope, "idempotency-in-progress", 1_000);
      await emitRetryOrDlq(
        options.telemetry,
        result,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );

      return result;
    }

    const context: RuntimeMessageContext = {
      envelope,
      payload: payloadResult.value,
      stage: "translation",
      receivedAt
    };

    let result: Awaited<ReturnType<TranslationInputProcessorOptions["handler"]>>;

    try {
      result = await options.handler(context);
    } catch (error: unknown) {
      try {
        await markFailed(options.dependencies.stateStore, envelope, classifyHandlerError(error), true, options.dependencies.clock);
      } catch {
        return completeWithRetryOrDlq(
          options.telemetry,
          envelope,
          "idempotency-mark-failed-error",
          options.dependencies.clock,
          queue,
          elapsedMs(options.dependencies.clock, startedAtMs)
        );
      }

      return completeWithRetryOrDlq(
        options.telemetry,
        envelope,
        "handler-error",
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );
    }

    if (result.status === "ok") {
      try {
        await markCompleted(options.dependencies.stateStore, envelope, options.dependencies.clock);
      } catch {
        await markFailedBestEffort(
          options.dependencies.stateStore,
          envelope,
          "idempotency-mark-completed-error",
          options.dependencies.clock
        );

        return completeWithRetryOrDlq(
          options.telemetry,
          envelope,
          "idempotency-mark-completed-error",
          options.dependencies.clock,
          queue,
          elapsedMs(options.dependencies.clock, startedAtMs)
        );
      }

      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.message.accepted",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "translation",
        ...envelopeTelemetryFields(envelope, queue, elapsedMs(options.dependencies.clock, startedAtMs)),
        outcome: "success"
      });

      return {
        action: "ack",
        reason: "handled",
        envelope
      };
    }

    if (result.status === "retry") {
      try {
        await markFailed(options.dependencies.stateStore, envelope, result.reason, true, options.dependencies.clock);
      } catch {
        return completeWithRetryOrDlq(
          options.telemetry,
          envelope,
          "idempotency-mark-failed-error",
          options.dependencies.clock,
          queue,
          elapsedMs(options.dependencies.clock, startedAtMs)
        );
      }

      return completeWithRetryOrDlq(
        options.telemetry,
        envelope,
        result.reason,
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs),
        result.retryAfterMs
      );
    }

    try {
      await markFailed(options.dependencies.stateStore, envelope, result.reason, false, options.dependencies.clock);
    } catch {
      return completeWithRetryOrDlq(
        options.telemetry,
        envelope,
        "idempotency-mark-failed-error",
        options.dependencies.clock,
        queue,
        elapsedMs(options.dependencies.clock, startedAtMs)
      );
    }

    const processingResult = terminalResult(envelope, result.reason);
    await emitRetryOrDlq(
      options.telemetry,
      processingResult,
      options.dependencies.clock,
      queue,
      elapsedMs(options.dependencies.clock, startedAtMs)
    );

    return processingResult;
  };
}

function livenessCheck(): RuntimeHealthCheck {
  return {
    name: "process",
    critical: true,
    check: () => "ok"
  };
}

function startupCheck(isStarted: () => boolean): RuntimeHealthCheck {
  return {
    name: "service-started",
    critical: true,
    check: () => isStarted() ? "ok" : "unhealthy"
  };
}

function brokerReadinessCheck(broker: BrokerLifecycle): RuntimeHealthCheck {
  return {
    name: "broker-lifecycle",
    critical: true,
    check: () => broker.state === "ready"
      ? {
          status: "ok",
          details: {
            state: broker.state
          }
        }
      : {
          status: "unhealthy",
          details: {
            state: broker.state
          }
        }
  };
}

function dependencyReadinessCheck(
  name: string,
  dependency: {
    readonly name: string;
    probe(): TranslationDependencyProbe | Promise<TranslationDependencyProbe>;
  }
): RuntimeHealthCheck {
  return {
    name,
    critical: true,
    check: async () => {
      const probe = await dependency.probe();

      return {
        status: probe.status,
        details: {
          dependency: dependency.name,
          summary: probe.summary
        }
      };
    }
  };
}

function shadowModeCheck(config: TranslationConfig): RuntimeHealthCheck {
  return {
    name: "shadow-mode",
    critical: true,
    check: () => config.shadowMode
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: "shadow-mode-disabled"
          }
        }
  };
}

function retryOrDlq(
  envelope: WorkerMessageEnvelope,
  reason: string,
  retryAfterMs?: number
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.count);

  if ("ttlMs" in destination) {
    if (retryAfterMs === undefined) {
      return {
        action: "retry",
        reason,
        envelope,
        destination
      };
    }

    return {
      action: "retry",
      reason,
      envelope,
      destination,
      retryAfterMs
    };
  }

  return {
    action: "dlq",
    reason,
    envelope,
    destination
  };
}

function terminalResult(
  envelope: WorkerMessageEnvelope,
  reason: string,
  issues?: readonly RuntimeValidationIssue[]
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.max);

  if (issues === undefined) {
    return {
      action: "dlq",
      reason,
      envelope,
      destination
    };
  }

  return {
    action: "dlq",
    reason,
    envelope,
    destination,
    issues
  };
}

function toRuntimeValidationIssue(issue: StagePayloadValidationIssue | RuntimeValidationIssue): RuntimeValidationIssue {
  return {
    path: issue.path,
    code: issue.code,
    message: issue.message
  };
}

async function markCompleted(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  clock: TranslationDependencies["clock"]
): Promise<void> {
  await store.markCompleted(envelope.idempotencyKey, {
    completedAt: runtimeNow(clock),
    messageId: envelope.messageId,
    stage: "translation"
  });
}

async function markFailed(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  reason: string,
  retryable: boolean,
  clock: TranslationDependencies["clock"]
): Promise<void> {
  await store.markFailed(envelope.idempotencyKey, {
    failedAt: runtimeNow(clock),
    messageId: envelope.messageId,
    stage: "translation",
    reason,
    retryable
  });
}

async function markFailedBestEffort(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  reason: string,
  clock: TranslationDependencies["clock"]
): Promise<void> {
  try {
    await markFailed(store, envelope, reason, true, clock);
  } catch {
    // The delivery still follows the bounded retry/DLQ path below.
  }
}

async function completeWithRetryOrDlq(
  telemetry: RuntimeTelemetrySink | undefined,
  envelope: WorkerMessageEnvelope,
  reason: string,
  clock: TranslationDependencies["clock"],
  queue: string,
  durationMs: number,
  retryAfterMs?: number
): Promise<RuntimeMessageProcessingResult> {
  const processingResult = retryOrDlq(envelope, reason, retryAfterMs);
  await emitRetryOrDlq(telemetry, processingResult, clock, queue, durationMs);

  return processingResult;
}

function classifyHandlerError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-handler-error";
}

async function emitInvalid(
  telemetry: RuntimeTelemetrySink | undefined,
  envelope: WorkerMessageEnvelope | undefined,
  issues: readonly RuntimeValidationIssue[],
  clock: TranslationDependencies["clock"],
  queue: string,
  durationMs: number
): Promise<void> {
  const firstIssue = issues[0];
  const attributes = firstIssue === undefined
    ? undefined
    : {
        issueCode: firstIssue.code,
        issuePath: firstIssue.path
      };

  await emitRuntimeTelemetry(telemetry, {
    name: "runtime.message.invalid",
    level: "warn",
    at: runtimeNow(clock),
    stage: "translation",
    queue,
    durationMs,
    outcome: "failure",
    ...(envelope === undefined
      ? {}
      : envelopeTelemetryFields(envelope, queue, durationMs)),
    ...(attributes === undefined
      ? {}
      : {
          attributes
        })
  });
}

async function emitRetryOrDlq(
  telemetry: RuntimeTelemetrySink | undefined,
  result: RuntimeMessageProcessingResult,
  clock: TranslationDependencies["clock"],
  queue: string,
  durationMs: number
): Promise<void> {
  if (result.action === "retry") {
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.retry",
      level: "warn",
      at: runtimeNow(clock),
      stage: "translation",
      ...envelopeTelemetryFields(result.envelope, queue, durationMs),
      outcome: "retry",
      attributes: {
        reason: result.reason,
        destination: result.destination.name
      }
    });

    return;
  }

  if (result.action === "dlq") {
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.dlq",
      level: "error",
      at: runtimeNow(clock),
      stage: "translation",
      ...(result.envelope === undefined
        ? {}
        : envelopeTelemetryFields(result.envelope, queue, durationMs)),
      outcome: "dlq",
      attributes: {
        reason: result.reason,
        destination: result.destination?.name ?? "unroutable"
      }
    });
  }
}

function elapsedMs(clock: TranslationDependencies["clock"], startedAtMs: number): number {
  return Math.max(0, clock.now().getTime() - startedAtMs);
}

function envelopeTelemetryFields(
  envelope: WorkerMessageEnvelope,
  queue: string,
  durationMs: number
): Readonly<Record<string, string | number>> {
  const base = {
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    traceparent: envelope.traceparent,
    idempotencyKey: envelope.idempotencyKey,
    queue,
    attempt: envelope.attempt.count,
    durationMs
  } as const;

  if (envelope.tracestate === undefined) {
    return base;
  }

  return {
    ...base,
    tracestate: envelope.tracestate
  };
}
