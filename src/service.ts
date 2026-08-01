import {
  getWorkerRoute
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createBrokerConsumerReadinessCheck,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  createRuntimeMessageProcessor,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type RuntimeHealthCheck,
  type RuntimeHealthProbeSet,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { TranslationConfig } from "./config.js";
import type {
  TranslationDependencies,
  TranslationDependencyProbe
} from "./dependencies.js";
import type {
  TranslationRuntimeMetricsSink
} from "./metrics.js";
import { withTranslationPublishSignal } from "./production.js";
import {
  bestEffortTelemetrySink,
  runTelemetryBestEffort
} from "./telemetry.js";

export const TRANSLATION_PROCESSING_DEADLINE_MS = 210_000;

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
  const processor = createRuntimeMessageProcessor({
    stage: "translation",
    idempotencyStore: options.dependencies.stateStore,
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    handler: async (context) => {
      const deadlineController = new AbortController();
      const deadline = setTimeout(() => {
        const error = new Error("Translation processing deadline exceeded.");
        error.name = "TimeoutError";
        deadlineController.abort(error);
      }, TRANSLATION_PROCESSING_DEADLINE_MS);
      deadline.unref();
      const signal = deadlineController.signal;

      try {
        return await drain.track(async () => {
          signal.throwIfAborted();
          const dependencyStartedAtMs = options.dependencies.clock.now().getTime();
          const result = await options.dependencies.workHandler.handle(context, {
            publish: async (command) => {
              signal.throwIfAborted();
              const receipt = await broker.publish(withTranslationPublishSignal(command, signal));
              signal.throwIfAborted();

              return receipt;
            },
            recordOutbox: async (command, receipt) => {
              signal.throwIfAborted();
              await options.dependencies.brokerOutbox.record(command, receipt);
              signal.throwIfAborted();
            },
            withTransaction: async (operation) => {
              signal.throwIfAborted();
              const value = await options.dependencies.transactionRunner.withTransaction(
                operation,
                signal
              );
              signal.throwIfAborted();

              return value;
            }
          }, signal);
          signal.throwIfAborted();

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
        clearTimeout(deadline);
      }
    }
  });
  let started = false;
  let consumer: BrokerConsumerHandle | undefined;
  let transportEmitsConsumerTelemetry = false;
  let lastFallbackConsumerState: string | undefined;
  const emitFallbackConsumerState = async (
    status: ReturnType<BrokerLifecycle["consumerStatus"]>
  ): Promise<void> => {
    const state = `${status.state}:${String(status.activeConsumers)}`;

    if (state === lastFallbackConsumerState) {
      return;
    }

    await emitConsumerStateBestEffort(telemetry, options.dependencies, status);
    lastFallbackConsumerState = state;
  };

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      return createRuntimeHealthProbeSet({
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
      const consumerStatus = broker.consumerStatus("translation");
      transportEmitsConsumerTelemetry = consumerStatus.reason !== "transport-status-unavailable";

      if (!transportEmitsConsumerTelemetry) {
        await emitFallbackConsumerState(consumerStatus);
      }

      consumer = {
        stage: brokerConsumer.stage,
        cancel: async () => {
          await brokerConsumer.cancel();

          if (!transportEmitsConsumerTelemetry) {
            await emitFallbackConsumerState(broker.consumerStatus("translation"));
          }

          await refreshHealthBestEffort(() => service.health.readiness());
        }
      };
      started = true;
      await refreshHealthBestEffort(async () => {
        await Promise.all([
          service.health.startup(),
          service.health.readiness()
        ]);
      });
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      setShutdownDraining(options.metrics, true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");

      if (!transportEmitsConsumerTelemetry) {
        await emitFallbackConsumerState(broker.consumerStatus("translation"));
      }

      consumer = undefined;
      started = false;
      setShutdownDraining(options.metrics, false);
      await refreshHealthBestEffort(async () => {
        await Promise.all([
          service.health.startup(),
          service.health.readiness()
        ]);
      });
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies TranslationService;

  return service;
}

function setShutdownDraining(
  metrics: TranslationRuntimeMetricsSink | undefined,
  draining: boolean
): void {
  runTelemetryBestEffort(() => metrics?.setShutdownDraining(draining));
}

async function refreshHealthBestEffort(
  operation: () => Promise<unknown>
): Promise<void> {
  try {
    await operation();
  } catch {
    // Health evaluation is observational and must not change lifecycle outcomes.
  }
}

async function emitConsumerStateBestEffort(
  telemetry: RuntimeTelemetrySink | undefined,
  dependencies: TranslationDependencies,
  status: ReturnType<BrokerLifecycle["consumerStatus"]>
): Promise<void> {
  await emitRuntimeTelemetry(telemetry, {
    name: "runtime.broker.consumer_state_changed",
    level: status.activeConsumers > 0 ? "info" : "error",
    at: runtimeNow(dependencies.clock),
    stage: status.stage,
    queue: status.queue,
    outcome: status.state,
    attributes: {
      activeConsumers: status.activeConsumers,
      state: status.state,
      reason: status.reason
    }
  });
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

function elapsedMs(clock: TranslationDependencies["clock"], startedAtMs: number): number {
  return Math.max(0, clock.now().getTime() - startedAtMs);
}
