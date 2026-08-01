import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadTranslationConfig,
  type TranslationConfig
} from "./config.js";
import type { TranslationDependencies } from "./dependencies.js";
import { createTranslationHttpServer } from "./http.js";
import { createTranslationPrometheusMetricsSink } from "./metrics.js";
import {
  bestEffortTelemetryFlusher,
  combineBestEffortTelemetrySinks
} from "./telemetry.js";
import { createProductionTranslationDependencies } from "./production.js";
import { createTranslationService } from "./service.js";
import { createLocalTranslationDependencies } from "./test-doubles.js";
import { createArticleTranslationWorkHandler } from "./translation.js";
import type { TranslationReconciler } from "./reconciliation.js";

export {
  TRANSLATION_CONFIG_SCHEMA,
  TRANSLATION_SERVICE_NAME,
  TRANSLATION_SERVICE_VERSION,
  TranslationConfigError,
  loadTranslationConfig,
  type TranslationConfig
} from "./config.js";
export type {
  TranslationBrokerOutbox,
  TranslationDatabaseTransaction,
  TranslationDatabaseTransactionRunner,
  TranslationDependencies,
  TranslationDependencyProbe,
  TranslationLanguagePolicy,
  TranslationLanguagePolicySnapshot,
  TranslationLanguageResultKey,
  TranslationPersistencePublication,
  TranslationPrompt,
  TranslationPromptRegistry,
  TranslationQualityValidator,
  TranslationQualityValidationRequest,
  TranslationQualityValidationResult,
  TranslationQwenClient,
  TranslationQwenRequest,
  TranslationStateStore,
  TranslationStoredLanguageResult,
  TranslationWorkHandler,
  TranslationWorkTools
} from "./dependencies.js";
export {
  TranslationQwenError
} from "./dependencies.js";
export {
  createTranslationHttpServer,
  type TranslationHttpServer
} from "./http.js";
export {
  TRANSLATION_RECONCILIATION_CONFIRMATION,
  TRANSLATION_RECONCILIATION_PATH,
  type TranslationReconciliationCandidate,
  type TranslationReconciliationReport,
  type TranslationReconciliationRequest,
  type TranslationReconciler
} from "./reconciliation.js";
export {
  TRANSLATION_DURATION_BUCKETS_SECONDS,
  createTranslationPrometheusMetricsSink,
  type TranslationHealthOutcome,
  type TranslationHealthProbe,
  type TranslationPrometheusMetricsSink,
  type TranslationPrometheusMetricsSinkOptions,
  type TranslationRuntimeMetricsSink
} from "./metrics.js";
export {
  LocalAiTranslationQwenClient,
  PayloadRabbitMqTransport,
  PostgresTranslationBrokerOutbox,
  PostgresTranslationOutboxReconciler,
  PostgresTranslationStateStore,
  PostgresTranslationTransactionRunner,
  StaticTranslationLanguagePolicy,
  StaticTranslationPromptRegistry,
  createProductionTranslationDependencies,
  type ProductionTranslationDependencies
} from "./production.js";
export {
  createTranslationService,
  type TranslationService
} from "./service.js";
export {
  InMemoryTranslationStateStore,
  LocalTranslationBrokerOutbox,
  LocalTranslationLanguagePolicy,
  LocalTranslationPromptRegistry,
  LocalTranslationQualityValidator,
  LocalTranslationQwenClient,
  LocalTranslationTransactionRunner,
  LocalTranslationWorkHandler,
  LocalBrokerTransport,
  ManualTranslationClock,
  createLocalTranslationDependencies,
  createMinimalTranslationDelivery,
  createMinimalTranslationEnvelope,
  createMinimalTranslationPayload
} from "./test-doubles.js";
export {
  createArticleTranslationWorkHandler,
  publishTranslationBacklogRecoveryTask,
  type ArticleTranslationWorkHandlerOptions,
  type TranslationBacklogRecoveryResult,
  type TranslationTaskInput
} from "./translation.js";

export interface TranslationApplication {
  readonly config: TranslationConfig;
  start(): Promise<void>;
  stop(): Promise<void>;
  url(path?: string): string;
}

export interface TranslationApplicationOptions {
  readonly dependencies?: TranslationDependencies;
}

export function createTranslationApplication(
  config = loadTranslationConfig(),
  options: TranslationApplicationOptions = {}
): TranslationApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host,
    revision: config.buildRevision,
    deployment: config.dependencyMode === "production"
      ? "shadow"
      : config.environment === "test" ? "test" : "local",
    adapter: config.dependencyMode === "production" ? "production" : "in_memory"
  } as const;
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createTranslationPrometheusMetricsSink({
        identity,
        allowedLanguages: config.languagePolicy.targetLanguages
      })
    : undefined;
  const telemetry = combineBestEffortTelemetrySinks(logSink, metrics);
  const telemetryFlusher = bestEffortTelemetryFlusher(logSink);
  const baseDependencies = options.dependencies ?? (config.dependencyMode === "production"
    ? createProductionTranslationDependencies({
        config,
        clock: SYSTEM_RUNTIME_CLOCK,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      })
    : createLocalTranslationDependencies({
        clock: SYSTEM_RUNTIME_CLOCK
      }));
  const dependencies = options.dependencies ?? {
    ...baseDependencies,
    workHandler: createArticleTranslationWorkHandler({
      config,
      dependencies: baseDependencies,
      ...(telemetry === undefined ? {} : {
        telemetry
      })
    })
  };
  const service = createTranslationService({
    config,
    dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const httpServer = createTranslationHttpServer({
    config,
    service,
    ...(hasReconciler(baseDependencies) ? {
      reconciler: baseDependencies.reconciler
    } : {}),
    ...(hasReconciliationToken(baseDependencies) ? {
      reconciliationToken: baseDependencies.reconciliationToken
    } : {}),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  let startPromise: Promise<void> | undefined;
  let listenerBound = false;
  let started = false;
  let stopped = false;
  let stopRequested = false;
  let dependenciesClosed = false;
  const isStopRequested = (): boolean => stopRequested;
  const closeListener = async (): Promise<void> => {
    if (!listenerBound) {
      return;
    }

    try {
      await httpServer.close();
    } finally {
      listenerBound = false;
    }
  };
  const closeDependencies = async (): Promise<void> => {
    if (dependenciesClosed || !hasDependencyCloser(baseDependencies)) {
      return;
    }

    dependenciesClosed = true;
    await baseDependencies.close();
  };
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        stopRequested = true;
        await closeListener();
      },
      async () => {
        await service.stop();
      },
      closeDependencies
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(telemetryFlusher === undefined ? {} : {
      telemetryFlusher
    })
  });

  return {
    config,
    async start(): Promise<void> {
      if (started) {
        return;
      }

      if (stopped) {
        throw new Error("Translation application cannot be restarted after shutdown.");
      }

      if (startPromise !== undefined) {
        await startPromise;
        return;
      }

      const operation = (async () => {
        assertPackageCompatibility();

        try {
          await httpServer.listen();
          listenerBound = true;
          shutdown.start();

          if (isStopRequested()) {
            await shutdown.trigger("manual");
            throw new Error("Translation application startup was interrupted by shutdown.");
          }

          await service.start();

          if (isStopRequested()) {
            await cleanupBestEffort(() => service.stop());
            throw new Error("Translation application startup was interrupted by shutdown.");
          }

          started = true;
        } catch (error: unknown) {
          shutdown.stop();
          await cleanupBestEffort(closeListener);
          await cleanupBestEffort(() => service.stop());
          await cleanupBestEffort(closeDependencies);
          stopped = true;

          throw error;
        }
      })();

      startPromise = operation;

      try {
        await operation;
      } finally {
        startPromise = undefined;
      }
    },
    async stop(): Promise<void> {
      if (stopped || (!started && startPromise === undefined)) {
        return;
      }

      stopRequested = true;

      try {
        if (shutdown.isStarted) {
          await shutdown.trigger("manual");
        }
      } finally {
        started = false;
        stopped = true;
        startPromise = undefined;
      }
    },
    url: (path) => httpServer.url(path)
  };
}

async function cleanupBestEffort(operation: () => Promise<void>): Promise<void> {
  try {
    await operation();
  } catch {
    // Startup cleanup cannot replace the original startup failure.
  }
}

function hasDependencyCloser(
  dependencies: TranslationDependencies
): dependencies is TranslationDependencies & { readonly close: () => Promise<void> } {
  const candidate = dependencies as Partial<{ readonly close: unknown }>;

  return typeof candidate.close === "function";
}

function hasReconciler(
  dependencies: TranslationDependencies
): dependencies is TranslationDependencies & { readonly reconciler: TranslationReconciler } {
  const candidate = dependencies as Partial<{ readonly reconciler: unknown }>;

  return typeof candidate.reconciler === "object" && candidate.reconciler !== null;
}

function hasReconciliationToken(
  dependencies: TranslationDependencies
): dependencies is TranslationDependencies & { readonly reconciliationToken: string } {
  const candidate = dependencies as Partial<{ readonly reconciliationToken: unknown }>;

  return typeof candidate.reconciliationToken === "string" && candidate.reconciliationToken.length > 0;
}

export const SUPPORTED_RUNTIME_PACKAGE_VERSION = "0.5.0";

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.4.0") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== SUPPORTED_RUNTIME_PACKAGE_VERSION) {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createTranslationApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start translation");
    process.exitCode = 1;
  });
}
