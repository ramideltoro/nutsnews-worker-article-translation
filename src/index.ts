import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadTranslationConfig,
  type TranslationConfig
} from "./config.js";
import { createTranslationHttpServer } from "./http.js";
import { createTranslationPrometheusMetricsSink } from "./metrics.js";
import { createTranslationService } from "./service.js";
import { createLocalTranslationDependencies } from "./test-doubles.js";
import { createArticleTranslationWorkHandler } from "./translation.js";

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
  createTranslationPrometheusMetricsSink,
  type TranslationPrometheusMetricsSink
} from "./metrics.js";
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
  type ArticleTranslationWorkHandlerOptions
} from "./translation.js";

export interface TranslationApplication {
  readonly config: TranslationConfig;
  start(): Promise<void>;
  stop(): Promise<void>;
}

export function createTranslationApplication(config = loadTranslationConfig()): TranslationApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host
  };
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
        identity
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const baseDependencies = createLocalTranslationDependencies({
    clock: SYSTEM_RUNTIME_CLOCK
  });
  const dependencies = {
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
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        await httpServer.close();
      },
      async () => {
        await service.stop();
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: logSink
    })
  });

  return {
    config,
    async start(): Promise<void> {
      assertPackageCompatibility();
      await service.start();
      await httpServer.listen();
      shutdown.start();
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
}

function combineTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        await sink.emit(event);
      }
    }
  };
}

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.4.0") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== "0.4.0") {
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
