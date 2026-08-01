import type {
  RuntimeTelemetryEvent,
  RuntimeTelemetryFlusher,
  RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export function bestEffortTelemetrySink(
  sink: RuntimeTelemetrySink | undefined
): RuntimeTelemetrySink | undefined {
  if (sink === undefined) {
    return undefined;
  }

  return {
    emit: (event) => emitTelemetryBestEffort(sink, event)
  };
}

export function combineBestEffortTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        await emitTelemetryBestEffort(sink, event);
      }
    }
  };
}

export async function emitTelemetryBestEffort(
  sink: RuntimeTelemetrySink | undefined,
  event: RuntimeTelemetryEvent
): Promise<void> {
  try {
    await sink?.emit(event);
  } catch {
    // Telemetry is deliberately non-semantic and cannot affect delivery state.
  }
}

export function bestEffortTelemetryFlusher(
  flusher: RuntimeTelemetryFlusher | undefined
): RuntimeTelemetryFlusher | undefined {
  if (flusher === undefined) {
    return undefined;
  }

  return {
    flush: async () => {
      try {
        await flusher.flush();
      } catch {
        // A failed flush must not prevent the bounded shutdown sequence.
      }
    }
  };
}

export function runTelemetryBestEffort(operation: () => unknown): void {
  try {
    const result = operation();

    if (isPromiseLike(result)) {
      void result.then(undefined, () => undefined);
    }
  } catch {
    // Telemetry is deliberately non-semantic and cannot affect delivery state.
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (typeof value === "object" || typeof value === "function")
    && value !== null
    && "then" in value
    && typeof value.then === "function";
}
