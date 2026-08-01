import {
  describe,
  expect,
  it,
  vi
} from "vitest";

const runtime1Delegate = vi.hoisted(() => ({
  healthEvents: 0
}));

vi.mock("@ramideltoro/nutsnews-worker-runtime", () => ({
  createPrometheusRuntimeTelemetrySink: () => ({
    allowedLabels: [],
    emit(event: { readonly name: string }): Promise<void> {
      if (event.name === "runtime.health.evaluated") {
        runtime1Delegate.healthEvents += 1;
      }

      return Promise.resolve();
    },
    collect(): string {
      if (runtime1Delegate.healthEvents === 0) {
        return "";
      }

      return [
        "# HELP nutsnews_worker_health_probe Runtime 1 worker health status.",
        "# TYPE nutsnews_worker_health_probe gauge",
        'nutsnews_worker_health_probe{environment="test",host="test",outcome="ok",probe="readiness",service="translation",version="1.0.0"} 1',
        ""
      ].join("\n");
    },
    setInFlight(): void {},
    setShutdownDraining(): void {}
  })
}));

import { createTranslationPrometheusMetricsSink } from "../src/metrics.js";

describe("Runtime 1 health metric compatibility", () => {
  it("keeps one service-owned health-probe family", async () => {
    runtime1Delegate.healthEvents = 0;
    const metrics = createTranslationPrometheusMetricsSink({
      identity: {
        service: "nutsnews-worker-article-translation",
        version: "0.1.0",
        environment: "test",
        host: "translation-test"
      }
    });

    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-08-01T00:00:00.000Z",
      stage: "translation",
      outcome: "ok",
      attributes: {
        probe: "readiness"
      }
    });

    const output = metrics.collect();
    const healthSamples = output
      .split("\n")
      .filter((line) => line.startsWith("nutsnews_worker_health_probe{"));

    expect(runtime1Delegate.healthEvents).toBe(0);
    expect(output.match(/^# HELP nutsnews_worker_health_probe /gmu)).toHaveLength(1);
    expect(output.match(/^# TYPE nutsnews_worker_health_probe gauge$/gmu)).toHaveLength(1);
    expect(healthSamples).toHaveLength(9);
    expect(new Set(healthSamples.map((line) => line.slice(0, line.lastIndexOf(" "))))).toHaveLength(9);
  });
});
