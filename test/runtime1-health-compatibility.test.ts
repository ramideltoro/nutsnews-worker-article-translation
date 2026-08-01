import {
  describe,
  expect,
  it
} from "vitest";

import { createTranslationPrometheusMetricsSink } from "../src/metrics.js";

describe("Runtime 1 metric ownership", () => {
  it("exports Runtime-owned health, activity, consumer, and freshness families exactly once", async () => {
    const metrics = createTranslationPrometheusMetricsSink({
      identity: {
        service: "nutsnews-worker-article-translation",
        version: "0.1.0",
        environment: "test",
        host: "translation-test"
      },
      expectedActive: true
    });

    expect(sampleValue(metrics.collect(), "nutsnews_worker_expected_active")).toBe(1);
    expect(sampleValue(metrics.collect(), "nutsnews_worker_last_success_timestamp_seconds")).toBe(0);

    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-08-01T00:00:00.000Z",
      stage: "translation",
      outcome: "ok",
      attributes: {
        probe: "liveness",
        checks: [
          {
            name: "process",
            status: "ok",
            durationMs: 4
          }
        ]
      }
    });
    await metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "info",
      at: "2026-08-01T00:00:01.000Z",
      stage: "translation",
      queue: "nutsnews.worker.translation.v1",
      outcome: "active",
      attributes: {
        activeConsumers: 1
      }
    });
    await metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-08-01T00:00:02.000Z",
      stage: "translation",
      queue: "nutsnews.worker.translation.v1",
      outcome: "success",
      durationMs: 25
    });

    const output = metrics.collect();

    expectMetricDeclarationOnce(output, "nutsnews_worker_expected_active", "gauge");
    expectMetricDeclarationOnce(output, "nutsnews_worker_last_success_timestamp_seconds", "gauge");
    expectMetricDeclarationOnce(output, "nutsnews_worker_consumers", "gauge");
    expectMetricDeclarationOnce(output, "nutsnews_worker_health_probe", "gauge");
    expectMetricDeclarationOnce(output, "nutsnews_worker_health_check", "gauge");
    expectMetricDeclarationOnce(output, "nutsnews_worker_health_check_duration_seconds", "histogram");
    expect(output.split("\n").filter((line) => line.startsWith("nutsnews_worker_health_probe{"))).toHaveLength(3);
    expect(sampleValue(output, "nutsnews_worker_consumers", {
      queue: "nutsnews.worker.translation.v1"
    })).toBe(1);
    expect(sampleValue(output, "nutsnews_worker_health_probe", {
      outcome: "ok",
      probe: "liveness"
    })).toBe(1);
    expect(sampleValue(output, "nutsnews_worker_health_check", {
      check: "process",
      outcome: "ok",
      probe: "liveness"
    })).toBe(1);
    expect(sampleValue(output, "nutsnews_worker_last_success_timestamp_seconds")).toBe(
      Date.parse("2026-08-01T00:00:02.000Z") / 1_000
    );
    expect(output).not.toContain("nutsnews_worker_consumer_active");
  });
});

function expectMetricDeclarationOnce(output: string, metric: string, type: string): void {
  expect(output.match(new RegExp(`^# HELP ${metric} `, "gmu"))).toHaveLength(1);
  expect(output.match(new RegExp(`^# TYPE ${metric} ${type}$`, "gmu"))).toHaveLength(1);
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
