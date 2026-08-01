import type {
  RuntimeTelemetryEvent,
  RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { combineBestEffortTelemetrySinks } from "../src/telemetry.js";

describe("best-effort telemetry fan-out", () => {
  it("isolates synchronous throws and rejected promises so later sinks still receive one event", async () => {
    const event: RuntimeTelemetryEvent = {
      name: "runtime.message.started",
      level: "info",
      at: "2026-08-01T00:00:00.000Z",
      stage: "translation",
      queue: "nutsnews.worker.translation.v1",
      outcome: "started"
    };
    let synchronousCalls = 0;
    let asynchronousCalls = 0;
    let recorderCalls = 0;
    const synchronousFailure: RuntimeTelemetrySink = {
      emit: () => {
        synchronousCalls += 1;
        throw new Error("synchronous telemetry failure");
      }
    };
    const asynchronousFailure: RuntimeTelemetrySink = {
      emit: () => {
        asynchronousCalls += 1;
        return Promise.reject(new Error("asynchronous telemetry failure"));
      }
    };
    const received: RuntimeTelemetryEvent[] = [];
    const recorder: RuntimeTelemetrySink = {
      emit: (receivedEvent) => {
        recorderCalls += 1;
        received.push(receivedEvent);
        return Promise.resolve();
      }
    };
    const combined = combineBestEffortTelemetrySinks(
      synchronousFailure,
      asynchronousFailure,
      recorder
    );

    await expect(combined?.emit(event)).resolves.toBeUndefined();
    expect(synchronousCalls).toBe(1);
    expect(asynchronousCalls).toBe(1);
    expect(recorderCalls).toBe(1);
    expect(received).toEqual([
      event
    ]);
  });
});
