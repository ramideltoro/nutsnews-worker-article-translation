import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadTranslationConfig } from "../src/config.js";
import { createTranslationApplication } from "../src/index.js";
import {
  LocalBrokerTransport,
  createLocalTranslationDependencies
} from "../src/test-doubles.js";

describe("createTranslationApplication", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("serves health and metrics while broker initialization is pending, then starts and stops idempotently", async () => {
    const config = applicationConfig();
    const dependencies = createLocalTranslationDependencies();
    const transport = dependencies.brokerTransport as LocalBrokerTransport;
    const originalConnect = transport.connect.bind(transport);
    const connectEntered = deferredSignal();
    const connectRelease = deferredSignal();
    const connect = vi.spyOn(transport, "connect").mockImplementation(async () => {
      connectEntered.resolve();
      await connectRelease.promise;
      await originalConnect();
    });
    const application = createTranslationApplication(config, {
      dependencies
    });
    const starting = application.start();
    const duplicateStart = application.start();

    await connectEntered.promise;

    try {
      const live = await fetch(application.url("/livez"));
      const startup = await fetch(application.url("/startupz"));
      const ready = await fetch(application.url("/readyz"));
      const metrics = await fetch(application.url("/metrics"));

      expect(live.status).toBe(200);
      await expect(live.json()).resolves.toMatchObject({
        status: "ok"
      });
      expect(startup.status).toBe(503);
      await expect(startup.json()).resolves.toMatchObject({
        status: "unhealthy"
      });
      expect(ready.status).toBe(503);
      await expect(ready.json()).resolves.toMatchObject({
        status: "unhealthy"
      });
      expect(metrics.status).toBe(200);
      const metricsBody = await metrics.text();
      expect(metricsBody).toContain("nutsnews_worker_health_probe");
      expect(metricsBody.split("\n").some((line) => line.includes('probe="startup"') && line.includes('outcome="unhealthy"') && line.endsWith(" 1"))).toBe(true);

      connectRelease.resolve();
      await Promise.all([
        starting,
        duplicateStart
      ]);
      await expect(application.start()).resolves.toBeUndefined();

      expect(connect).toHaveBeenCalledTimes(1);
      expect((await fetch(application.url("/startupz"))).status).toBe(200);
      expect((await fetch(application.url("/readyz"))).status).toBe(200);

      await expect(application.stop()).resolves.toBeUndefined();
      await expect(application.stop()).resolves.toBeUndefined();
      expect(() => application.url("/livez")).toThrow("not listening");
    } finally {
      connectRelease.resolve();
      await Promise.all([
        starting.catch(() => undefined),
        duplicateStart.catch(() => undefined)
      ]);
      await application.stop();
    }
  });

  it("closes the listener, removes signal handlers, and preserves a startup rejection", async () => {
    const config = applicationConfig();
    const dependencies = createLocalTranslationDependencies();
    const transport = dependencies.brokerTransport as LocalBrokerTransport;
    const startupError = new Error("broker initialization failed");
    const connectEntered = deferredSignal();
    const connectRelease = deferredSignal();
    const signalListenerCounts = listenerCounts();
    vi.spyOn(transport, "connect").mockImplementation(async () => {
      connectEntered.resolve();
      await connectRelease.promise;
      throw startupError;
    });
    const application = createTranslationApplication(config, {
      dependencies
    });
    const starting = application.start();

    await connectEntered.promise;
    expect((await fetch(application.url("/livez"))).status).toBe(200);
    connectRelease.resolve();

    await expect(starting).rejects.toBe(startupError);
    expect(() => application.url("/livez")).toThrow("not listening");
    expect(listenerCounts()).toEqual(signalListenerCounts);
    await expect(application.stop()).resolves.toBeUndefined();
  });

  it("closes health endpoints when shutdown is requested during pending startup", async () => {
    const config = applicationConfig();
    const dependencies = createLocalTranslationDependencies();
    const transport = dependencies.brokerTransport as LocalBrokerTransport;
    const originalConnect = transport.connect.bind(transport);
    const connectEntered = deferredSignal();
    const connectRelease = deferredSignal();
    const signalListenerCounts = listenerCounts();
    vi.spyOn(transport, "connect").mockImplementation(async () => {
      connectEntered.resolve();
      await connectRelease.promise;
      await originalConnect();
    });
    const application = createTranslationApplication(config, {
      dependencies
    });
    const starting = application.start();

    await connectEntered.promise;
    await expect(application.stop()).resolves.toBeUndefined();
    expect(() => application.url("/livez")).toThrow("not listening");
    expect(listenerCounts()).toEqual(signalListenerCounts);

    connectRelease.resolve();
    await expect(starting).rejects.toThrow("startup was interrupted by shutdown");
    await expect(application.stop()).resolves.toBeUndefined();
  });
});

function applicationConfig() {
  return loadTranslationConfig({
    HOSTNAME: "translation-application-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_TRANSLATION_HTTP_HOST: "127.0.0.1",
    NUTSNEWS_TRANSLATION_HTTP_PORT: "0",
    NUTSNEWS_TRANSLATION_TELEMETRY_LOGS: "silent"
  });
}

function deferredSignal() {
  let resolvePromise!: () => void;
  let rejectPromise!: (reason?: unknown) => void;
  const promise = new Promise<void>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });

  return {
    promise,
    resolve: resolvePromise,
    reject: rejectPromise
  };
}

function listenerCounts() {
  return {
    SIGINT: process.listenerCount("SIGINT"),
    SIGTERM: process.listenerCount("SIGTERM")
  };
}
