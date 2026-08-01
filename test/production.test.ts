import {
  describe,
  expect,
  it,
  vi
} from "vitest";
import type {
  ChannelModel,
  ConfirmChannel,
  ConsumeMessage
} from "amqplib";
import type { Pool } from "pg";

import { loadTranslationConfig } from "../src/config.js";
import {
  TranslationQwenError,
  type TranslationQwenRequest
} from "../src/dependencies.js";
import {
  LocalAiTranslationQwenClient,
  PayloadRabbitMqTransport,
  PostgresTranslationTransactionRunner,
  createProductionTranslationDependencies,
  withTranslationPublishSignal
} from "../src/production.js";
import { createMinimalTranslationEnvelope } from "../src/test-doubles.js";

const clock = {
  now: () => new Date("2026-07-23T00:00:00.000Z")
};

describe("production translation dependencies", () => {
  it("wires production mode to RabbitMQ, Postgres, local AI, and static policy dependencies", async () => {
    const env = productionEnv();
    const config = loadTranslationConfig({
      ...env,
      NUTSNEWS_TRANSLATION_DEPENDENCY_MODE: "production"
    });
    const dependencies = createProductionTranslationDependencies({
      config,
      clock,
      env
    });

    expect(dependencies.brokerTransport.name).toBe("rabbitmq-payload-transport");
    expect(dependencies.stateStore.name).toBe("postgres-translation-state");
    expect(dependencies.transactionRunner.name).toBe("postgres-translation-transactions");
    expect(dependencies.brokerOutbox.name).toBe("postgres-translation-outbox");
    expect(dependencies.qwenClient.name).toBe("local-ai-translation-client");
    expect(dependencies.promptRegistry.name).toBe("static-translation-prompt-registry");
    expect(dependencies.languagePolicy.name).toBe("static-translation-language-policy");
    await expect(dependencies.languagePolicy.getPolicy()).resolves.toMatchObject({
      requiredLanguageCodes: [
        "fr",
        "ja",
        "de-CH",
        "de",
        "el"
      ],
      perLanguageConcurrency: 1
    });

    await dependencies.close();
  });

  it("maps the legacy local AI translation response into a translation result", async () => {
    const sourceQuery = vi.fn(() => Promise.resolve({
      rowCount: 1,
      rows: [{
        canonical_url: "https://publisher.example.test/community/library",
        title: "Neighbors build a free community library",
        source_summary: "Neighbors created a free library that gives local families easier access to books.",
        category: "Community | Uplifting"
      }]
    }));
    const fetcher = vi.fn((_input: string, _init?: RequestInit) => {
      void _input;
      void _init;
      return Promise.resolve(new Response(JSON.stringify({
        language_code: "fr",
        title: "Bibliotheques de quartier",
        summary: "Des eleves creent des boites a livres pour leur quartier, donnant aux familles un acces plus simple a des lectures positives.",
        prompt_tokens: 120,
        completion_tokens: 38,
        total_tokens: 158,
        duration_ms: 4210
      }), {
        status: 200
      }));
    });
    const client = new LocalAiTranslationQwenClient({
      baseUrl: "https://ai.example.test/",
      apiKey: "local-key",
      clock,
      fetcher,
      sourcePool: {
        query: sourceQuery
      } as unknown as Pool
    });

    await expect(client.translate(translationRequest())).resolves.toMatchObject({
      summary: "Des eleves creent des boites a livres pour leur quartier, donnant aux familles un acces plus simple a des lectures positives.",
      qualityScore: 92,
      latencyMs: 4210,
      usage: {
        inputTokens: 120,
        outputTokens: 38,
        totalTokens: 158
      }
    });
    expect(fetcher).toHaveBeenCalledWith("https://ai.example.test/translate", expect.objectContaining({
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-nutsnews-ai-key": "local-key"
      }
    }));
    const request = fetcher.mock.calls[0]?.[1];

    if (typeof request?.body !== "string") {
      throw new Error("translation request body was not JSON text");
    }

    const body = JSON.parse(request.body) as Readonly<Record<string, unknown>>;

    expect(sourceQuery).toHaveBeenCalledWith(expect.stringContaining("worker_uplift_views.approval_projection"), [
      "article-001",
      1
    ]);
    expect(body).toMatchObject({
      source: "publisher.example.test",
      title: "Neighbors build a free community library",
      summary: "Neighbors created a free library that gives local families easier access to books.",
      category: "Community | Uplifting"
    });
  });

  it("fails closed before calling local AI when the API key is not a valid header", async () => {
    const fetcher = vi.fn();
    const client = new LocalAiTranslationQwenClient({
      baseUrl: "https://ai.example.test",
      apiKey: "bad\nkey",
      clock,
      fetcher
    });

    await expect(client.translate(translationRequest())).rejects.toMatchObject({
      name: "TranslationQwenError",
      reason: "qwen-unauthorized",
      retryable: false
    } satisfies Partial<TranslationQwenError>);
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("rolls back instead of committing a transaction whose processing deadline expired", async () => {
    const queries: string[] = [];
    const release = vi.fn();
    const client = {
      query: vi.fn((sql: string) => {
        queries.push(sql);
        return Promise.resolve({
          rowCount: 0,
          rows: []
        });
      }),
      release
    };
    const pool = {
      connect: vi.fn(() => Promise.resolve(client))
    } as unknown as Pool;
    const runner = new PostgresTranslationTransactionRunner(pool);
    const deadline = new AbortController();
    const deadlineError = new Error("processing deadline exceeded");

    await expect(runner.withTransaction(async () => {
      deadline.abort(deadlineError);
      await Promise.resolve();

      return "must-not-commit";
    }, deadline.signal)).rejects.toBe(deadlineError);
    expect(queries).toEqual([
      "BEGIN",
      "ROLLBACK"
    ]);
    expect(release).toHaveBeenCalledTimes(1);
  });

  it("destroys a Postgres client when rollback rejects", async () => {
    const operationError = new Error("translation operation failed");
    const rollbackError = new Error("database connection lost during rollback");
    const release = vi.fn();
    const client = {
      query: vi.fn((sql: string) => sql === "ROLLBACK"
        ? Promise.reject(rollbackError)
        : Promise.resolve({
            rowCount: 0,
            rows: []
          })),
      release
    };
    const pool = {
      connect: vi.fn(() => Promise.resolve(client))
    } as unknown as Pool;
    const runner = new PostgresTranslationTransactionRunner(pool);

    await expect(runner.withTransaction(() => Promise.reject(operationError))).rejects.toBe(
      operationError
    );
    expect(client.query).toHaveBeenCalledWith("ROLLBACK");
    expect(release).toHaveBeenCalledTimes(1);
    expect(release).toHaveBeenCalledWith(rollbackError);
  });

  it("does not publish when channel creation finishes after the processing deadline aborts", async () => {
    const channelGate = deferred<ConfirmChannel>();
    const channelClose = vi.fn(() => Promise.resolve());
    const publish = vi.fn();
    const channel = {
      close: channelClose,
      publish,
      on: vi.fn(),
      off: vi.fn()
    } as unknown as ConfirmChannel;
    const connectionClose = vi.fn(() => Promise.resolve());
    const createConfirmChannel = vi.fn(() => channelGate.promise);
    const connection = {
      createConfirmChannel,
      close: connectionClose,
      on: vi.fn()
    } as unknown as ChannelModel;
    const connect = vi.fn((url: string) => {
      void url;
      return Promise.resolve(connection);
    });
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://translation:secret@example.invalid:5672",
      prefetch: 1,
      clock,
      connect
    });
    const deadline = new AbortController();
    const deadlineError = new Error("processing deadline exceeded during reconnect");
    const pendingPublish = transport.publish(withTranslationPublishSignal({
      envelope: createMinimalTranslationEnvelope(),
      payload: {}
    }, deadline.signal));

    await settleMicrotasks();
    expect(createConfirmChannel).toHaveBeenCalledTimes(1);
    const rejection = expect(pendingPublish).rejects.toBe(deadlineError);
    deadline.abort(deadlineError);
    await rejection;

    channelGate.resolve(channel);
    await settleMicrotasks();
    expect(publish).not.toHaveBeenCalled();
    expect(connectionClose).toHaveBeenCalledTimes(1);
    expect(channelClose).toHaveBeenCalledTimes(1);
  });

  it("aborts stalled consumer restoration before a reconnect publish and cleans partial state", async () => {
    const firstChannelListeners = new Map<string, (...arguments_: unknown[]) => void>();
    const firstPrefetch = vi.fn(() => Promise.resolve({}));
    const firstConsume = vi.fn(() => Promise.resolve({
      consumerTag: "translation-consumer-1"
    }));
    const firstChannelClose = vi.fn(() => Promise.resolve());
    const firstChannelOn = vi.fn((event: string, listener: (...arguments_: unknown[]) => void) => {
      firstChannelListeners.set(event, listener);
    });
    const firstChannel = {
      prefetch: firstPrefetch,
      consume: firstConsume,
      cancel: vi.fn(() => Promise.resolve({})),
      close: firstChannelClose,
      publish: vi.fn(),
      on: firstChannelOn,
      off: vi.fn()
    } as unknown as ConfirmChannel;

    const firstConnectionClose = vi.fn(() => Promise.resolve());
    const firstConnection = {
      createConfirmChannel: vi.fn(() => Promise.resolve(firstChannel)),
      close: firstConnectionClose,
      on: vi.fn()
    } as unknown as ChannelModel;

    const stalledPrefetch = deferred<unknown>();
    const secondPrefetch = vi.fn(() => stalledPrefetch.promise);
    const secondConsume = vi.fn(() => Promise.resolve({
      consumerTag: "translation-consumer-2"
    }));
    const secondPublish = vi.fn();
    const secondChannelCloseGate = deferred<unknown>();
    const secondChannelClose = vi.fn(() => secondChannelCloseGate.promise);
    const secondChannel = {
      prefetch: secondPrefetch,
      consume: secondConsume,
      cancel: vi.fn(() => Promise.resolve({})),
      close: secondChannelClose,
      publish: secondPublish,
      on: vi.fn(),
      off: vi.fn()
    } as unknown as ConfirmChannel;

    const secondConnectionCloseGate = deferred<unknown>();
    const secondConnectionClose = vi.fn(() => secondConnectionCloseGate.promise);
    const secondConnection = {
      createConfirmChannel: vi.fn(() => Promise.resolve(secondChannel)),
      close: secondConnectionClose,
      on: vi.fn()
    } as unknown as ChannelModel;
    const thirdPrefetch = vi.fn(() => Promise.resolve({}));
    const thirdConsume = vi.fn(() => Promise.resolve({
      consumerTag: "translation-consumer-3"
    }));
    const thirdChannelClose = vi.fn(() => Promise.resolve());
    const thirdChannel = {
      prefetch: thirdPrefetch,
      consume: thirdConsume,
      cancel: vi.fn(() => Promise.resolve({})),
      close: thirdChannelClose,
      publish: vi.fn(),
      on: vi.fn(),
      off: vi.fn()
    } as unknown as ConfirmChannel;
    const thirdConnectionClose = vi.fn(() => Promise.resolve());
    const thirdConnection = {
      createConfirmChannel: vi.fn(() => Promise.resolve(thirdChannel)),
      close: thirdConnectionClose,
      on: vi.fn()
    } as unknown as ChannelModel;
    const connect = vi.fn()
      .mockResolvedValueOnce(firstConnection)
      .mockResolvedValueOnce(secondConnection)
      .mockResolvedValueOnce(thirdConnection);
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://translation:secret@example.invalid:5672",
      prefetch: 1,
      clock,
      connect
    });
    const envelope = createMinimalTranslationEnvelope();

    await transport.connect();
    await transport.consume("translation", () => Promise.resolve({
      action: "ack",
      reason: "handled",
      envelope
    }));
    expect(firstPrefetch).toHaveBeenCalledTimes(1);
    expect(firstConsume).toHaveBeenCalledTimes(1);

    const closeListener = firstChannelListeners.get("close");

    if (closeListener === undefined) {
      throw new Error("Expected the transport to register a channel close listener.");
    }

    closeListener();
    await settleMicrotasks();
    expect(secondPrefetch).toHaveBeenCalledTimes(1);

    const deadline = new AbortController();
    const deadlineError = new Error("processing deadline exceeded during consumer restoration");
    const pendingPublish = transport.publish(withTranslationPublishSignal({
      envelope,
      payload: {}
    }, deadline.signal));
    const rejection = expect(pendingPublish).rejects.toBe(deadlineError);

    deadline.abort(deadlineError);
    await rejection;
    await settleMicrotasks();

    expect(secondPublish).not.toHaveBeenCalled();
    expect(secondConsume).not.toHaveBeenCalled();
    expect(secondChannelClose).toHaveBeenCalledTimes(1);
    expect(secondConnectionClose).toHaveBeenCalledTimes(1);
    expect(firstConnectionClose).toHaveBeenCalledTimes(1);
    expect(transport.consumerStatus("translation")).toMatchObject({
      state: "channel-dropped"
    });

    await transport.connect();
    expect(connect).toHaveBeenCalledTimes(3);
    expect(thirdPrefetch).toHaveBeenCalledTimes(1);
    expect(thirdConsume).toHaveBeenCalledTimes(1);
    expect(transport.consumerStatus("translation")).toMatchObject({
      state: "active"
    });
    await transport.close();
    expect(thirdChannelClose).toHaveBeenCalledTimes(1);
    expect(thirdConnectionClose).toHaveBeenCalledTimes(1);
  });

  it("does not let a late old-channel reactivation failure discard a replacement connection", async () => {
    const channelListeners = new Map<string, (...arguments_: unknown[]) => void>();
    const staleReactivation = deferred<unknown>();
    let deliveryCallback: ((message: ConsumeMessage | null) => void) | undefined;
    const firstPrefetch = vi.fn()
      .mockResolvedValueOnce({})
      .mockImplementationOnce(() => staleReactivation.promise);
    const firstConsume = vi.fn((queue: string, callback: (message: ConsumeMessage | null) => void) => {
      void queue;
      deliveryCallback = callback;
      return Promise.resolve({
        consumerTag: "translation-consumer-old"
      });
    });
    const firstChannelClose = vi.fn(() => Promise.resolve());
    const firstChannel = {
      prefetch: firstPrefetch,
      consume: firstConsume,
      cancel: vi.fn(() => Promise.resolve({})),
      close: firstChannelClose,
      publish: vi.fn(),
      on: vi.fn((event: string, listener: (...arguments_: unknown[]) => void) => {
        channelListeners.set(event, listener);
      }),
      off: vi.fn()
    } as unknown as ConfirmChannel;
    const firstConnectionClose = vi.fn(() => Promise.resolve());
    const firstConnection = {
      createConfirmChannel: vi.fn(() => Promise.resolve(firstChannel)),
      close: firstConnectionClose,
      on: vi.fn()
    } as unknown as ChannelModel;

    const replacementPrefetch = vi.fn(() => Promise.resolve({}));
    const replacementConsume = vi.fn(() => Promise.resolve({
      consumerTag: "translation-consumer-new"
    }));
    const replacementChannelClose = vi.fn(() => Promise.resolve());
    const replacementChannel = {
      prefetch: replacementPrefetch,
      consume: replacementConsume,
      cancel: vi.fn(() => Promise.resolve({})),
      close: replacementChannelClose,
      publish: vi.fn(),
      on: vi.fn(),
      off: vi.fn()
    } as unknown as ConfirmChannel;
    const replacementConnectionClose = vi.fn(() => Promise.resolve());
    const replacementConnection = {
      createConfirmChannel: vi.fn(() => Promise.resolve(replacementChannel)),
      close: replacementConnectionClose,
      on: vi.fn()
    } as unknown as ChannelModel;
    const connect = vi.fn()
      .mockResolvedValueOnce(firstConnection)
      .mockResolvedValueOnce(replacementConnection);
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://translation:secret@example.invalid:5672",
      prefetch: 1,
      clock,
      connect
    });
    const envelope = createMinimalTranslationEnvelope();

    await transport.connect();
    await transport.consume("translation", () => Promise.resolve({
      action: "ack",
      reason: "handled",
      envelope
    }));

    if (deliveryCallback === undefined) {
      throw new Error("Expected the transport to register a delivery callback.");
    }

    deliveryCallback(null);
    await settleMicrotasks();
    expect(firstPrefetch).toHaveBeenCalledTimes(2);

    const closeListener = channelListeners.get("close");

    if (closeListener === undefined) {
      throw new Error("Expected the transport to register a channel close listener.");
    }

    closeListener();
    await transport.connect();
    expect(replacementPrefetch).toHaveBeenCalledTimes(1);
    expect(replacementConsume).toHaveBeenCalledTimes(1);
    expect(transport.consumerStatus("translation")).toMatchObject({
      state: "active"
    });

    staleReactivation.reject(new Error("old channel prefetch failed late"));
    await settleMicrotasks();

    expect(replacementChannelClose).not.toHaveBeenCalled();
    expect(replacementConnectionClose).not.toHaveBeenCalled();
    expect(transport.consumerStatus("translation")).toMatchObject({
      state: "active"
    });
    await transport.close();
    expect(replacementChannelClose).toHaveBeenCalledTimes(1);
    expect(replacementConnectionClose).toHaveBeenCalledTimes(1);
  });

  it("drains a failed delivery without an unhandled rejection when its channel is already closed", async () => {
    let deliveryCallback: ((message: ConsumeMessage | null) => void) | undefined;
    const nack = vi.fn(() => {
      throw new Error("channel is closed");
    });
    const channel = {
      prefetch: vi.fn(() => Promise.resolve({})),
      consume: vi.fn((queue: string, callback: (message: ConsumeMessage | null) => void) => {
        void queue;
        deliveryCallback = callback;
        return Promise.resolve({
          consumerTag: "translation-consumer-closed-channel"
        });
      }),
      cancel: vi.fn(() => Promise.resolve({})),
      close: vi.fn(() => Promise.resolve()),
      publish: vi.fn(),
      nack,
      on: vi.fn(),
      off: vi.fn()
    } as unknown as ConfirmChannel;
    const connection = {
      createConfirmChannel: vi.fn(() => Promise.resolve(channel)),
      close: vi.fn(() => Promise.resolve()),
      on: vi.fn()
    } as unknown as ChannelModel;
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://translation:secret@example.invalid:5672",
      prefetch: 1,
      clock,
      connect: () => Promise.resolve(connection)
    });
    const handler = vi.fn();
    const unhandledRejections: unknown[] = [];
    const recordUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };

    process.on("unhandledRejection", recordUnhandledRejection);

    try {
      await transport.consume("translation", handler);

      if (deliveryCallback === undefined) {
        throw new Error("Expected the transport to register a delivery callback.");
      }

      deliveryCallback({
        content: Buffer.from("not-json", "utf8")
      } as ConsumeMessage);
      expect(transport.inFlightDeliveryCount).toBe(1);

      await settleMicrotasks();
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });

      expect(nack).toHaveBeenCalledTimes(1);
      expect(handler).not.toHaveBeenCalled();
      expect(transport.inFlightDeliveryCount).toBe(0);
      expect(unhandledRejections).toEqual([]);
    } finally {
      process.off("unhandledRejection", recordUnhandledRejection);
      await transport.close();
    }
  });
});

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(reason: Error): void;
}

function deferred<T>(): Deferred<T> {
  let resolvePromise: ((value: T) => void) | undefined;
  let rejectPromise: ((reason: Error) => void) | undefined;
  const promise = new Promise<T>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });

  return {
    promise,
    resolve(value: T): void {
      if (resolvePromise === undefined) {
        throw new Error("Deferred promise resolver is unavailable.");
      }

      resolvePromise(value);
    },
    reject(reason: Error): void {
      if (rejectPromise === undefined) {
        throw new Error("Deferred promise rejector is unavailable.");
      }

      rejectPromise(reason);
    }
  };
}

async function settleMicrotasks(): Promise<void> {
  for (let index = 0; index < 5; index += 1) {
    await Promise.resolve();
  }
}

function productionEnv(): NodeJS.ProcessEnv {
  return {
    NUTSNEWS_TRANSLATION_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
    NUTSNEWS_TRANSLATION_DATABASE_URL: "postgres://translation:secret@example.invalid:5432/nutsnews",
    NUTSNEWS_TRANSLATION_RABBITMQ_URL: "amqp://translation:secret@example.invalid:5672",
    NUTSNEWS_TRANSLATION_QWEN_BASE_URL: "https://ai.example.test",
    NUTSNEWS_TRANSLATION_QWEN_API_KEY: "local-key",
    NUTSNEWS_TRANSLATION_PREFETCH: "2",
    NUTSNEWS_TRANSLATION_CONCURRENCY: "1"
  };
}

function translationRequest(): TranslationQwenRequest {
  return {
    model: "qwen2.5:3b",
    prompt: {
      id: "summary-translation-v1",
      version: "0.1.0",
      purpose: "summary-translation",
      instructions: "Return JSON."
    },
    timeoutMs: 30_000,
    maxInputBytes: 32_768,
    deterministic: {
      temperature: 0,
      topP: 1
    },
    responseSchema: {
      name: "translation_result_v1",
      requiredFields: [
        "summary",
        "qualityScore"
      ]
    },
    input: {
      articleId: "article-001",
      articleVersion: 1,
      sourceLanguage: "en",
      targetLanguage: "fr"
    }
  };
}
