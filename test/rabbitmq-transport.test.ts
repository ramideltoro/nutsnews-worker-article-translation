import type {
  ChannelModel,
  ConfirmChannel,
  ConsumeMessage
} from "amqplib";
import {
  describe,
  expect,
  it
} from "vitest";

import { PayloadRabbitMqTransport } from "../src/production.js";

type CloseHandler = () => void;

interface FakeBroker {
  readonly connections: FakeConnection[];
  readonly connect: (url: string) => Promise<ChannelModel>;
}

interface FakeConnection {
  readonly channel: FakeChannel;
  emitClose(): void;
  toChannelModel(): ChannelModel;
}

interface FakeChannel {
  readonly cancelTags: string[];
  readonly consumeQueues: string[];
  readonly prefetchCalls: number[];
  emitClose(): void;
  toConfirmChannel(): ConfirmChannel;
}

const clock = {
  now: () => new Date("2026-07-25T00:00:00.000Z")
};

describe("RabbitMQ payload transport", () => {
  it("restores registered translation consumers after reconnecting", async () => {
    const broker = createFakeBroker();
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://translation:test@example.invalid:5672",
      prefetch: 2,
      clock,
      connect: broker.connect
    });

    await transport.consume("translation", () => Promise.resolve({
      action: "dlq",
      reason: "not-used"
    }));

    expect(broker.connections).toHaveLength(1);
    expect(broker.connections[0]?.channel.consumeQueues).toEqual([
      "nutsnews.worker.translation.v1"
    ]);
    expect(broker.connections[0]?.channel.prefetchCalls).toEqual([
      2
    ]);

    broker.connections[0]?.emitClose();
    await transport.connect();

    expect(broker.connections).toHaveLength(2);
    expect(broker.connections[1]?.channel.consumeQueues).toEqual([
      "nutsnews.worker.translation.v1"
    ]);
    expect(broker.connections[1]?.channel.prefetchCalls).toEqual([
      2
    ]);
  });
});

function createFakeBroker(): FakeBroker {
  const connections: FakeConnection[] = [];

  return {
    connections,
    connect: (url: string): Promise<ChannelModel> => {
      expect(url).toBe("amqp://translation:test@example.invalid:5672");
      const connection = createFakeConnection();
      connections.push(connection);
      return Promise.resolve(connection.toChannelModel());
    }
  };
}

function createFakeConnection(): FakeConnection {
  const channel = createFakeChannel();
  const closeHandlers: CloseHandler[] = [];
  const connection = {
    createConfirmChannel(): Promise<ConfirmChannel> {
      return Promise.resolve(channel.toConfirmChannel());
    },
    close(): Promise<void> {
      for (const handler of closeHandlers) {
        handler();
      }

      return Promise.resolve();
    },
    on(event: string, handler: unknown): unknown {
      if (event === "close" && isCloseHandler(handler)) {
        closeHandlers.push(handler);
      }

      return connection;
    }
  };

  return {
    channel,
    emitClose(): void {
      for (const handler of closeHandlers) {
        handler();
      }
    },
    toChannelModel(): ChannelModel {
      return connection as unknown as ChannelModel;
    }
  };
}

function createFakeChannel(): FakeChannel {
  const cancelTags: string[] = [];
  const consumeQueues: string[] = [];
  const prefetchCalls: number[] = [];
  const closeHandlers: CloseHandler[] = [];
  const channel = {
    prefetch(count: number): Promise<void> {
      prefetchCalls.push(count);
      return Promise.resolve();
    },
    consume(queue: string, onMessage: (message: ConsumeMessage | null) => void): Promise<{ readonly consumerTag: string }> {
      void onMessage;
      consumeQueues.push(queue);
      return Promise.resolve({
        consumerTag: `consumer-${String(consumeQueues.length)}`
      });
    },
    cancel(consumerTag: string): Promise<void> {
      cancelTags.push(consumerTag);
      return Promise.resolve();
    },
    close(): Promise<void> {
      for (const handler of closeHandlers) {
        handler();
      }

      return Promise.resolve();
    },
    on(event: string, handler: unknown): unknown {
      if (event === "close" && isCloseHandler(handler)) {
        closeHandlers.push(handler);
      }

      return channel;
    }
  };

  return {
    cancelTags,
    consumeQueues,
    prefetchCalls,
    emitClose(): void {
      for (const handler of closeHandlers) {
        handler();
      }
    },
    toConfirmChannel(): ConfirmChannel {
      return channel as unknown as ConfirmChannel;
    }
  };
}

function isCloseHandler(handler: unknown): handler is CloseHandler {
  return typeof handler === "function";
}
