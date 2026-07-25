import http from "node:http";
import type { AddressInfo } from "node:net";

import {
  runtimeHealthEndpointResponse,
  type PrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  TRANSLATION_CONFIG_SCHEMA,
  type TranslationConfig
} from "./config.js";
import type { TranslationService } from "./service.js";

export interface TranslationHttpServerOptions {
  readonly config: TranslationConfig;
  readonly service: TranslationService;
  readonly metrics?: PrometheusRuntimeTelemetrySink;
}

export interface TranslationHttpServer {
  readonly server: http.Server;
  listen(): Promise<http.Server>;
  close(): Promise<void>;
  url(path?: string): string;
}

export function createTranslationHttpServer(options: TranslationHttpServerOptions): TranslationHttpServer {
  const server = http.createServer((request, response) => {
    void routeRequest(options, request, response);
  });

  return {
    server,
    listen: () => new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.config.http.port, options.config.http.host, () => {
        server.off("error", reject);
        resolve(server);
      });
    }),
    close: () => new Promise((resolve, reject) => {
      server.close((error) => {
        if (error !== undefined) {
          reject(error);
          return;
        }

        resolve();
      });
    }),
    url: (path = "/") => {
      const address = server.address();

      if (!isAddressInfo(address)) {
        throw new Error("Translation HTTP server is not listening on a TCP address.");
      }

      return `http://127.0.0.1:${String(address.port)}${path}`;
    }
  };
}

async function routeRequest(
  options: TranslationHttpServerOptions,
  request: http.IncomingMessage,
  response: http.ServerResponse
): Promise<void> {
  const url = new URL(request.url ?? "/", "http://localhost");

  if (request.method !== "GET") {
    writeJson(response, 405, {
      status: "method-not-allowed"
    });
    return;
  }

  switch (url.pathname) {
    case "/live":
    case "/healthz":
      writeHealth(response, await options.service.health.liveness());
      return;
    case "/startup":
    case "/startupz":
      writeHealth(response, await options.service.health.startup());
      return;
    case "/ready":
    case "/readyz":
      writeHealth(response, await options.service.health.readiness());
      return;
    case "/metrics":
      writeText(response, 200, options.metrics?.collect() ?? "", "text/plain; version=0.0.4; charset=utf-8");
      return;
    case "/config-schema":
      writeJson(response, 200, {
        service: options.config.serviceName,
        version: options.config.serviceVersion,
        variables: TRANSLATION_CONFIG_SCHEMA
      });
      return;
    default:
      writeJson(response, 404, {
        status: "not-found"
      });
  }
}

function writeHealth(
  response: http.ServerResponse,
  report: Awaited<ReturnType<TranslationService["health"]["liveness"]>>
): void {
  const endpointResponse = runtimeHealthEndpointResponse(report);
  writeJson(response, endpointResponse.statusCode, endpointResponse.body, endpointResponse.headers);
}

function writeJson(
  response: http.ServerResponse,
  statusCode: number,
  body: unknown,
  headers: Readonly<Record<string, string>> = {}
): void {
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    ...headers
  });
  response.end(`${JSON.stringify(body)}\n`);
}

function writeText(
  response: http.ServerResponse,
  statusCode: number,
  body: string,
  contentType: string
): void {
  response.writeHead(statusCode, {
    "content-type": contentType,
    "cache-control": "no-store"
  });
  response.end(body);
}

function isAddressInfo(address: string | AddressInfo | null): address is AddressInfo {
  return typeof address === "object" && address !== null;
}
