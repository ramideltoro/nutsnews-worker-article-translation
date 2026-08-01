import http from "node:http";
import { timingSafeEqual } from "node:crypto";
import type { AddressInfo } from "node:net";

import {
  runtimeHealthEndpointResponse
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  TRANSLATION_CONFIG_SCHEMA,
  type TranslationConfig
} from "./config.js";
import {
  TRANSLATION_RECONCILIATION_PATH,
  type TranslationReconciliationRequest,
  type TranslationReconciler
} from "./reconciliation.js";
import type { TranslationService } from "./service.js";
import type { TranslationRuntimeMetricsSink } from "./metrics.js";

export interface TranslationHttpServerOptions {
  readonly config: TranslationConfig;
  readonly service: TranslationService;
  readonly metrics?: TranslationRuntimeMetricsSink;
  readonly reconciler?: TranslationReconciler;
  readonly reconciliationToken?: string;
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

  if (request.method === "POST" && url.pathname === TRANSLATION_RECONCILIATION_PATH) {
    await handleReconciliationRequest(options, request, response);
    return;
  }

  if (request.method !== "GET") {
    writeJson(response, 405, {
      status: "method-not-allowed"
    });
    return;
  }

  switch (url.pathname) {
    case "/live":
    case "/livez":
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

async function handleReconciliationRequest(
  options: TranslationHttpServerOptions,
  request: http.IncomingMessage,
  response: http.ServerResponse
): Promise<void> {
  if (options.reconciler === undefined || options.reconciliationToken === undefined) {
    writeJson(response, 503, {
      service: "translation",
      status: "not_configured",
      writesPerformed: false,
      dryRun: true,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false,
      errors: [
        "translation reconciliation endpoint is not configured"
      ]
    });
    return;
  }

  if (!authorized(request.headers.authorization, options.reconciliationToken)) {
    writeJson(response, 401, {
      service: "translation",
      status: "unauthorized",
      writesPerformed: false,
      dryRun: true,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false,
      errors: [
        "valid bearer token required"
      ]
    });
    return;
  }

  let body: TranslationReconciliationRequest;

  try {
    body = await readJsonBody(request);
  } catch (error: unknown) {
    writeJson(response, 400, {
      service: "translation",
      status: "failed_closed",
      writesPerformed: false,
      dryRun: true,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false,
      errors: [
        error instanceof Error ? error.message : "invalid reconciliation request body"
      ]
    });
    return;
  }

  const report = await options.reconciler.reconcile(body);
  const statusCode = report.status === "applied" || report.status === "dry_run"
    ? 200
    : report.status === "kill_switch_active"
      ? 423
      : 409;

  writeJson(response, statusCode, report);
}

function authorized(header: string | undefined, expectedToken: string): boolean {
  if (!header?.startsWith("Bearer ")) {
    return false;
  }

  const provided = header.slice("Bearer ".length);
  const providedBuffer = Buffer.from(provided);
  const expectedBuffer = Buffer.from(expectedToken);

  return providedBuffer.length === expectedBuffer.length && timingSafeEqual(providedBuffer, expectedBuffer);
}

function readJsonBody(request: http.IncomingMessage): Promise<TranslationReconciliationRequest> {
  const chunks: Buffer[] = [];
  let totalBytes = 0;
  const maxBytes = 16_384;

  return new Promise((resolve, reject) => {
    request.on("data", (chunk: Buffer) => {
      totalBytes += chunk.byteLength;

      if (totalBytes > maxBytes) {
        reject(new Error("reconciliation request body is too large"));
        request.destroy();
        return;
      }

      chunks.push(chunk);
    });
    request.on("error", reject);
    request.on("end", () => {
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;

        if (!isRecord(parsed)) {
          reject(new Error("reconciliation request body must be a JSON object"));
          return;
        }

        resolve(parsed as unknown as TranslationReconciliationRequest);
      } catch {
        reject(new Error("reconciliation request body must be valid JSON"));
      }
    });
  });
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

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
