# nutsnews-worker-article-translation

Deployable worker-uplift translation service shell for NutsNews.

## Responsibility

Consume translation jobs, fan accepted articles into per-language Qwen translation calls, validate translated summaries and quality gates, and publish persistence jobs after successful language results.

The worker records one independently replayable result per article version, source language, target language, prompt version, and model. Already successful combinations are reused on replay, while transient Qwen or retryable quality failures retry only the failed language path and do not roll back successfully persisted languages.

## Runtime Surface

- Consumes the contracted `translation` route and asserts the downstream `persistence` route.
- Accepts `translationTask` payloads whose contract consumer is `translation`.
- Resolves the configured summary-translation prompt and required language policy before Qwen calls.
- Enforces deterministic quality gates for empty output, length bounds, source-copy leakage, target script expectations, boilerplate, encoding, summary policy, and quality score threshold.
- Bounds quality re-prompts with `NUTSNEWS_TRANSLATION_QUALITY_REPROMPT_MAX_ATTEMPTS`; exhausted retryable quality failures become audited permanent language failures, never persistence inputs.
- Publishes one `persistenceCommand` per successful language summary and one `translationResult` status event after the language set is processed.
- Exposes `publishTranslationBacklogRecoveryTask` for reconciliation jobs to republish idempotent `backlog_recovery` translation tasks from durable language-result state without redoing valid languages.
- Provides injectable Qwen client, prompt registry, language policy, quality validator, durable state, transaction, outbox, broker, and work-handler boundaries.
- Configures low default prefetch and concurrency, plus per-language concurrency for Qwen-bound translation work.
- Uses shared runtime broker lifecycle, in-flight drain, idempotency store, retry/DLQ destinations, health reports, and Prometheus metrics. Its consumer-aware processor emits exactly one accepted, duplicate, invalid, retry, or DLQ completion for every started delivery.
- Exposes the canonical `nutsnews_worker_uplift_stage_events_total` lifecycle counter and fixed-bucket `nutsnews_worker_uplift_stage_latency_seconds` histogram, plus bounded per-language outcome and duration metrics and separate input/output/total token counters.
- Retains the runtime package's generic `_duration_ms` summaries temporarily for existing backend outage-report compatibility; the translation-specific millisecond summary has been replaced by seconds histograms.
- Keeps liveness independent from Qwen, prompt registry, language policy, and quality validator readiness; `/live` and `/livez` only check process health, while `/ready` gates an active `translation` main-queue consumer, broker, state, outbox, Qwen, prompt registry, language policy, quality validator, and shadow mode.
- Emits bounded structured events and Prometheus metrics when RabbitMQ cancels the consumer, drops its channel, or restores consumption.
- Contains no approval decision, article persistence, or publication logic.

## Metrics contract

`/metrics` retains the generic runtime 0.5 families and adds two worker-uplift lifecycle families: `nutsnews_worker_uplift_stage_events_total{environment,service,outcome}` and `nutsnews_worker_uplift_stage_latency_seconds{environment,service}`. It also exports `nutsnews_worker_expected_active{environment,service}=0` while translation remains shadow-only, `nutsnews_worker_consumer_active{environment,service,queue}`, and one-hot `nutsnews_worker_health_probe{environment,service,probe,outcome}` series initialized before the first scrape. Startup and consumer state transition with service lifecycle; readiness reflects the last real readiness evaluation and is reset on cancellation or shutdown. A delivery contributes one completion outcome (`success`, `duplicate`, `invalid`, `retry`, `dlq`, or `failure`) and, when measured, one latency observation. All six bounded outcome series are seeded at zero so collection completeness is independently observable. The fixed cumulative buckets are `0.01`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2.5`, `5`, `10`, `30`, `60`, `120`, and `300` seconds, followed by `+Inf`, `_sum`, and `_count`.

Per-language metrics use only `environment`, `service`, `stage`, `outcome`, `language`, and `provider`. Languages are restricted to the reviewed `fr`, `ja`, `de-CH`, `de`, and `el` metric allowlist and the provider allowlist contains only `local_ai`; all other values collapse to `unknown`. Message, article, pipeline, correlation, trace, idempotency, model, and prompt identifiers remain structured log fields and are never Prometheus labels.

Runtime 0.5's generic `_duration_ms` summaries remain temporarily for backend outage-report compatibility. Only dependency events with a real measured duration are forwarded to that sink: service startup no longer records a synthetic zero-duration dependency call. Translation-owned latency metrics are fixed-bucket seconds histograms.

Telemetry delivery is best effort at both fan-out and service boundaries. A synchronous sink failure, rejected emission, or metrics-setter error cannot change idempotency state, acknowledgement, retry, or DLQ behavior.

## Configuration

The HTTP server exposes `/config-schema` with names, defaults, sensitivity, and production requirements only. Runtime config records dependency presence booleans and never retains database URLs, RabbitMQ URLs, Qwen endpoint URLs, or API keys.

| Variable | Default | Production | Sensitive |
| --- | --- | --- | --- |
| `NUTSNEWS_TRANSLATION_BUILD_REVISION` | `development` | required lowercase 40-character Git SHA | no |
| `NUTSNEWS_TRANSLATION_DATABASE_URL` | unset | required | yes |
| `NUTSNEWS_TRANSLATION_RABBITMQ_URL` | unset | required | yes |
| `NUTSNEWS_TRANSLATION_QWEN_BASE_URL` | unset | required | yes |
| `NUTSNEWS_TRANSLATION_QWEN_API_KEY` | unset | required | yes |
| `NUTSNEWS_TRANSLATION_QWEN_MODEL` | `qwen2.5:3b` | optional | no |
| `NUTSNEWS_TRANSLATION_PROMPT_ID` | `summary-translation-v1` | optional | no |
| `NUTSNEWS_TRANSLATION_LANGUAGE_POLICY_ID` | `required-summaries-v1` | optional | no |
| `NUTSNEWS_TRANSLATION_TARGET_LANGUAGES` | `fr,ja,de-CH,de,el` | optional | no |
| `NUTSNEWS_TRANSLATION_PER_LANGUAGE_CONCURRENCY` | `1` | optional | no |
| `NUTSNEWS_TRANSLATION_QUALITY_MIN_SCORE` | `80` | optional | no |
| `NUTSNEWS_TRANSLATION_SUMMARY_MIN_CHARS` | `24` | optional | no |
| `NUTSNEWS_TRANSLATION_SUMMARY_MAX_CHARS` | `420` | optional | no |
| `NUTSNEWS_TRANSLATION_QUALITY_REPROMPT_MAX_ATTEMPTS` | `2` | optional | no |
| `NUTSNEWS_TRANSLATION_CONCURRENCY` | `2` | optional | no |
| `NUTSNEWS_TRANSLATION_PREFETCH` | `4` | optional | no |
| `NUTSNEWS_TRANSLATION_SHADOW_MODE` | `true` | must remain true here | no |

## Local Verification

```sh
npm ci
npm run ci
NODE_AUTH_TOKEN=<github-packages-token> npm run container:build
```

`npm run ci` runs lint, typecheck, unit tests, integration tests, build, SBOM generation, and a production dependency audit.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-article-translation:${GITHUB_SHA}`. This repository is deployable only through backend-owned infrastructure.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

Backend deployments consume immutable SHA-tagged GHCR images. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for CI. Workflows use least-privilege permissions and request `packages: write` only for publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
