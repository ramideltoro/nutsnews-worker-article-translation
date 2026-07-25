# nutsnews-worker-article-translation

Deployable worker-uplift translation service shell for NutsNews.

## Responsibility

Consume translation jobs, call the Qwen translation endpoint, validate translated fields and quality gates, and publish persistence jobs after successful translation.

This bootstrap establishes the translation service boundary, runtime shell, health/metrics surface, container, and injectable dependency contracts. Summary translation behavior, persistence-command publication, language-specific recovery, and quality scoring are intentionally deferred to follow-up translation issues.

## Runtime Surface

- Consumes the contracted `translation` route and asserts the downstream `persistence` route for future publish work.
- Accepts `translationTask` payloads whose contract consumer is `translation`.
- Provides injectable Qwen client, language policy, quality validator, durable state, transaction, outbox, broker, and work-handler boundaries.
- Configures low default prefetch and concurrency, plus per-language concurrency for future Qwen-bound translation work.
- Uses shared runtime broker lifecycle, in-flight drain, idempotency store, retry/DLQ destinations, health reports, and Prometheus metrics.
- Keeps liveness independent from Qwen, language policy, and quality validator readiness; `/live` only checks process health, while `/ready` gates broker, state, outbox, Qwen, language policy, quality validator, and shadow mode.
- Contains no approval decision, article persistence, or publication logic.

## Configuration

The HTTP server exposes `/config-schema` with names, defaults, sensitivity, and production requirements only. Runtime config records dependency presence booleans and never retains database URLs, RabbitMQ URLs, Qwen endpoint URLs, or API keys.

| Variable | Default | Production | Sensitive |
| --- | --- | --- | --- |
| `NUTSNEWS_TRANSLATION_DATABASE_URL` | unset | required | yes |
| `NUTSNEWS_TRANSLATION_RABBITMQ_URL` | unset | required | yes |
| `NUTSNEWS_TRANSLATION_QWEN_BASE_URL` | unset | required | yes |
| `NUTSNEWS_TRANSLATION_QWEN_API_KEY` | unset | required | yes |
| `NUTSNEWS_TRANSLATION_QWEN_MODEL` | `qwen2.5:3b` | optional | no |
| `NUTSNEWS_TRANSLATION_LANGUAGE_POLICY_ID` | `required-summaries-v1` | optional | no |
| `NUTSNEWS_TRANSLATION_TARGET_LANGUAGES` | `fr,ja,de-CH,de,el` | optional | no |
| `NUTSNEWS_TRANSLATION_PER_LANGUAGE_CONCURRENCY` | `1` | optional | no |
| `NUTSNEWS_TRANSLATION_QUALITY_MIN_SCORE` | `80` | optional | no |
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
