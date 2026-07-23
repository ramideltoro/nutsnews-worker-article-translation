# nutsnews-worker-article-translation

Worker-uplift repository for NutsNews.

## Responsibility

Consume translation jobs, call the Qwen translation endpoint, validate translated fields and quality gates, and publish persistence jobs after successful translation.

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
