# Repository Boundary

| Field | Value |
| --- | --- |
| Repository | `ramideltoro/nutsnews-worker-article-translation` |
| Owner | `@ramideltoro` |
| Responsibility | Consume translation jobs, call the Qwen translation endpoint, validate translated fields and quality gates, and publish persistence jobs after successful translation. |
| Deployable / package type | Deployable service repo. Publishes immutable SHA-tagged images to `ghcr.io/ramideltoro/nutsnews-worker-article-translation`. |
| Primary artifact | Signed GHCR image tagged only by commit SHA |
| Support boundary | Repo-local code, tests, CI, package/image publishing, and service-local run notes. |
| Outside boundary | Backend host runtime/deployments, Grafana Cloud resources, explanatory architecture/operations docs, production secrets, legacy ingestion. |

## Existing Ownership Boundaries

- `ramideltoro/nutsnews-backend` owns backend-host runtime and deployments.
- `production-backend` in `ramideltoro/nutsnews-backend` remains the runtime secret and deployment boundary.
- `ramideltoro/nutsnews-infra` owns Grafana Cloud resources.
- `ramideltoro/nutsnews-docs` owns explanatory architecture and operations docs.
- `ramideltoro/nutsnews-worker` remains the active legacy ingestion and failover path until explicit cutover.

## Control Baseline

- Public repository with MIT license to match existing NutsNews worker/docs visibility and licensing.
- Default branch: `main`.
- CODEOWNERS assigns repository ownership to `@ramideltoro`.
- Default Actions token permission: read-only.
- Publish workflows request package write permission only inside publish jobs.
- CI validates repo boundary docs on every push and pull request.
- CodeQL and dependency review workflows are present for future code-bearing changes.
- Dependabot checks GitHub Actions and npm manifests.
- Branch protection requires pull requests, resolved conversations, and the `validate` status check where GitHub permits repository branch protection. CODEOWNERS documents ownership for reviews.

## Conditional Controls

Some GitHub package access controls can be applied only after the first package or container image exists. Until then, this repository documents the intended access model and publish workflows:

- Package repos publish exact versions to GitHub Packages. Downstream repos install through `GITHUB_TOKEN` with `packages: read`.
- Service repos publish signed, immutable SHA-tagged images to GHCR. Backend production deploys pull through `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` only.
- App repos must not define production environments or store production runtime secrets.
