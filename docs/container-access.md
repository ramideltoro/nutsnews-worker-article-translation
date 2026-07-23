# Container Access

Image: `ghcr.io/ramideltoro/nutsnews-worker-article-translation:<commit-sha>`

The publish workflow signs and pushes immutable SHA-tagged images after implementation adds a Dockerfile. It does not publish mutable `latest` tags.

The intended production package consumer is:

```text
ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml
```

That workflow runs in the protected `production-backend` environment and must use a short-lived `GITHUB_TOKEN` with `packages: read`. App repositories must not receive production deployment secrets.
