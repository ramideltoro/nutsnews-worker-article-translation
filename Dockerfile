# syntax=docker/dockerfile:1

FROM node:24.16.0-alpine AS dependencies
WORKDIR /app
COPY .npmrc package.json package-lock.json ./
RUN --mount=type=secret,id=npm_token NODE_AUTH_TOKEN="$(cat /run/secrets/npm_token)" npm ci

FROM dependencies AS build
COPY tsconfig.json tsconfig.eslint.json eslint.config.js vitest.config.ts vitest.integration.config.ts ./
COPY src ./src
COPY test ./test
RUN npm run ci && npm run build

FROM dependencies AS production-dependencies
RUN --mount=type=secret,id=npm_token NODE_AUTH_TOKEN="$(cat /run/secrets/npm_token)" npm prune --omit=dev --ignore-scripts \
    && npm cache clean --force

FROM node:24.16.0-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
RUN rm -rf /usr/local/lib/node_modules/npm \
    && rm -f /usr/local/bin/npm /usr/local/bin/npx \
    && addgroup -S nutsnews \
    && adduser -S -G nutsnews nutsnews
COPY --from=production-dependencies /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json README.md LICENSE ./
USER nutsnews
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 CMD node -e "fetch('http://127.0.0.1:8080/ready').then((r)=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "dist/src/index.js"]
