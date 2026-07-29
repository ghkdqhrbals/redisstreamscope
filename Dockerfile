FROM node:24-alpine AS web-builder
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json tsconfig.app.json tsconfig.node.json vite.config.ts ./
COPY web ./web
RUN npm run build:web

FROM golang:1.25-alpine AS go-builder
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY config.properties ./
COPY --from=web-builder /src/dist ./dist
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.buildVersion=${VERSION}" -o /out/streamscope . \
    && mkdir -p /out/data \
    && cp config.properties /out/data/config.properties \
    && chmod 600 /out/data/config.properties \
    && chown 65532:65532 /out/data

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=go-builder --chown=65532:65532 /out/streamscope /app/streamscope
COPY --from=go-builder --chown=65532:65532 /out/data /data
VOLUME ["/data"]
EXPOSE 8080
ENV GOMEMLIMIT=96MiB \
    GOGC=75
ENTRYPOINT ["/app/streamscope"]
