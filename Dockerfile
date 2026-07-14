# Multi-stage, тот же паттерн, что в linkpulse-link.
#
# Нюанс мульти-репо: пока linkpulse-contracts не опубликован, go.mod содержит
# replace на ../linkpulse-contracts — поэтому контекст сборки должен быть
# родительской папкой: docker build -f linkpulse-analytics/Dockerfile .
# После публикации contracts на GitHub replace уйдёт и контекст станет обычным.
FROM golang:1.26.5-alpine AS build

WORKDIR /src

COPY linkpulse-contracts/ ../linkpulse-contracts/
COPY linkpulse-analytics/go.mod linkpulse-analytics/go.sum ./
RUN go mod download

COPY linkpulse-analytics/ .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/analytics ./cmd/analytics

FROM alpine:3.22

RUN adduser -D -u 10001 app
USER app

COPY --from=build /out/analytics /usr/local/bin/analytics
COPY --from=build /src/migrations /migrations

ENTRYPOINT ["analytics"]
