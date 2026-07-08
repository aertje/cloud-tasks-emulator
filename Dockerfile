FROM --platform=$BUILDPLATFORM golang:1.26-alpine3.24 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o cloud-tasks-emulator ./cmd/emulator

FROM alpine:3.24

LABEL org.opencontainers.image.source=https://github.com/aertje/cloud-tasks-emulator

RUN adduser -D -u 10001 appuser

WORKDIR /

COPY --from=builder --chown=appuser /app/cloud-tasks-emulator .

USER appuser

ENTRYPOINT ["./cloud-tasks-emulator"]
