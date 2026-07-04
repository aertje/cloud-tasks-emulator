FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o emulator ./cmd/emulator

FROM alpine:3.21

LABEL org.opencontainers.image.source=https://github.com/aertje/cloud-tasks-emulator

RUN adduser -D -u 10001 appuser

WORKDIR /

COPY --from=builder --chown=appuser /app/emulator .
COPY --from=builder --chown=appuser /app/emulator_from_env.sh .
RUN chmod +x emulator_from_env.sh

USER appuser

ENTRYPOINT ["./emulator"]
