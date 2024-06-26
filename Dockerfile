FROM golang:1.13-alpine as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .
RUN go build -o emulator .

FROM alpine:latest

LABEL org.opencontainers.image.source=https://github.com/aertje/cloud-tasks-emulator

WORKDIR /

COPY --from=builder /app/oidc.key oidc.key
COPY --from=builder /app/oidc.cert oidc.cert
COPY --from=builder /app/emulator .
COPY --from=builder /app/emulator_from_env.sh .
RUN chmod +x emulator_from_env.sh

ENTRYPOINT ["./emulator"]
