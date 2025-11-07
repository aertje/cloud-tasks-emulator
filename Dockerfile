FROM golang:1.13-alpine as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .
RUN go build -o emulator .

FROM alpine:latest

LABEL org.opencontainers.image.source=https://github.com/aertje/cloud-tasks-emulator

WORKDIR /

# Install ca-certificates package
RUN apk add --no-cache ca-certificates

# Copy local CA certificate if it exists and add it to trusted certificates
COPY --from=builder /app/rootCA.pem /usr/local/share/ca-certificates/rootCA.crt
RUN update-ca-certificates

COPY --from=builder /app/oidc.key oidc.key
COPY --from=builder /app/oidc.cert oidc.cert
COPY --from=builder /app/emulator .
COPY --from=builder /app/emulator_from_env.sh .
RUN chmod +x emulator_from_env.sh

ENTRYPOINT ["./emulator"]
