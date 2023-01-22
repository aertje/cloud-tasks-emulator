FROM golang:1.19-alpine as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o emulator .


FROM alpine:latest

LABEL org.opencontainers.image.source=https://github.com/aertje/cloud-tasks-emulator

ENTRYPOINT ["/emulator"]

WORKDIR /

COPY --from=builder /app/emulator .
