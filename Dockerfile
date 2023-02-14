FROM golang:1.20-alpine as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN (cd ./cmd/emulator && go build -o emulator .)


FROM alpine:latest

LABEL org.opencontainers.image.source=https://github.com/aertje/cloud-tasks-emulator

WORKDIR /app

COPY --from=builder /app/cmd/emulator/emulator /app

ENTRYPOINT ["./emulator"]
