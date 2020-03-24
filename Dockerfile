FROM golang:1.13-alpine as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o emulator .


FROM alpine:latest

ENTRYPOINT ["/emulator"]

WORKDIR /

COPY --from=builder /app/emulator .
