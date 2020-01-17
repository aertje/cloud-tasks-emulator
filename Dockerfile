FROM golang:1.13-alpine

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o emulator .

EXPOSE 8123

ENTRYPOINT ["./emulator"]
