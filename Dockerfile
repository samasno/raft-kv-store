FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod .
RUN go mod download
COPY . .
RUN go build -o raft-kv ./app/

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/raft-kv .
EXPOSE 8000 9000
CMD ["./raft-kv"]
