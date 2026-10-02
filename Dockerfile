FROM golang:1.23-alpine AS builder

RUN apk add --no-cache protobuf-dev git ca-certificates

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.2 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

WORKDIR /app
COPY go.mod ./
COPY proto ./proto
RUN mkdir -p gen && \
    protoc -I proto --go_out=gen --go_opt=paths=source_relative \
           --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
           events/v1/events.proto

COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go test ./internal/domain/... ./internal/resilience/... ./internal/crawler/... ./internal/api/... && \
    CGO_ENABLED=0 GOOS=linux go build -o /bin/api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -o /bin/worker ./cmd/worker

FROM alpine:3.20 AS api
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/api /usr/local/bin/api
EXPOSE 8080 50051
ENTRYPOINT ["/usr/local/bin/api"]

FROM alpine:3.20 AS worker
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/worker /usr/local/bin/worker
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/worker"]
