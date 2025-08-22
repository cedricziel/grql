# GRQL - gRPC Query Language Frontend

A gRPC-based query frontend service for SQL-like query language. Users can submit queries via gRPC and receive results.

## Features

- gRPC-based query service
- Support for SQL-like query syntax
- Streaming support for large result sets
- Query parameters support
- Query metadata (execution time, affected rows, column info)

## Project Structure

```
grql/
├── cmd/
│   └── server/         # Server entry point
├── internal/
│   └── server/         # Server implementation
├── proto/              # Protocol buffer definitions
├── Makefile           # Build commands
└── go.mod             # Go module definition
```

## Prerequisites

- Go 1.21 or later
- Protocol buffer compiler (protoc)
- gRPC and protobuf Go plugins

## Installation

Install the required Go plugins:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## Building

```bash
# Generate gRPC code from protobuf
make proto

# Build the server
make build

# Or build directly with go
go build -o bin/grql-server cmd/server/main.go
```

## Running

```bash
# Run with default settings (localhost:50051)
make run

# Or run with custom host/port
./bin/grql-server -host 0.0.0.0 -port 8080
```

## API

The service exposes two main RPC methods:

### ExecuteQuery
Execute a query and receive all results at once.

```protobuf
rpc ExecuteQuery(QueryRequest) returns (QueryResponse);
```

### StreamQuery
Execute a query and receive results as a stream.

```protobuf
rpc StreamQuery(QueryRequest) returns (stream QueryResult);
```

## Testing with grpcurl

The server has reflection enabled for easy testing:

```bash
# List services
grpcurl -plaintext localhost:50051 list

# Describe service
grpcurl -plaintext localhost:50051 describe grql.QueryService

# Execute a query
grpcurl -plaintext -d '{"query": "SELECT * FROM users", "limit": 10}' \
  localhost:50051 grql.QueryService/ExecuteQuery

# Stream query results
grpcurl -plaintext -d '{"query": "SELECT * FROM events"}' \
  localhost:50051 grql.QueryService/StreamQuery
```

## Development

```bash
# Run tests
make test

# Clean generated files
make clean
```

## License

MIT