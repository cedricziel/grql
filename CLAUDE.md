# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Generate gRPC code from protobuf definitions
make proto

# Build the server binary
make build

# Run the server (builds first)
make run

# Run tests
make test

# Clean generated protobuf files
make clean

# Run server with custom host/port
./bin/grql-server -host 0.0.0.0 -port 8080
```

## Architecture

This is a gRPC-based query service that provides a SQL-like query interface over gRPC. The service architecture follows these patterns:

### Service Structure
- **gRPC Service**: `QueryService` with two RPC methods:
  - `ExecuteQuery`: Returns all results at once
  - `StreamQuery`: Streams results for large datasets
- **Server Implementation**: Located in `internal/server/server.go`, currently contains mock implementation that generates sample data
- **Proto Definitions**: `proto/query.proto` defines the service contract and message types

### Key Components
- **QueryRequest**: Contains query string, parameters map, limit, and offset
- **QueryResponse**: Contains results array, metadata (rows affected, execution time, column info), and error string
- **Value Type**: Supports multiple data types (string, int64, double, bool, bytes) via protobuf oneof
- **Streaming**: The `StreamQuery` RPC uses server-side streaming for handling large result sets

### Testing with grpcurl
The server has gRPC reflection enabled, allowing testing with grpcurl:
```bash
grpcurl -plaintext localhost:50051 grql.QueryService/ExecuteQuery -d '{"query": "SELECT * FROM users", "limit": 10}'
grpcurl -plaintext localhost:50051 grql.QueryService/StreamQuery -d '{"query": "SELECT * FROM events"}'
```

## Module Configuration
- Go module: `github.com/cedricziel/grql`
- Go version: 1.25.0
- Main dependencies: `google.golang.org/grpc` and `google.golang.org/protobuf`
- Proto package option: `go_package = "github.com/cedricziel/grql/internal/proto"`