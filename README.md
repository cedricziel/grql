# GRQL - gRPC Query Language Frontend

[![CI](https://github.com/cedricziel/grql/actions/workflows/ci.yml/badge.svg)](https://github.com/cedricziel/grql/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/cedricziel/grql)](https://goreportcard.com/report/github.com/cedricziel/grql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A unified query engine that provides a SQL-like query interface (similar to NRQL) over gRPC, federating queries across Grafana Mimir (metrics), Loki (logs), and Tempo (traces) backends.

## Features

- **Unified Query Language**: NRQL-like SQL dialect for querying metrics, logs, and traces
- **Multi-Backend Support**: Seamlessly query Grafana Mimir, Loki, and Tempo
- **Query Federation**: Join and correlate data across different observability backends
- **Query Optimization**: Cost-based optimizer with predicate pushdown
- **Streaming Support**: Efficient streaming for large result sets
- **Result Caching**: Built-in query result caching for improved performance
- **gRPC API**: High-performance gRPC interface with reflection support

## Project Structure

```
grql/
├── cmd/
│   └── server/         # Server entry point
├── internal/
│   ├── server/         # gRPC server implementation
│   └── engine/         # Query engine components
│       ├── parser.go   # SQL parser with NRQL extensions
│       ├── planner.go  # Query planner and optimizer
│       ├── executor.go # Query executor with caching
│       ├── plan.go     # Query plan data structures
│       └── backends/   # Backend adapters
│           ├── mimir.go  # Mimir/PromQL adapter
│           ├── loki.go   # Loki/LogQL adapter
│           └── tempo.go # Tempo/TraceQL adapter
├── proto/              # Protocol buffer definitions
├── Makefile           # Build commands
└── go.mod             # Go module definition
```

## Prerequisites

- Go 1.25 or later
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

## Configuration

Set environment variables to configure backend connections:

```bash
export MIMIR_URL=http://localhost:9009     # Grafana Mimir endpoint
export LOKI_URL=http://localhost:3100      # Grafana Loki endpoint  
export TEMPO_URL=http://localhost:3200     # Grafana Tempo endpoint
export TENANT_ID=my-tenant                 # Optional multi-tenant ID
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

## Query Examples

The query engine supports NRQL-like SQL syntax for querying across backends:

```sql
-- Query metrics from Mimir
SELECT avg(cpu_usage), max(memory_usage) FROM metrics 
WHERE service="api" GROUP BY instance SINCE 1 hour ago

-- Query logs from Loki
SELECT count(*) FROM logs 
WHERE level="error" AND service="frontend" SINCE 24 hours ago

-- Query traces from Tempo
SELECT avg(duration), count(*) FROM traces 
WHERE service_name="checkout" AND duration > 100 GROUP BY operation_name

-- Correlate logs and metrics (federation)
SELECT l.message, m.cpu_usage FROM logs l, metrics m
WHERE l.service = m.service AND l.level = "error"
```

## Testing with grpcurl

The server has reflection enabled for easy testing:

```bash
# List services
grpcurl -plaintext localhost:50051 list

# Describe service
grpcurl -plaintext localhost:50051 describe grql.QueryService

# Execute a metrics query
grpcurl -plaintext localhost:50051 grql.QueryService/ExecuteQuery \
  -d '{"query": "SELECT avg(cpu_usage) FROM metrics WHERE service=\"api\""}'

# Stream logs
grpcurl -plaintext localhost:50051 grql.QueryService/StreamQuery \
  -d '{"query": "SELECT * FROM logs WHERE level=\"error\" LIMIT 100"}'

# Query with parameters
grpcurl -plaintext localhost:50051 grql.QueryService/ExecuteQuery \
  -d '{"query": "SELECT * FROM metrics", "parameters": {"since": "1h", "limit": "10"}}'
```

## Development

```bash
# Run tests
make test
# Or directly with go test
go test ./...

# Clean generated files
make clean

# Build directly (if protoc has issues)
go build -o bin/grql-server cmd/server/main.go
```

## Architecture Notes

The query engine compiles and includes:
- SQL parser with NRQL-like extensions using Vitess sqlparser
- Query planner with cost-based optimization
- Backend adapters for Mimir (PromQL), Loki (LogQL), and Tempo (TraceQL)
- Result caching with LRU eviction
- Streaming support for large result sets

Note: Some tests may fail for advanced NRQL syntax features that are still being implemented.

## License

MIT