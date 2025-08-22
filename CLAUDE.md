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

This is a unified query engine that provides a SQL-like query interface (similar to NRQL) over gRPC, federating queries across Grafana Mimir (metrics), Loki (logs), and Tempo (traces).

### Query Engine Architecture
- **Parser**: Uses Vitess sqlparser with NRQL-like extensions (`internal/engine/parser.go`)
- **Query Planner**: Creates logical and physical execution plans (`internal/engine/planner.go`)
- **Query Optimizer**: Applies optimization rules like predicate pushdown
- **Query Executor**: Executes plans against backend systems (`internal/engine/executor.go`)
- **Backend Adapters**: Translate SQL to backend-specific query languages:
  - **Mimir Adapter**: Translates to PromQL for metrics (`internal/engine/backends/mimir.go`)
  - **Loki Adapter**: Translates to LogQL for logs (`internal/engine/backends/loki.go`)
  - **Tempo Adapter**: Translates to TraceQL for traces (`internal/engine/backends/tempo.go`)

### Service Structure
- **gRPC Service**: `QueryService` with two RPC methods:
  - `ExecuteQuery`: Returns all results at once with caching
  - `StreamQuery`: Streams results for large datasets
- **Server Implementation**: Located in `internal/server/server.go`, integrates with the query engine
- **Proto Definitions**: `proto/query.proto` defines the service contract and message types

### Key Components
- **QueryRequest**: Contains query string, parameters map, limit, and offset
- **QueryResponse**: Contains results array, metadata (rows affected, execution time, column info), and error string
- **Value Type**: Supports multiple data types (string, int64, double, bool, bytes) via protobuf oneof
- **Streaming**: The `StreamQuery` RPC uses server-side streaming for handling large result sets

### Environment Variables
Configure backend connections via environment variables:
```bash
export MIMIR_URL=http://localhost:9009     # Grafana Mimir endpoint
export LOKI_URL=http://localhost:3100      # Grafana Loki endpoint  
export TEMPO_URL=http://localhost:3200     # Grafana Tempo endpoint
export TENANT_ID=my-tenant                 # Optional multi-tenant ID
```

### Query Examples
The query engine supports NRQL-like SQL syntax:
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
```

### Testing with grpcurl
The server has gRPC reflection enabled, allowing testing with grpcurl:
```bash
# Execute a metrics query
grpcurl -plaintext localhost:50051 grql.QueryService/ExecuteQuery \
  -d '{"query": "SELECT avg(cpu_usage) FROM metrics WHERE service=\"api\" SINCE 1h ago"}'

# Stream logs
grpcurl -plaintext localhost:50051 grql.QueryService/StreamQuery \
  -d '{"query": "SELECT * FROM logs WHERE level=\"error\""}'
```

## Module Configuration
- Go module: `github.com/cedricziel/grql`
- Go version: 1.25.0
- Main dependencies: `google.golang.org/grpc` and `google.golang.org/protobuf`
- Proto package option: `go_package = "github.com/cedricziel/grql/internal/proto"`