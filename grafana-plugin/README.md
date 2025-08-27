# gRQL Grafana Datasource Plugin

A Grafana datasource plugin for querying the gRQL unified query engine via gRPC.

## Features

- **Unified Query Interface**: Query metrics, logs, and traces using SQL-like syntax
- **gRPC Communication**: Direct connection to gRQL server using efficient gRPC protocol  
- **Multiple Data Sources**: Federated queries across Grafana Mimir, Loki, and Tempo
- **Flexible Visualization**: Support for both time series and table formats
- **Secure Connections**: Optional TLS support with certificate authentication

## Installation

### Development Setup

1. Install dependencies:
```bash
make plugin-install
```

2. Build the plugin:
```bash
make plugin-build
```

3. Start development environment:
```bash
make plugin-up
```

This will start:
- gRQL server on port 50051
- Grafana with the plugin on port 3000

### Production Installation

1. Build the plugin:
```bash
make plugin-build
```

2. Copy the `dist/` folder to your Grafana plugins directory:
```bash
cp -r grafana-plugin/dist /var/lib/grafana/plugins/cedricziel-grql-datasource
```

3. Restart Grafana

## Configuration

### Data Source Settings

- **Host**: gRQL server hostname (default: localhost)
- **Port**: gRQL server port (default: 50051)
- **Use TLS**: Enable secure connection
- **Skip TLS Verify**: Skip certificate verification (not recommended for production)
- **TLS Certificate**: Client certificate for mTLS
- **TLS Key**: Client key for mTLS
- **TLS CA Certificate**: CA certificate for server verification

## Query Examples

### Metrics Query
```sql
SELECT avg(cpu_usage), max(memory_usage) 
FROM metrics 
WHERE service="api" 
GROUP BY instance 
SINCE 1 hour ago
```

### Logs Query
```sql
SELECT count(*) 
FROM logs 
WHERE level="error" AND service="frontend" 
SINCE 24 hours ago
```

### Traces Query
```sql
SELECT avg(duration), count(*) 
FROM traces 
WHERE service_name="checkout" AND duration > 100 
GROUP BY operation_name
```

## Development

### Frontend Development
```bash
# Watch for changes
make plugin-dev

# Build production bundle
make plugin-frontend
```

### Backend Development
```bash
# Build backend binary
make plugin-backend
```

### Running Tests
```bash
cd grafana-plugin
npm test
go test ./...
```

### Using Mage

Backend plugin binaries can be built using Mage:

```bash
# Build for all platforms
mage -v

# Build for specific platform
mage -v build:linuxARM64

# List all available targets
mage -l
```

### E2E Testing

```bash
# Start Grafana server
npm run server

# Run E2E tests
npm run e2e
```

## Architecture

The plugin consists of:
- **Frontend (TypeScript/React)**: Query editor and configuration UI
- **Backend (Go)**: gRPC client connecting to gRQL server
- **gRPC Protocol**: Efficient binary protocol for query execution

## Distribution

When distributing the plugin, it must be signed for Grafana to verify authenticity.

### Initial Setup

1. Create a [Grafana Cloud account](https://grafana.com/signup)
2. Ensure plugin ID in `plugin.json` matches your account slug
3. Create a Grafana Cloud API key with `PluginPublisher` role
4. Add the API key as a GitHub secret named `GRAFANA_API_KEY`

### Release Process

1. Update version: `npm version <major|minor|patch>`
2. Push tags: `git push origin main --follow-tags`
3. GitHub Actions will automatically build and sign the plugin

## License

See LICENSE file in the root repository.