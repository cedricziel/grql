package backends

import "context"

// BackendAdapter is the common interface for all backend adapters
type BackendAdapter interface {
	ExecuteQuery(ctx context.Context, query QueryRequest) (*QueryResponse, error)
	GetCapabilities() Capabilities
}
