package backends

// Registry manages backend adapters
type Registry struct {
	backends map[string]BackendAdapter
}

// NewRegistry creates a new backend registry
func NewRegistry() *Registry {
	return &Registry{
		backends: make(map[string]BackendAdapter),
	}
}

// Register adds a backend adapter
func (r *Registry) Register(name string, adapter BackendAdapter) {
	r.backends[name] = adapter
}

// Get retrieves a backend adapter
func (r *Registry) Get(name string) (BackendAdapter, bool) {
	adapter, ok := r.backends[name]
	return adapter, ok
}
