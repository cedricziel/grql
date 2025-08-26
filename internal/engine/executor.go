package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cedricziel/grql/internal/engine/backends"
	pb "github.com/cedricziel/grql/proto"
)

// BackendClient interface for backend operations
type BackendClient interface {
	ExecuteQuery(ctx context.Context, query backends.QueryRequest) (*backends.QueryResponse, error)
	Stream(ctx context.Context, query backends.QueryRequest, ch chan<- backends.Result) error
	GetCapabilities() backends.Capabilities
}

// Executor executes query plans
type Executor struct {
	parser    *ExtendedParser
	planner   *Planner
	optimizer *Optimizer
	backends  map[Backend]BackendClient
	cache     *QueryCache
}

// NewExecutor creates a new query executor
func NewExecutor(config ExecutorConfig) *Executor {
	exec := &Executor{
		parser:    NewExtendedParser(),
		planner:   NewPlanner(),
		optimizer: NewOptimizer(),
		backends:  make(map[Backend]BackendClient),
		cache:     NewQueryCache(config.CacheSize),
	}

	// Register backends
	if config.MimirURL != "" {
		exec.backends[BackendMimir] = backends.NewMimirAdapter(config.MimirURL, config.TenantID)
	}
	if config.LokiURL != "" {
		exec.backends[BackendLoki] = backends.NewLokiAdapter(config.LokiURL, config.TenantID)
	}
	if config.TempoURL != "" {
		exec.backends[BackendTempo] = backends.NewTempoAdapter(config.TempoURL, config.TenantID)
	}

	return exec
}

// ExecutorConfig configures the query executor
type ExecutorConfig struct {
	MimirURL  string
	LokiURL   string
	TempoURL  string
	TenantID  string
	CacheSize int
}

// ExecuteQuery executes a SQL query and returns results
func (e *Executor) ExecuteQuery(ctx context.Context, sqlQuery string, params map[string]string) (*pb.QueryResponse, error) {
	startTime := time.Now()

	// Check cache first
	cacheKey := e.cache.GenerateKey(sqlQuery, params)
	if cached := e.cache.Get(cacheKey); cached != nil {
		return cached, nil
	}

	// Parse query
	query, err := e.parser.ParseNRQL(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	// Apply parameters
	e.applyParameters(query, params)

	// Create logical plan
	logicalPlan, err := e.planner.CreateLogicalPlan(query)
	if err != nil {
		return nil, fmt.Errorf("failed to create logical plan: %w", err)
	}

	// Optimize plan
	optimizedPlan, err := e.optimizer.Optimize(logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("failed to optimize plan: %w", err)
	}

	// Create physical plan
	physicalPlan, err := e.planner.CreatePhysicalPlan(optimizedPlan)
	if err != nil {
		return nil, fmt.Errorf("failed to create physical plan: %w", err)
	}

	// Execute plan
	resultSet, err := e.executePlan(ctx, physicalPlan)
	if err != nil {
		return nil, fmt.Errorf("failed to execute plan: %w", err)
	}
	defer resultSet.Close()

	// Convert to protobuf response
	response := e.convertToProtoResponse(resultSet, time.Since(startTime))

	// Cache the result
	e.cache.Set(cacheKey, response)

	return response, nil
}

// StreamQuery executes a SQL query and streams results
func (e *Executor) StreamQuery(ctx context.Context, sqlQuery string, params map[string]string, stream pb.QueryService_StreamQueryServer) error {
	// Parse query
	query, err := e.parser.ParseNRQL(sqlQuery)
	if err != nil {
		return fmt.Errorf("failed to parse query: %w", err)
	}

	// Apply parameters
	e.applyParameters(query, params)

	// Create logical plan
	logicalPlan, err := e.planner.CreateLogicalPlan(query)
	if err != nil {
		return fmt.Errorf("failed to create logical plan: %w", err)
	}

	// Optimize plan
	optimizedPlan, err := e.optimizer.Optimize(logicalPlan)
	if err != nil {
		return fmt.Errorf("failed to optimize plan: %w", err)
	}

	// Create physical plan
	physicalPlan, err := e.planner.CreatePhysicalPlan(optimizedPlan)
	if err != nil {
		return fmt.Errorf("failed to create physical plan: %w", err)
	}

	// Execute plan with streaming
	return e.executePlanStreaming(ctx, physicalPlan, stream)
}

// applyParameters applies query parameters
func (e *Executor) applyParameters(query *Query, params map[string]string) {
	// Apply parameters to query filters and time range
	// This is a simplified implementation
	for key, value := range params {
		switch key {
		case "since":
			if duration, err := time.ParseDuration(value); err == nil {
				query.TimeRange.Since = time.Now().Add(-duration)
			}
		case "until":
			if duration, err := time.ParseDuration(value); err == nil {
				query.TimeRange.Until = time.Now().Add(-duration)
			}
		case "limit":
			_, _ = fmt.Sscanf(value, "%d", &query.Limit) // Ignoring error, using default if invalid
		}
	}
}

// executePlan executes a physical plan
func (e *Executor) executePlan(ctx context.Context, plan *PhysicalPlan) (ResultSet, error) {
	// Execute based on node type
	switch node := plan.Root.(type) {
	case *ScanNode:
		return e.executeScan(ctx, node)
	case *FilterNode:
		return node.Execute(ctx)
	case *AggregateNode:
		return node.Execute(ctx)
	case *JoinNode:
		return node.Execute(ctx)
	case *UnionNode:
		return node.Execute(ctx)
	default:
		return nil, fmt.Errorf("unsupported node type: %T", node)
	}
}

// executeScan executes a scan node against appropriate backend
func (e *Executor) executeScan(ctx context.Context, node *ScanNode) (ResultSet, error) {
	backend, ok := e.backends[node.Backend]
	if !ok {
		return nil, fmt.Errorf("backend not configured: %s", node.Backend)
	}

	// Convert to backend query request
	request := backends.QueryRequest{
		TimeRange: backends.TimeRange{
			Since: node.TimeRange.Since,
			Until: node.TimeRange.Until,
		},
		Filters: convertFilters(node.Filters),
		Limit:   1000, // Default limit
	}

	// Determine metric/log/trace name based on data source
	switch node.DataSource {
	case DataSourceMetrics:
		// Extract metric name from filters or use default
		request.MetricName = e.extractMetricName(node.Filters)
	case DataSourceLogs:
		// Logs don't need a specific name
	case DataSourceTraces:
		// Traces use filters directly
	}

	// Execute query
	response, err := backend.ExecuteQuery(ctx, request)
	if err != nil {
		return nil, err
	}

	// Convert to ResultSet
	return e.convertToResultSet(response), nil
}

// convertFilters converts engine filters to backend filters
func convertFilters(filters []Filter) []backends.Filter {
	result := make([]backends.Filter, len(filters))
	for i, f := range filters {
		result[i] = backends.Filter{
			Field:    f.Field,
			Operator: string(f.Operator),
			Value:    f.Value,
		}
	}
	return result
}

// extractMetricName extracts metric name from filters
func (e *Executor) extractMetricName(filters []Filter) string {
	for _, filter := range filters {
		if filter.Field == "__name__" || filter.Field == "metric" {
			if str, ok := filter.Value.(string); ok {
				return str
			}
		}
	}
	return "up" // Default metric
}

// convertToResultSet converts backend response to ResultSet
func (e *Executor) convertToResultSet(response *backends.QueryResponse) ResultSet {
	return &BackendResultSet{
		response: response,
		index:    -1,
	}
}

// BackendResultSet implements ResultSet for backend responses
type BackendResultSet struct {
	current  Row
	response *backends.QueryResponse
	index    int
}

func (b *BackendResultSet) Next() bool {
	b.index++
	if b.index >= len(b.response.Results) {
		return false
	}

	result := b.response.Results[b.index]
	b.current = Row{
		Values: make(map[string]interface{}),
	}

	// Add labels as columns
	for k, v := range result.Labels {
		b.current.Values[k] = v
	}

	// Add values
	if len(result.Values) > 0 {
		b.current.Values["timestamp"] = result.Values[0].Timestamp
		b.current.Values["value"] = result.Values[0].Value
	}

	return true
}

func (b *BackendResultSet) Current() Row {
	return b.current
}

func (b *BackendResultSet) Close() error {
	return nil
}

func (b *BackendResultSet) Schema() Schema {
	// Build schema from first result
	if len(b.response.Results) == 0 {
		return Schema{}
	}

	columns := make([]Column, 0)

	// Add label columns
	for label := range b.response.Results[0].Labels {
		columns = append(columns, Column{
			Name: label,
			Type: DataTypeString,
		})
	}

	// Add value columns
	columns = append(columns,
		Column{Name: "timestamp", Type: DataTypeTimestamp},
		Column{Name: "value", Type: DataTypeString},
	)

	return Schema{Columns: columns}
}

// executePlanStreaming executes a plan with streaming results
func (e *Executor) executePlanStreaming(ctx context.Context, plan *PhysicalPlan, stream pb.QueryService_StreamQueryServer) error {
	// For streaming, we need to handle differently based on node type
	if scanNode, ok := plan.Root.(*ScanNode); ok {
		backend, ok := e.backends[scanNode.Backend]
		if !ok {
			return fmt.Errorf("backend not configured: %s", scanNode.Backend)
		}

		// Create result channel
		resultCh := make(chan backends.Result, 100)
		errCh := make(chan error, 1)

		// Start streaming from backend
		go func() {
			request := backends.QueryRequest{
				TimeRange: backends.TimeRange{
					Since: scanNode.TimeRange.Since,
					Until: scanNode.TimeRange.Until,
				},
				Filters: convertFilters(scanNode.Filters),
			}

			if err := backend.Stream(ctx, request, resultCh); err != nil {
				errCh <- err
			}
			close(resultCh)
		}()

		// Stream results to client
		for {
			select {
			case result, ok := <-resultCh:
				if !ok {
					return nil
				}

				// Convert and send result
				protoResult := e.convertResultToProto(result)
				if err := stream.Send(protoResult); err != nil {
					return err
				}

			case err := <-errCh:
				return err

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// For non-scan nodes, execute normally and stream results
	resultSet, err := e.executePlan(ctx, plan)
	if err != nil {
		return err
	}
	defer resultSet.Close()

	// Stream all results
	for resultSet.Next() {
		row := resultSet.Current()
		protoResult := e.convertRowToProto(row)
		if err := stream.Send(protoResult); err != nil {
			return err
		}
	}

	return nil
}

// convertToProtoResponse converts ResultSet to protobuf response
func (e *Executor) convertToProtoResponse(resultSet ResultSet, executionTime time.Duration) *pb.QueryResponse {
	results := make([]*pb.QueryResult, 0)
	schema := resultSet.Schema()

	// Collect all results
	for resultSet.Next() {
		row := resultSet.Current()
		protoResult := e.convertRowToProto(row)
		results = append(results, protoResult)
	}

	// Build column info
	columnInfo := make([]*pb.ColumnInfo, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		columnInfo = append(columnInfo, &pb.ColumnInfo{
			Name: col.Name,
			Type: string(col.Type),
		})
	}

	return &pb.QueryResponse{
		Results: results,
		Metadata: &pb.QueryMetadata{
			RowsAffected:    int64(len(results)),
			ExecutionTimeMs: executionTime.Milliseconds(),
			Columns:         columnInfo,
		},
	}
}

// convertResultToProto converts a backend result to protobuf
func (e *Executor) convertResultToProto(result backends.Result) *pb.QueryResult {
	fields := make(map[string]*pb.Value)

	// Add labels
	for k, v := range result.Labels {
		fields[k] = &pb.Value{
			Value: &pb.Value_StringValue{StringValue: v},
		}
	}

	// Add first value (for streaming)
	if len(result.Values) > 0 {
		fields["timestamp"] = &pb.Value{
			Value: &pb.Value_IntValue{IntValue: result.Values[0].Timestamp.Unix()},
		}
		fields["value"] = &pb.Value{
			Value: &pb.Value_StringValue{StringValue: result.Values[0].Value},
		}
	}

	return &pb.QueryResult{Fields: fields}
}

// convertRowToProto converts a row to protobuf
func (e *Executor) convertRowToProto(row Row) *pb.QueryResult {
	fields := make(map[string]*pb.Value)

	for key, value := range row.Values {
		fields[key] = e.convertValueToProto(value)
	}

	return &pb.QueryResult{Fields: fields}
}

// convertValueToProto converts a value to protobuf
func (e *Executor) convertValueToProto(value interface{}) *pb.Value {
	switch v := value.(type) {
	case string:
		return &pb.Value{Value: &pb.Value_StringValue{StringValue: v}}
	case int:
		return &pb.Value{Value: &pb.Value_IntValue{IntValue: int64(v)}}
	case int32:
		return &pb.Value{Value: &pb.Value_IntValue{IntValue: int64(v)}}
	case int64:
		return &pb.Value{Value: &pb.Value_IntValue{IntValue: v}}
	case float32:
		return &pb.Value{Value: &pb.Value_FloatValue{FloatValue: float64(v)}}
	case float64:
		return &pb.Value{Value: &pb.Value_FloatValue{FloatValue: v}}
	case bool:
		return &pb.Value{Value: &pb.Value_BoolValue{BoolValue: v}}
	case []byte:
		return &pb.Value{Value: &pb.Value_BytesValue{BytesValue: v}}
	case time.Time:
		return &pb.Value{Value: &pb.Value_IntValue{IntValue: v.Unix()}}
	default:
		// Convert to string as fallback
		return &pb.Value{Value: &pb.Value_StringValue{StringValue: fmt.Sprintf("%v", v)}}
	}
}

// QueryCache caches query results
type QueryCache struct {
	cache map[string]*cacheEntry
	mu    sync.RWMutex
	size  int
}

type cacheEntry struct {
	result    *pb.QueryResponse
	timestamp time.Time
}

// NewQueryCache creates a new query cache
func NewQueryCache(size int) *QueryCache {
	return &QueryCache{
		cache: make(map[string]*cacheEntry),
		size:  size,
	}
}

// GenerateKey generates a cache key for a query
func (c *QueryCache) GenerateKey(query string, params map[string]string) string {
	// Simple key generation - would use hash in production
	key := query
	for k, v := range params {
		key += fmt.Sprintf("_%s_%s", k, v)
	}
	return key
}

// Get retrieves a cached result
func (c *QueryCache) Get(key string) *pb.QueryResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.cache[key]
	if !ok {
		return nil
	}

	// Check if cache entry is still valid (5 minute TTL)
	if time.Since(entry.timestamp) > 5*time.Minute {
		return nil
	}

	return entry.result
}

// Set stores a result in cache
func (c *QueryCache) Set(key string, result *pb.QueryResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Simple LRU eviction if cache is full
	if len(c.cache) >= c.size {
		// Remove oldest entry
		var oldestKey string
		var oldestTime time.Time
		for k, v := range c.cache {
			if oldestTime.IsZero() || v.timestamp.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.timestamp
			}
		}
		delete(c.cache, oldestKey)
	}

	c.cache[key] = &cacheEntry{
		result:    result,
		timestamp: time.Now(),
	}
}
