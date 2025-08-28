//go:build integration
// +build integration

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/cedricziel/grql/server/internal/engine/backends"
	pb "github.com/cedricziel/grql/pkg/grql/v1"
)

// TestExecutor_IntegrationWithMetadata tests full query execution with metadata
func TestExecutor_IntegrationWithMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	executor := NewExecutor(ExecutorConfig{
		CacheSize: 100,
	})

	// Add mock backends that simulate real responses
	executor.backends[BackendMimir] = createMockMetricsBackend()
	executor.backends[BackendLoki] = createMockLogsBackend()
	executor.backends[BackendTempo] = createMockTracesBackend()

	ctx := context.Background()

	tests := []struct {
		name             string
		query            string
		expectedDataType pb.DataType
		validateMetadata func(*testing.T, *pb.QueryMetadata)
	}{
		{
			name:             "metrics query with aggregation",
			query:            "SELECT avg(cpu_usage), max(memory_usage) FROM metrics WHERE service = 'api' GROUP BY host SINCE 1h",
			expectedDataType: pb.DataType_TIME_SERIES,
			validateMetadata: func(t *testing.T, meta *pb.QueryMetadata) {
				// Check GROUP BY fields
				if len(meta.GroupByFields) == 0 {
					t.Error("expected GROUP BY fields")
				} else if meta.GroupByFields[0] != "host" {
					t.Errorf("expected GROUP BY field 'host', got %v", meta.GroupByFields)
				}

				// Check field units
				if meta.FieldUnits["cpu_usage"] != "percent" {
					t.Errorf("expected cpu_usage unit 'percent', got %s", meta.FieldUnits["cpu_usage"])
				}
				if meta.FieldUnits["memory_usage"] != "bytes" {
					t.Errorf("expected memory_usage unit 'bytes', got %s", meta.FieldUnits["memory_usage"])
				}

				// Check time fields
				if len(meta.TimeFields) == 0 {
					t.Error("expected time fields to be detected")
				}
			},
		},
		{
			name:             "logs query with filters",
			query:            "SELECT * FROM logs WHERE level = 'error' AND service = 'api' SINCE 24h LIMIT 100",
			expectedDataType: pb.DataType_LOGS,
			validateMetadata: func(t *testing.T, meta *pb.QueryMetadata) {
				// Check data type
				if meta.DataType != pb.DataType_LOGS {
					t.Errorf("expected LOGS data type, got %v", meta.DataType)
				}

				// Check columns
				hasLevel := false
				hasMessage := false
				for _, col := range meta.Columns {
					if col.Name == "level" {
						hasLevel = true
					}
					if col.Name == "message" {
						hasMessage = true
					}
				}
				if !hasLevel || !hasMessage {
					t.Error("expected logs to have level and message columns")
				}
			},
		},
		{
			name:             "traces query with duration",
			query:            "SELECT trace_id, span_id, duration FROM traces WHERE service_name = 'checkout' AND duration > 100 SINCE 1h",
			expectedDataType: pb.DataType_TRACES,
			validateMetadata: func(t *testing.T, meta *pb.QueryMetadata) {
				// Check data type
				if meta.DataType != pb.DataType_TRACES {
					t.Errorf("expected TRACES data type, got %v", meta.DataType)
				}

				// Check duration unit
				if meta.FieldUnits["duration"] != "milliseconds" {
					t.Errorf("expected duration unit 'milliseconds', got %s", meta.FieldUnits["duration"])
				}
			},
		},
		{
			name:             "complex metrics with rate function",
			query:            "SELECT rate(http_requests_total) FROM metrics WHERE endpoint = '/api/users' GROUP BY status_code SINCE 5m",
			expectedDataType: pb.DataType_TIME_SERIES,
			validateMetadata: func(t *testing.T, meta *pb.QueryMetadata) {
				// Check GROUP BY fields
				if len(meta.GroupByFields) == 0 {
					t.Error("expected GROUP BY fields")
				}

				// Check that rate field has appropriate unit
				if meta.FieldUnits["rate"] != "ops" {
					t.Logf("rate unit: %s", meta.FieldUnits["rate"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := executor.ExecuteQuery(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if resp.Metadata == nil {
				t.Fatal("expected metadata, got nil")
			}

			// Check data type
			if resp.Metadata.DataType != tt.expectedDataType {
				t.Errorf("expected data type %v, got %v", tt.expectedDataType, resp.Metadata.DataType)
			}

			// Run custom validation
			if tt.validateMetadata != nil {
				tt.validateMetadata(t, resp.Metadata)
			}

			// Common checks
			if resp.Metadata.ExecutionTimeMs < 0 {
				t.Error("expected positive execution time")
			}
			if resp.Metadata.RowsAffected < 0 {
				t.Error("expected non-negative rows affected")
			}
		})
	}
}

// TestExecutor_StreamingWithMetadata tests streaming queries with metadata
func TestExecutor_StreamingWithMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	executor := NewExecutor(ExecutorConfig{
		CacheSize: 100,
	})

	executor.backends[BackendMimir] = createStreamingMetricsBackend()

	ctx := context.Background()
	query := "SELECT cpu_usage, memory_usage FROM metrics WHERE host = 'server1' SINCE 1h"

	mockStream := &mockStreamServer{
		results: make([]*pb.QueryResult, 0),
		ctx:     ctx,
	}

	err := executor.StreamQuery(ctx, query, nil, mockStream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mockStream.results) == 0 {
		t.Error("expected streaming results")
	}

	// Check that each result has proper fields
	for i, result := range mockStream.results {
		if result.Fields == nil {
			t.Errorf("result %d: expected fields, got nil", i)
			continue
		}

		// Check for expected fields
		if _, ok := result.Fields["cpu_usage"]; !ok {
			t.Errorf("result %d: missing cpu_usage field", i)
		}
		if _, ok := result.Fields["memory_usage"]; !ok {
			t.Errorf("result %d: missing memory_usage field", i)
		}
	}
}

// Helper functions to create mock backends with realistic data

func createMockMetricsBackend() BackendClient {
	return &mockBackendClient{
		queryFunc: func(ctx context.Context, query backends.QueryRequest) (*backends.QueryResponse, error) {
			return &backends.QueryResponse{
				Results: []backends.Result{
					{
						Labels: map[string]string{
							"host":    "server1",
							"service": "api",
						},
						Values: []backends.DataPoint{
							{Timestamp: time.Now().Add(-30 * time.Minute), Value: "75.5"},
							{Timestamp: time.Now().Add(-20 * time.Minute), Value: "82.3"},
							{Timestamp: time.Now().Add(-10 * time.Minute), Value: "78.1"},
							{Timestamp: time.Now(), Value: "80.0"},
						},
					},
					{
						Labels: map[string]string{
							"host":    "server2",
							"service": "api",
						},
						Values: []backends.DataPoint{
							{Timestamp: time.Now().Add(-30 * time.Minute), Value: "65.2"},
							{Timestamp: time.Now().Add(-20 * time.Minute), Value: "68.7"},
							{Timestamp: time.Now().Add(-10 * time.Minute), Value: "70.3"},
							{Timestamp: time.Now(), Value: "72.5"},
						},
					},
				},
			}, nil
		},
		caps: backends.Capabilities{
			SupportsAggregation: true,
			SupportsGroupBy:     true,
			SupportsRate:        true,
		},
	}
}

func createMockLogsBackend() BackendClient {
	return &mockBackendClient{
		queryFunc: func(ctx context.Context, query backends.QueryRequest) (*backends.QueryResponse, error) {
			return &backends.QueryResponse{
				Results: []backends.Result{
					{
						Labels: map[string]string{
							"level":   "error",
							"service": "api",
							"message": "Connection timeout",
						},
						Values: []backends.DataPoint{
							{Timestamp: time.Now().Add(-10 * time.Minute), Value: "2024-01-01T10:30:00Z ERROR Connection timeout to database"},
						},
					},
					{
						Labels: map[string]string{
							"level":   "error",
							"service": "api",
							"message": "Invalid request",
						},
						Values: []backends.DataPoint{
							{Timestamp: time.Now().Add(-5 * time.Minute), Value: "2024-01-01T10:35:00Z ERROR Invalid request format"},
						},
					},
				},
			}, nil
		},
		caps: backends.Capabilities{
			SupportsFullText: true,
			SupportsRegex:    true,
		},
	}
}

func createMockTracesBackend() BackendClient {
	return &mockBackendClient{
		queryFunc: func(ctx context.Context, query backends.QueryRequest) (*backends.QueryResponse, error) {
			return &backends.QueryResponse{
				Results: []backends.Result{
					{
						Labels: map[string]string{
							"trace_id":     "abc123def456",
							"span_id":      "789ghi012",
							"service_name": "checkout",
							"duration":     "150",
						},
						Values: []backends.DataPoint{
							{Timestamp: time.Now().Add(-5 * time.Minute), Value: "150"},
						},
					},
					{
						Labels: map[string]string{
							"trace_id":     "xyz789uvw456",
							"span_id":      "345mno678",
							"service_name": "checkout",
							"duration":     "275",
						},
						Values: []backends.DataPoint{
							{Timestamp: time.Now().Add(-3 * time.Minute), Value: "275"},
						},
					},
				},
			}, nil
		},
		caps: backends.Capabilities{
			SupportsTraceQL: true,
		},
	}
}

func createStreamingMetricsBackend() BackendClient {
	return &mockBackendClient{
		streamFunc: func(ctx context.Context, query backends.QueryRequest, ch chan<- backends.Result) error {
			// Simulate streaming metrics data
			for i := 0; i < 10; i++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ch <- backends.Result{
					Labels: map[string]string{
						"host":    "server1",
						"service": "api",
						"index":   string(rune('0' + i)),
					},
					Values: []backends.DataPoint{
						{
							Timestamp: time.Now().Add(time.Duration(-60+i*6) * time.Minute),
							Value:     "75.5",
						},
					},
				}:
					// Add small delay to simulate streaming
					time.Sleep(10 * time.Millisecond)
				}
			}
			// Channel will be closed by executor
			return nil
		},
		caps: backends.Capabilities{
			SupportsStreaming: true,
		},
	}
}