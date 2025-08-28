package engine

import (
	"testing"
	"time"

	pb "github.com/cedricziel/grql/pkg/grql/v1"
)

func TestDetectDataType(t *testing.T) {
	executor := &Executor{}

	tests := []struct {
		name       string
		schema     Schema
		timeFields []string
		expected   pb.DataType
	}{
		{
			name: "time series with timestamp and numeric",
			schema: Schema{
				Columns: []Column{
					{Name: "timestamp", Type: DataTypeTimestamp},
					{Name: "cpu_usage", Type: DataTypeFloat},
					{Name: "host", Type: DataTypeString},
				},
			},
			timeFields: []string{"timestamp"},
			expected:   pb.DataType_TIME_SERIES,
		},
		{
			name: "logs with level and message",
			schema: Schema{
				Columns: []Column{
					{Name: "timestamp", Type: DataTypeTimestamp},
					{Name: "level", Type: DataTypeString},
					{Name: "message", Type: DataTypeString},
				},
			},
			timeFields: []string{"timestamp"},
			expected:   pb.DataType_LOGS,
		},
		{
			name: "traces with span_id",
			schema: Schema{
				Columns: []Column{
					{Name: "trace_id", Type: DataTypeString},
					{Name: "span_id", Type: DataTypeString},
					{Name: "duration", Type: DataTypeInt},
				},
			},
			timeFields: []string{},
			expected:   pb.DataType_TRACES,
		},
		{
			name: "generic table",
			schema: Schema{
				Columns: []Column{
					{Name: "id", Type: DataTypeInt},
					{Name: "name", Type: DataTypeString},
					{Name: "value", Type: DataTypeFloat},
				},
			},
			timeFields: []string{},
			expected:   pb.DataType_TABLE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executor.detectDataType(tt.schema, tt.timeFields)
			if result != tt.expected {
				t.Errorf("detectDataType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDetectFieldUnit(t *testing.T) {
	executor := &Executor{}

	tests := []struct {
		fieldName string
		expected  string
	}{
		{"cpu_usage", "percent"},
		{"memory_usage", "bytes"},
		{"disk_space", "bytes"},
		{"network_throughput", "Bps"},
		{"response_latency", "milliseconds"},
		{"request_duration", "milliseconds"},
		{"error_count", "short"},
		{"request_rate", "ops"},
		{"temperature_celsius", "celsius"},
		{"completion_percentage", "percent"},
		{"data_bytes", "bytes"},
		{"elapsed_seconds", "seconds"},
		{"response_time_ms", "milliseconds"},
		{"unknown_field", ""},
	}

	for _, tt := range tests {
		t.Run(tt.fieldName, func(t *testing.T) {
			result := executor.detectFieldUnit(tt.fieldName)
			if result != tt.expected {
				t.Errorf("detectFieldUnit(%s) = %s, want %s", tt.fieldName, result, tt.expected)
			}
		})
	}
}

func TestConvertToProtoResponseWithMetadata(t *testing.T) {
	executor := &Executor{}
	
	// Create a mock result set with proper initialization
	mockResultSet := &mockResultSet{
		rows: []Row{
			{Values: map[string]interface{}{
				"timestamp": time.Now(),
				"cpu_usage": 75.5,
				"host":      "server1",
			}},
			{Values: map[string]interface{}{
				"timestamp": time.Now().Add(1 * time.Minute),
				"cpu_usage": 82.3,
				"host":      "server1",
			}},
		},
		schema: Schema{
			Columns: []Column{
				{Name: "timestamp", Type: DataTypeTimestamp},
				{Name: "cpu_usage", Type: DataTypeFloat},
				{Name: "host", Type: DataTypeString},
			},
		},
		current: 0, // Initialize current position
	}
	
	response := executor.convertToProtoResponse(mockResultSet, 100*time.Millisecond)
	
	// Check metadata
	if response.Metadata == nil {
		t.Fatal("Expected metadata to be set")
	}
	
	// Check data type detection
	if response.Metadata.DataType != pb.DataType_TIME_SERIES {
		t.Errorf("Expected DataType to be TIME_SERIES, got %v", response.Metadata.DataType)
	}
	
	// Check time fields detection
	if len(response.Metadata.TimeFields) == 0 {
		t.Error("Expected TimeFields to be detected")
	}
	
	// Check field units
	if response.Metadata.FieldUnits["cpu_usage"] != "percent" {
		t.Errorf("Expected cpu_usage unit to be 'percent', got %s", response.Metadata.FieldUnits["cpu_usage"])
	}
	
	// Check execution time
	if response.Metadata.ExecutionTimeMs != 100 {
		t.Errorf("Expected ExecutionTimeMs to be 100, got %d", response.Metadata.ExecutionTimeMs)
	}
	
	// Check rows affected
	if response.Metadata.RowsAffected != 2 {
		t.Errorf("Expected RowsAffected to be 2, got %d", response.Metadata.RowsAffected)
	}
	
	// Check columns
	if len(response.Metadata.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(response.Metadata.Columns))
	}
}

// mockResultSet implements ResultSet for testing
type mockResultSet struct {
	rows    []Row
	schema  Schema
	current int
}

func (m *mockResultSet) Next() bool {
	m.current++
	return m.current <= len(m.rows)
}

func (m *mockResultSet) Current() Row {
	if m.current > 0 && m.current <= len(m.rows) {
		return m.rows[m.current-1]
	}
	return Row{}
}

func (m *mockResultSet) Close() error {
	return nil
}

func (m *mockResultSet) Schema() Schema {
	return m.schema
}