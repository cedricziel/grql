package plugin

import (
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	pb "github.com/cedricziel/grql/pkg/grql/v1"
)

func TestConvertToDataFrame_WithMetadata(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name           string
		response       *pb.QueryResponse
		format         string
		expectedVis    data.VisType
		expectedFields int
		validateFrame  func(*testing.T, *data.Frame)
	}{
		{
			name: "time series with metadata",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"timestamp": {Value: &pb.Value_IntValue{IntValue: time.Now().Unix()}},
							"cpu_usage": {Value: &pb.Value_FloatValue{FloatValue: 75.5}},
							"host":      {Value: &pb.Value_StringValue{StringValue: "server1"}},
						},
					},
					{
						Fields: map[string]*pb.Value{
							"timestamp": {Value: &pb.Value_IntValue{IntValue: time.Now().Add(1 * time.Minute).Unix()}},
							"cpu_usage": {Value: &pb.Value_FloatValue{FloatValue: 82.3}},
							"host":      {Value: &pb.Value_StringValue{StringValue: "server1"}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					DataType:      pb.DataType_TIME_SERIES,
					TimeFields:    []string{"timestamp"},
					GroupByFields: []string{"host"},
					FieldUnits: map[string]string{
						"cpu_usage": "percent",
					},
					Columns: []*pb.ColumnInfo{
						{Name: "timestamp", Type: "TIMESTAMP"},
						{Name: "cpu_usage", Type: "FLOAT64"},
						{Name: "host", Type: "STRING"},
					},
				},
			},
			format:         "time_series",
			expectedVis:    data.VisTypeGraph,
			expectedFields: 3,
			validateFrame: func(t *testing.T, frame *data.Frame) {
				// Check if cpu_usage field has unit set
				for _, field := range frame.Fields {
					if field.Name == "cpu_usage" {
						if field.Config == nil || field.Config.Unit != "percent" {
							t.Error("expected cpu_usage field to have 'percent' unit")
						}
					}
				}
			},
		},
		{
			name: "logs with metadata",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"timestamp": {Value: &pb.Value_IntValue{IntValue: time.Now().Unix()}},
							"level":     {Value: &pb.Value_StringValue{StringValue: "error"}},
							"message":   {Value: &pb.Value_StringValue{StringValue: "Error occurred"}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					DataType:   pb.DataType_LOGS,
					TimeFields: []string{"timestamp"},
					Columns: []*pb.ColumnInfo{
						{Name: "timestamp", Type: "TIMESTAMP"},
						{Name: "level", Type: "STRING"},
						{Name: "message", Type: "STRING"},
					},
				},
			},
			format:         "logs",
			expectedVis:    data.VisTypeLogs,
			expectedFields: 3,
			validateFrame: func(t *testing.T, frame *data.Frame) {
				if frame.Meta.PreferredVisualization != data.VisTypeLogs {
					t.Errorf("expected logs visualization, got %v", frame.Meta.PreferredVisualization)
				}
			},
		},
		{
			name: "table with no specific metadata",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"id":   {Value: &pb.Value_IntValue{IntValue: 1}},
							"name": {Value: &pb.Value_StringValue{StringValue: "Item 1"}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					DataType: pb.DataType_TABLE,
					Columns: []*pb.ColumnInfo{
						{Name: "id", Type: "INT64"},
						{Name: "name", Type: "STRING"},
					},
				},
			},
			format:         "table",
			expectedVis:    data.VisTypeTable,
			expectedFields: 2,
			validateFrame: func(t *testing.T, frame *data.Frame) {
				if frame.Meta.PreferredVisualization != data.VisTypeTable {
					t.Errorf("expected table visualization, got %v", frame.Meta.PreferredVisualization)
				}
			},
		},
		{
			name: "traces with metadata",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"trace_id": {Value: &pb.Value_StringValue{StringValue: "abc123"}},
							"span_id":  {Value: &pb.Value_StringValue{StringValue: "def456"}},
							"duration": {Value: &pb.Value_IntValue{IntValue: 150}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					DataType: pb.DataType_TRACES,
					FieldUnits: map[string]string{
						"duration": "milliseconds",
					},
					Columns: []*pb.ColumnInfo{
						{Name: "trace_id", Type: "STRING"},
						{Name: "span_id", Type: "STRING"},
						{Name: "duration", Type: "INT64"},
					},
				},
			},
			format:         "traces",
			expectedVis:    data.VisTypeTrace,
			expectedFields: 3,
			validateFrame: func(t *testing.T, frame *data.Frame) {
				// Check duration field has milliseconds unit
				for _, field := range frame.Fields {
					if field.Name == "duration" {
						if field.Config == nil || field.Config.Unit != "milliseconds" {
							t.Error("expected duration field to have 'milliseconds' unit")
						}
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := ds.convertToDataFrame(tt.response, tt.format, backend.TimeRange{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if frame == nil {
				t.Fatal("expected frame, got nil")
			}

			// Check preferred visualization
			if frame.Meta != nil && frame.Meta.PreferredVisualization != tt.expectedVis {
				t.Errorf("expected visualization %v, got %v", tt.expectedVis, frame.Meta.PreferredVisualization)
			}

			// Check field count
			if len(frame.Fields) != tt.expectedFields {
				t.Errorf("expected %d fields, got %d", tt.expectedFields, len(frame.Fields))
			}

			// Run custom validation
			if tt.validateFrame != nil {
				tt.validateFrame(t, frame)
			}
		})
	}
}

func TestConvertToDataFrame_EmptyResponse(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name     string
		response *pb.QueryResponse
	}{
		{
			name: "nil response",
			response: nil,
		},
		{
			name: "nil metadata",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{},
			},
		},
		{
			name: "empty results",
			response: &pb.QueryResponse{
				Results:  []*pb.QueryResult{},
				Metadata: &pb.QueryMetadata{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := ds.convertToDataFrame(tt.response, "table", backend.TimeRange{})

			if tt.response == nil || tt.response.Metadata == nil {
				// Should return error for nil response or metadata
				if err == nil {
					t.Error("expected error for invalid response")
				}
			} else {
				// Should return empty frame for empty results
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if frame == nil {
					t.Error("expected frame, got nil")
				}
				if frame != nil && len(frame.Fields) != 0 {
					t.Error("expected no fields in empty frame")
				}
			}
		})
	}
}

func TestConvertToDataFrame_TimeFieldHandling(t *testing.T) {
	ds := &Datasource{}

	response := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"time":  {Value: &pb.Value_StringValue{StringValue: time.Now().Format(time.RFC3339)}},
					"value": {Value: &pb.Value_FloatValue{FloatValue: 100.0}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			DataType:   pb.DataType_TIME_SERIES,
			TimeFields: []string{"time"},
			Columns: []*pb.ColumnInfo{
				{Name: "time", Type: "TIMESTAMP"},
				{Name: "value", Type: "FLOAT64"},
			},
		},
	}

	frame, err := ds.convertToDataFrame(response, "", backend.TimeRange{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that time field is properly configured
	timeFieldFound := false
	for _, field := range frame.Fields {
		if field.Name == "time" {
			timeFieldFound = true
			if field.Config != nil && field.Config.Interval > 0 {
				// Time field should have interval set
				t.Logf("Time field has interval: %f", field.Config.Interval)
			} else {
				t.Error("expected time field to have interval configured")
			}
		}
	}

	if !timeFieldFound {
		t.Error("time field not found in frame")
	}
}

func TestConvertToDataFrame_BackwardCompatibility(t *testing.T) {
	ds := &Datasource{}

	// Test response without enhanced metadata (backward compatibility)
	response := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"metric": {Value: &pb.Value_FloatValue{FloatValue: 42.0}},
					"label":  {Value: &pb.Value_StringValue{StringValue: "test"}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			// No DataType, TimeFields, or FieldUnits - old format
			Columns: []*pb.ColumnInfo{
				{Name: "metric", Type: "FLOAT64"},
				{Name: "label", Type: "STRING"},
			},
			RowsAffected:    1,
			ExecutionTimeMs: 10,
		},
	}

	frame, err := ds.convertToDataFrame(response, "table", backend.TimeRange{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if frame == nil {
		t.Fatal("expected frame, got nil")
	}

	// Should default to table visualization when DataType is not set
	if frame.Meta != nil && frame.Meta.PreferredVisualization != data.VisTypeTable {
		t.Errorf("expected default table visualization for old format")
	}

	// Should still create fields correctly
	if len(frame.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(frame.Fields))
	}
}

func TestConvertToDataFrame_FieldUnitDetection(t *testing.T) {
	ds := &Datasource{}

	response := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"cpu_usage":       {Value: &pb.Value_FloatValue{FloatValue: 75.5}},
					"memory_bytes":    {Value: &pb.Value_IntValue{IntValue: 1048576}},
					"response_time":   {Value: &pb.Value_FloatValue{FloatValue: 250.5}},
					"error_rate":      {Value: &pb.Value_FloatValue{FloatValue: 0.05}},
					"network_throughput": {Value: &pb.Value_IntValue{IntValue: 1000000}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			DataType: pb.DataType_TABLE,
			FieldUnits: map[string]string{
				"cpu_usage":          "percent",
				"memory_bytes":       "bytes",
				"response_time":      "milliseconds",
				"error_rate":         "percentunit",
				"network_throughput": "Bps",
			},
			Columns: []*pb.ColumnInfo{
				{Name: "cpu_usage", Type: "FLOAT64"},
				{Name: "memory_bytes", Type: "INT64"},
				{Name: "response_time", Type: "FLOAT64"},
				{Name: "error_rate", Type: "FLOAT64"},
				{Name: "network_throughput", Type: "INT64"},
			},
		},
	}

	frame, err := ds.convertToDataFrame(response, "", backend.TimeRange{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check each field has the correct unit
	expectedUnits := map[string]string{
		"cpu_usage":          "percent",
		"memory_bytes":       "bytes",
		"response_time":      "milliseconds",
		"error_rate":         "percentunit",
		"network_throughput": "Bps",
	}

	for _, field := range frame.Fields {
		expectedUnit, ok := expectedUnits[field.Name]
		if !ok {
			continue
		}

		if field.Config == nil || field.Config.Unit != expectedUnit {
			t.Errorf("field %s: expected unit '%s', got '%s'",
				field.Name, expectedUnit, field.Config.Unit)
		}
	}
}