package plugin

import (
	"testing"
	"time"

	pb "github.com/cedricziel/grql/pkg/grql/v1"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func TestConvertToDataFrame_AllValueTypes(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name         string
		response     *pb.QueryResponse
		validateFunc func(*testing.T, *data.Frame)
	}{
		{
			name: "string values",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"name":    {Value: &pb.Value_StringValue{StringValue: "Alice"}},
							"status":  {Value: &pb.Value_StringValue{StringValue: "active"}},
							"message": {Value: &pb.Value_StringValue{StringValue: "Hello, World!"}},
						},
					},
					{
						Fields: map[string]*pb.Value{
							"name":    {Value: &pb.Value_StringValue{StringValue: "Bob"}},
							"status":  {Value: &pb.Value_StringValue{StringValue: "inactive"}},
							"message": {Value: &pb.Value_StringValue{StringValue: "Goodbye!"}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "name", Type: "STRING"},
						{Name: "status", Type: "STRING"},
						{Name: "message", Type: "STRING"},
					},
				},
			},
			validateFunc: func(t *testing.T, frame *data.Frame) {
				if len(frame.Fields) != 3 {
					t.Errorf("expected 3 fields, got %d", len(frame.Fields))
				}
				// Check first row values
				if frame.Fields[0].At(0) != nil {
					val := *(frame.Fields[0].At(0).(*string))
					if val != "Alice" {
						t.Errorf("expected 'Alice', got '%s'", val)
					}
				}
			},
		},
		{
			name: "integer values",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"id":    {Value: &pb.Value_IntValue{IntValue: 1}},
							"count": {Value: &pb.Value_IntValue{IntValue: 100}},
							"size":  {Value: &pb.Value_IntValue{IntValue: 1024}},
						},
					},
					{
						Fields: map[string]*pb.Value{
							"id":    {Value: &pb.Value_IntValue{IntValue: 2}},
							"count": {Value: &pb.Value_IntValue{IntValue: 200}},
							"size":  {Value: &pb.Value_IntValue{IntValue: 2048}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "id", Type: "INT64"},
						{Name: "count", Type: "INT64"},
						{Name: "size", Type: "INT64"},
					},
				},
			},
			validateFunc: func(t *testing.T, frame *data.Frame) {
				if len(frame.Fields) != 3 {
					t.Errorf("expected 3 fields, got %d", len(frame.Fields))
				}
				// Check integer values
				if frame.Fields[0].At(0) != nil {
					val := *(frame.Fields[0].At(0).(*int64))
					if val != 1 {
						t.Errorf("expected 1, got %d", val)
					}
				}
			},
		},
		{
			name: "float values",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"temperature": {Value: &pb.Value_FloatValue{FloatValue: 23.5}},
							"humidity":    {Value: &pb.Value_FloatValue{FloatValue: 65.2}},
							"pressure":    {Value: &pb.Value_FloatValue{FloatValue: 1013.25}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "temperature", Type: "FLOAT64"},
						{Name: "humidity", Type: "FLOAT64"},
						{Name: "pressure", Type: "FLOAT64"},
					},
				},
			},
			validateFunc: func(t *testing.T, frame *data.Frame) {
				if frame.Fields[0].At(0) != nil {
					val := *(frame.Fields[0].At(0).(*float64))
					if val != 23.5 {
						t.Errorf("expected 23.5, got %f", val)
					}
				}
			},
		},
		{
			name: "boolean values",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"is_active":   {Value: &pb.Value_BoolValue{BoolValue: true}},
							"is_verified": {Value: &pb.Value_BoolValue{BoolValue: false}},
							"has_access":  {Value: &pb.Value_BoolValue{BoolValue: true}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "is_active", Type: "BOOL"},
						{Name: "is_verified", Type: "BOOL"},
						{Name: "has_access", Type: "BOOL"},
					},
				},
			},
			validateFunc: func(t *testing.T, frame *data.Frame) {
				if frame.Fields[0].At(0) != nil {
					val := *(frame.Fields[0].At(0).(*bool))
					if val != true {
						t.Errorf("expected true, got %v", val)
					}
				}
			},
		},
		{
			name: "bytes values",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"data":    {Value: &pb.Value_BytesValue{BytesValue: []byte("binary data")}},
							"hash":    {Value: &pb.Value_BytesValue{BytesValue: []byte{0x01, 0x02, 0x03, 0x04}}},
							"payload": {Value: &pb.Value_BytesValue{BytesValue: []byte("test payload")}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "data", Type: "BYTES"},
						{Name: "hash", Type: "BYTES"},
						{Name: "payload", Type: "STRING"}, // Test bytes treated as string
					},
				},
			},
			validateFunc: func(t *testing.T, frame *data.Frame) {
				// Bytes are converted to strings in Grafana
				if frame.Fields[0].At(0) != nil {
					val := *(frame.Fields[0].At(0).(*string))
					if val != "binary data" {
						t.Errorf("expected 'binary data', got '%s'", val)
					}
				}
				// Check payload (bytes treated as string due to column type)
				if frame.Fields[2].At(0) != nil {
					val := *(frame.Fields[2].At(0).(*string))
					if val != "test payload" {
						t.Errorf("expected 'test payload', got '%s'", val)
					}
				}
			},
		},
		{
			name: "mixed value types",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"id":       {Value: &pb.Value_IntValue{IntValue: 1}},
							"name":     {Value: &pb.Value_StringValue{StringValue: "Test"}},
							"score":    {Value: &pb.Value_FloatValue{FloatValue: 95.5}},
							"passed":   {Value: &pb.Value_BoolValue{BoolValue: true}},
							"metadata": {Value: &pb.Value_BytesValue{BytesValue: []byte("meta")}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "id", Type: "INT64"},
						{Name: "name", Type: "STRING"},
						{Name: "score", Type: "FLOAT64"},
						{Name: "passed", Type: "BOOL"},
						{Name: "metadata", Type: "BYTES"},
					},
				},
			},
			validateFunc: func(t *testing.T, frame *data.Frame) {
				if len(frame.Fields) != 5 {
					t.Errorf("expected 5 fields, got %d", len(frame.Fields))
				}
				// Verify each field type
				if frame.Fields[0].Type() != data.FieldTypeNullableInt64 {
					t.Errorf("field 0: expected int64, got %v", frame.Fields[0].Type())
				}
				if frame.Fields[1].Type() != data.FieldTypeNullableString {
					t.Errorf("field 1: expected string, got %v", frame.Fields[1].Type())
				}
				if frame.Fields[2].Type() != data.FieldTypeNullableFloat64 {
					t.Errorf("field 2: expected float64, got %v", frame.Fields[2].Type())
				}
				if frame.Fields[3].Type() != data.FieldTypeNullableBool {
					t.Errorf("field 3: expected bool, got %v", frame.Fields[3].Type())
				}
				if frame.Fields[4].Type() != data.FieldTypeNullableString {
					t.Errorf("field 4: expected string (for bytes), got %v", frame.Fields[4].Type())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := ds.convertToDataFrame(tt.response, "", backend.TimeRange{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if frame == nil {
				t.Fatal("expected frame, got nil")
			}
			if tt.validateFunc != nil {
				tt.validateFunc(t, frame)
			}
		})
	}
}

func TestConvertToDataFrame_NullValues(t *testing.T) {
	ds := &Datasource{}

	response := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"id":   {Value: &pb.Value_IntValue{IntValue: 1}},
					"name": {Value: &pb.Value_StringValue{StringValue: "Alice"}},
					"age":  nil, // Null value
				},
			},
			{
				Fields: map[string]*pb.Value{
					"id":   {Value: &pb.Value_IntValue{IntValue: 2}},
					"name": nil, // Null value
					"age":  {Value: &pb.Value_IntValue{IntValue: 25}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			Columns: []*pb.ColumnInfo{
				{Name: "id", Type: "INT64"},
				{Name: "name", Type: "STRING"},
				{Name: "age", Type: "INT64"},
			},
		},
	}

	frame, err := ds.convertToDataFrame(response, "", backend.TimeRange{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Debug: Print field order and values
	for i, field := range frame.Fields {
		t.Logf("Field %d: %s", i, field.Name)
		for j := 0; j < field.Len(); j++ {
			t.Logf("  Row %d: %v", j, field.At(j))
		}
	}

	// Find fields by name instead of assuming order
	var nameField, ageField *data.Field
	for _, field := range frame.Fields {
		switch field.Name {
		case "name":
			nameField = field
		case "age":
			ageField = field
		}
	}

	if nameField == nil || ageField == nil {
		t.Fatal("name or age field not found")
	}

	// Check that null values are handled correctly
	// Note: At() returns interface{}, so we need to check the actual value
	nameVal := nameField.At(1)
	if nameVal != nil {
		// Check if it's a typed nil pointer
		if strPtr, ok := nameVal.(*string); !ok || strPtr != nil {
			t.Errorf("expected nil for null name value, got %v", nameVal)
		}
	}

	ageVal := ageField.At(0)
	if ageVal != nil {
		// Check if it's a typed nil pointer
		if intPtr, ok := ageVal.(*int64); !ok || intPtr != nil {
			t.Errorf("expected nil for null age value, got %v", ageVal)
		}
	}
}

func TestConvertToDataFrame_TimestampFormats(t *testing.T) {
	ds := &Datasource{}

	now := time.Now()
	nowUnix := now.Unix()
	nowUnixMs := now.UnixNano() / 1000000
	nowRFC3339 := now.Format(time.RFC3339)

	tests := []struct {
		name         string
		response     *pb.QueryResponse
		validateFunc func(*testing.T, *data.Frame)
	}{
		{
			name: "unix seconds timestamp",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"timestamp": {Value: &pb.Value_IntValue{IntValue: nowUnix}},
							"value":     {Value: &pb.Value_FloatValue{FloatValue: 100.0}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "timestamp", Type: "TIMESTAMP"},
						{Name: "value", Type: "FLOAT64"},
					},
				},
			},
			validateFunc: func(t *testing.T, frame *data.Frame) {
				if frame.Fields[0].Type() != data.FieldTypeNullableTime {
					t.Errorf("expected time field, got %v", frame.Fields[0].Type())
				}
				if frame.Fields[0].At(0) != nil {
					ts := frame.Fields[0].At(0).(*time.Time)
					if ts.Unix() != nowUnix {
						t.Errorf("expected unix timestamp %d, got %d", nowUnix, ts.Unix())
					}
				}
			},
		},
		{
			name: "unix milliseconds timestamp",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"timestamp": {Value: &pb.Value_IntValue{IntValue: nowUnixMs}},
							"value":     {Value: &pb.Value_FloatValue{FloatValue: 100.0}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "timestamp", Type: "TIMESTAMP"},
						{Name: "value", Type: "FLOAT64"},
					},
				},
			},
			validateFunc: func(t *testing.T, frame *data.Frame) {
				if frame.Fields[0].Type() != data.FieldTypeNullableTime {
					t.Errorf("expected time field, got %v", frame.Fields[0].Type())
				}
				if frame.Fields[0].At(0) != nil {
					ts := frame.Fields[0].At(0).(*time.Time)
					// Check that milliseconds are preserved (within 1 second tolerance)
					diff := ts.UnixNano()/1000000 - nowUnixMs
					if diff < -1000 || diff > 1000 {
						t.Errorf("expected unix ms timestamp ~%d, got %d (diff: %d)",
							nowUnixMs, ts.UnixNano()/1000000, diff)
					}
				}
			},
		},
		{
			name: "RFC3339 string timestamp",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"timestamp": {Value: &pb.Value_StringValue{StringValue: nowRFC3339}},
							"value":     {Value: &pb.Value_FloatValue{FloatValue: 100.0}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "timestamp", Type: "TIMESTAMP"},
						{Name: "value", Type: "FLOAT64"},
					},
				},
			},
			validateFunc: func(t *testing.T, frame *data.Frame) {
				if frame.Fields[0].Type() != data.FieldTypeNullableTime {
					t.Errorf("expected time field, got %v", frame.Fields[0].Type())
				}
				if frame.Fields[0].At(0) != nil {
					ts := frame.Fields[0].At(0).(*time.Time)
					// Compare formatted strings due to potential precision differences
					if ts.Format(time.RFC3339) != nowRFC3339 {
						t.Errorf("expected timestamp %s, got %s", nowRFC3339, ts.Format(time.RFC3339))
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := ds.convertToDataFrame(tt.response, "", backend.TimeRange{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.validateFunc != nil {
				tt.validateFunc(t, frame)
			}
		})
	}
}

func TestConvertToDataFrame_LargeDataset(t *testing.T) {
	ds := &Datasource{}

	// Create a large dataset
	numRows := 10000
	results := make([]*pb.QueryResult, numRows)
	for i := 0; i < numRows; i++ {
		results[i] = &pb.QueryResult{
			Fields: map[string]*pb.Value{
				"id":    {Value: &pb.Value_IntValue{IntValue: int64(i)}},
				"value": {Value: &pb.Value_FloatValue{FloatValue: float64(i) * 1.5}},
				"name":  {Value: &pb.Value_StringValue{StringValue: "item"}},
			},
		}
	}

	response := &pb.QueryResponse{
		Results: results,
		Metadata: &pb.QueryMetadata{
			Columns: []*pb.ColumnInfo{
				{Name: "id", Type: "INT64"},
				{Name: "value", Type: "FLOAT64"},
				{Name: "name", Type: "STRING"},
			},
			RowsAffected: int64(numRows),
		},
	}

	start := time.Now()
	frame, err := ds.convertToDataFrame(response, "", backend.TimeRange{})
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if frame.Rows() != numRows {
		t.Errorf("expected %d rows, got %d", numRows, frame.Rows())
	}

	// Performance check - should process 10k rows in under 100ms
	if duration > 100*time.Millisecond {
		t.Logf("Warning: Large dataset processing took %v (expected < 100ms)", duration)
	}

	// Verify some sample values
	if frame.Fields[0].At(0) != nil {
		val := *(frame.Fields[0].At(0).(*int64))
		if val != 0 {
			t.Errorf("first id: expected 0, got %d", val)
		}
	}

	if frame.Fields[0].At(numRows-1) != nil {
		val := *(frame.Fields[0].At(numRows - 1).(*int64))
		if val != int64(numRows-1) {
			t.Errorf("last id: expected %d, got %d", numRows-1, val)
		}
	}
}

func TestConvertToDataFrame_FieldConfiguration(t *testing.T) {
	ds := &Datasource{}

	response := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"timestamp":    {Value: &pb.Value_IntValue{IntValue: time.Now().Unix()}},
					"cpu_usage":    {Value: &pb.Value_FloatValue{FloatValue: 75.5}},
					"memory_bytes": {Value: &pb.Value_IntValue{IntValue: 1048576}},
					"temperature":  {Value: &pb.Value_FloatValue{FloatValue: 23.5}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			DataType:   pb.DataType_TIME_SERIES,
			TimeFields: []string{"timestamp"},
			FieldUnits: map[string]string{
				"cpu_usage":    "percent",
				"memory_bytes": "bytes",
				"temperature":  "celsius",
			},
			Columns: []*pb.ColumnInfo{
				{Name: "timestamp", Type: "TIMESTAMP"},
				{Name: "cpu_usage", Type: "FLOAT64"},
				{Name: "memory_bytes", Type: "INT64"},
				{Name: "temperature", Type: "FLOAT64"},
			},
		},
	}

	frame, err := ds.convertToDataFrame(response, "", backend.TimeRange{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check field configurations
	for i, field := range frame.Fields {
		if field.Config == nil {
			t.Errorf("field %d (%s): expected config, got nil", i, field.Name)
			continue
		}

		// Check display name
		if field.Config.DisplayName == "" {
			t.Errorf("field %d (%s): expected display name", i, field.Name)
		}

		// Check units
		switch field.Name {
		case "cpu_usage":
			if field.Config.Unit != "percent" {
				t.Errorf("cpu_usage: expected unit 'percent', got '%s'", field.Config.Unit)
			}
		case "memory_bytes":
			if field.Config.Unit != "bytes" {
				t.Errorf("memory_bytes: expected unit 'bytes', got '%s'", field.Config.Unit)
			}
		case "temperature":
			if field.Config.Unit != "celsius" {
				t.Errorf("temperature: expected unit 'celsius', got '%s'", field.Config.Unit)
			}
		case "timestamp":
			if field.Config.Interval == 0 {
				t.Error("timestamp: expected interval to be set")
			}
		}
	}
}

func TestConvertToDataFrame_ErrorCases(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		name        string
		response    *pb.QueryResponse
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil response",
			response:    nil,
			expectError: true,
			errorMsg:    "invalid response: missing metadata",
		},
		{
			name: "nil metadata",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{},
			},
			expectError: true,
			errorMsg:    "invalid response: missing metadata",
		},
		{
			name: "mismatched column count",
			response: &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"a": {Value: &pb.Value_IntValue{IntValue: 1}},
							"b": {Value: &pb.Value_IntValue{IntValue: 2}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					Columns: []*pb.ColumnInfo{
						{Name: "a", Type: "INT64"},
						// Missing column b definition
					},
				},
			},
			expectError: false, // Should handle gracefully
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := ds.convertToDataFrame(tt.response, "", backend.TimeRange{})

			if tt.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("expected error '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if frame == nil && !tt.expectError {
					t.Error("expected frame, got nil")
				}
			}
		})
	}
}

func TestConvertToDataFrame_SpecialCharacters(t *testing.T) {
	ds := &Datasource{}

	// Test with special characters and unicode
	response := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"message": {Value: &pb.Value_StringValue{StringValue: "Hello 世界! 🌍"}},
					"path":    {Value: &pb.Value_StringValue{StringValue: "/path/with spaces/and-special_chars!"}},
					"json":    {Value: &pb.Value_StringValue{StringValue: `{"key": "value", "nested": {"id": 123}}`}},
					"binary":  {Value: &pb.Value_BytesValue{BytesValue: []byte{0xFF, 0xFE, 0xFD, 0xFC}}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			Columns: []*pb.ColumnInfo{
				{Name: "message", Type: "STRING"},
				{Name: "path", Type: "STRING"},
				{Name: "json", Type: "STRING"},
				{Name: "binary", Type: "BYTES"},
			},
		},
	}

	frame, err := ds.convertToDataFrame(response, "", backend.TimeRange{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify special characters are preserved
	if frame.Fields[0].At(0) != nil {
		val := *(frame.Fields[0].At(0).(*string))
		if val != "Hello 世界! 🌍" {
			t.Errorf("unicode not preserved: got '%s'", val)
		}
	}

	// Verify JSON string is preserved
	if frame.Fields[2].At(0) != nil {
		val := *(frame.Fields[2].At(0).(*string))
		if val != `{"key": "value", "nested": {"id": 123}}` {
			t.Errorf("JSON string not preserved: got '%s'", val)
		}
	}
}
