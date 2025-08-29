package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pb "github.com/cedricziel/grql/pkg/grql/v1"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// mockGrqlClient is a mock implementation of the gRPC client for testing
type mockGrqlClient struct {
	responses map[string]*pb.QueryResponse
	errors    map[string]error
	closed    bool
}

func (m *mockGrqlClient) ExecuteQuery(ctx context.Context, query string, params map[string]string) (*pb.QueryResponse, error) {
	if err, ok := m.errors[query]; ok {
		return nil, err
	}
	if resp, ok := m.responses[query]; ok {
		return resp, nil
	}
	// Default empty response
	return &pb.QueryResponse{
		Results:  []*pb.QueryResult{},
		Metadata: &pb.QueryMetadata{},
	}, nil
}

func (m *mockGrqlClient) StreamQuery(ctx context.Context, query string, params map[string]string) (pb.QueryService_StreamQueryClient, error) {
	return nil, fmt.Errorf("streaming not implemented in mock")
}

func (m *mockGrqlClient) Close() error {
	m.closed = true
	return nil
}

func TestQueryData(t *testing.T) {
	ds := Datasource{}

	resp, err := ds.QueryData(
		context.Background(),
		&backend.QueryDataRequest{
			Queries: []backend.DataQuery{
				{RefID: "A"},
			},
		},
	)
	if err != nil {
		t.Error(err)
	}

	if len(resp.Responses) != 1 {
		t.Fatal("QueryData must return a response")
	}
}

func TestQueryData_MultipleQueries(t *testing.T) {
	mockClient := &mockGrqlClient{
		responses: make(map[string]*pb.QueryResponse),
	}

	// Set up different responses for different queries
	mockClient.responses["SELECT cpu FROM metrics"] = &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"cpu": {Value: &pb.Value_FloatValue{FloatValue: 75.5}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			Columns: []*pb.ColumnInfo{
				{Name: "cpu", Type: "FLOAT64"},
			},
		},
	}

	mockClient.responses["SELECT memory FROM metrics"] = &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"memory": {Value: &pb.Value_IntValue{IntValue: 1024}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			Columns: []*pb.ColumnInfo{
				{Name: "memory", Type: "INT64"},
			},
		},
	}

	ds := Datasource{
		client: mockClient,
	}

	queries := []backend.DataQuery{
		{
			RefID: "A",
			JSON:  json.RawMessage(`{"rawQuery": "SELECT cpu FROM metrics"}`),
		},
		{
			RefID: "B",
			JSON:  json.RawMessage(`{"rawQuery": "SELECT memory FROM metrics"}`),
		},
	}

	resp, err := ds.QueryData(
		context.Background(),
		&backend.QueryDataRequest{
			Queries: queries,
		},
	)

	if err != nil {
		t.Fatalf("QueryData error: %v", err)
	}

	if len(resp.Responses) != 2 {
		t.Fatalf("Expected 2 responses, got %d", len(resp.Responses))
	}

	// Check response A
	respA := resp.Responses["A"]
	if respA.Error != nil {
		t.Errorf("Response A error: %v", respA.Error)
	}
	if len(respA.Frames) != 1 {
		t.Errorf("Response A: expected 1 frame, got %d", len(respA.Frames))
	}

	// Check response B
	respB := resp.Responses["B"]
	if respB.Error != nil {
		t.Errorf("Response B error: %v", respB.Error)
	}
	if len(respB.Frames) != 1 {
		t.Errorf("Response B: expected 1 frame, got %d", len(respB.Frames))
	}
}

func TestQuery_EmptyQuery(t *testing.T) {
	ds := Datasource{
		client: &mockGrqlClient{},
	}

	query := backend.DataQuery{
		RefID: "A",
		JSON:  json.RawMessage(`{"rawQuery": ""}`),
	}

	resp := ds.query(context.Background(), backend.PluginContext{}, query)

	if resp.Error == nil {
		t.Error("Expected error for empty query")
	}

	if resp.Status != backend.StatusBadRequest {
		t.Errorf("Expected BadRequest status, got %d", resp.Status)
	}
}

func TestQuery_InvalidJSON(t *testing.T) {
	ds := Datasource{
		client: &mockGrqlClient{},
	}

	query := backend.DataQuery{
		RefID: "A",
		JSON:  json.RawMessage(`{invalid json}`),
	}

	resp := ds.query(context.Background(), backend.PluginContext{}, query)

	if resp.Error == nil {
		t.Error("Expected error for invalid JSON")
	}

	if resp.Status != backend.StatusBadRequest {
		t.Errorf("Expected BadRequest status, got %d", resp.Status)
	}
}

func TestQuery_ExecutionError(t *testing.T) {
	mockClient := &mockGrqlClient{
		errors: map[string]error{
			"SELECT error": fmt.Errorf("execution failed"),
		},
	}

	ds := Datasource{
		client: mockClient,
	}

	query := backend.DataQuery{
		RefID: "A",
		JSON:  json.RawMessage(`{"rawQuery": "SELECT error"}`),
	}

	resp := ds.query(context.Background(), backend.PluginContext{}, query)

	if resp.Error == nil {
		t.Error("Expected error for failed execution")
	}

	if resp.Status != backend.StatusInternal {
		t.Errorf("Expected Internal status, got %d", resp.Status)
	}
}

func TestQuery_ResponseWithError(t *testing.T) {
	mockClient := &mockGrqlClient{
		responses: map[string]*pb.QueryResponse{
			"SELECT fail": {
				Error: "query failed on server",
			},
		},
	}

	ds := Datasource{
		client: mockClient,
	}

	query := backend.DataQuery{
		RefID: "A",
		JSON:  json.RawMessage(`{"rawQuery": "SELECT fail"}`),
	}

	resp := ds.query(context.Background(), backend.PluginContext{}, query)

	if resp.Error == nil {
		t.Error("Expected error for server error response")
	}

	if resp.Status != backend.StatusInternal {
		t.Errorf("Expected Internal status, got %d", resp.Status)
	}
}

func TestConvertToDataFrame_NilResponse(t *testing.T) {
	ds := Datasource{}

	frame, err := ds.convertToDataFrame(nil, "", backend.TimeRange{})

	if err == nil {
		t.Error("Expected error for nil response")
	}

	if frame != nil {
		t.Error("Expected nil frame for nil response")
	}
}

func TestConvertToDataFrame_NilMetadata(t *testing.T) {
	ds := Datasource{}

	resp := &pb.QueryResponse{
		Results: []*pb.QueryResult{},
	}

	frame, err := ds.convertToDataFrame(resp, "", backend.TimeRange{})

	if err == nil {
		t.Error("Expected error for nil metadata")
	}

	if frame != nil {
		t.Error("Expected nil frame for nil metadata")
	}
}

func TestConvertToDataFrame_EmptyResults(t *testing.T) {
	ds := Datasource{}

	resp := &pb.QueryResponse{
		Results: []*pb.QueryResult{},
		Metadata: &pb.QueryMetadata{
			DataType: pb.DataType_TABLE,
		},
	}

	frame, err := ds.convertToDataFrame(resp, "", backend.TimeRange{})

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if frame == nil {
		t.Fatal("Expected non-nil frame")
	}

	if len(frame.Fields) != 0 {
		t.Errorf("Expected 0 fields for empty results, got %d", len(frame.Fields))
	}
}

func TestConvertToDataFrame_DataTypeVisualization(t *testing.T) {
	ds := Datasource{}

	tests := []struct {
		name                  string
		dataType              pb.DataType
		expectedVisualization data.VisType
	}{
		{
			name:                  "time series",
			dataType:              pb.DataType_TIME_SERIES,
			expectedVisualization: data.VisTypeGraph,
		},
		{
			name:                  "logs",
			dataType:              pb.DataType_LOGS,
			expectedVisualization: data.VisTypeLogs,
		},
		{
			name:                  "traces",
			dataType:              pb.DataType_TRACES,
			expectedVisualization: data.VisTypeTrace,
		},
		{
			name:                  "table",
			dataType:              pb.DataType_TABLE,
			expectedVisualization: data.VisTypeTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &pb.QueryResponse{
				Results: []*pb.QueryResult{},
				Metadata: &pb.QueryMetadata{
					DataType: tt.dataType,
				},
			}

			frame, err := ds.convertToDataFrame(resp, "", backend.TimeRange{})

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if frame.Meta == nil {
				t.Fatal("Expected frame metadata")
			}

			if frame.Meta.PreferredVisualization != tt.expectedVisualization {
				t.Errorf("Expected visualization %v, got %v",
					tt.expectedVisualization, frame.Meta.PreferredVisualization)
			}
		})
	}
}

func TestConvertToDataFrame_FieldUnits(t *testing.T) {
	ds := Datasource{}

	resp := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"cpu":        {Value: &pb.Value_FloatValue{FloatValue: 75.5}},
					"memory":     {Value: &pb.Value_IntValue{IntValue: 1024}},
					"throughput": {Value: &pb.Value_FloatValue{FloatValue: 100.0}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			Columns: []*pb.ColumnInfo{
				{Name: "cpu", Type: "FLOAT64"},
				{Name: "memory", Type: "INT64"},
				{Name: "throughput", Type: "FLOAT64"},
			},
			FieldUnits: map[string]string{
				"cpu":        "percent",
				"memory":     "bytes",
				"throughput": "Bps",
			},
		},
	}

	frame, err := ds.convertToDataFrame(resp, "", backend.TimeRange{})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(frame.Fields) != 3 {
		t.Fatalf("Expected 3 fields, got %d", len(frame.Fields))
	}

	// Check field units
	for _, field := range frame.Fields {
		expectedUnit, ok := resp.Metadata.FieldUnits[field.Name]
		if !ok {
			t.Errorf("Field %s: no unit in metadata", field.Name)
			continue
		}

		if field.Config == nil {
			t.Errorf("Field %s: missing config", field.Name)
			continue
		}

		if field.Config.Unit != expectedUnit {
			t.Errorf("Field %s: expected unit %s, got %s",
				field.Name, expectedUnit, field.Config.Unit)
		}
	}
}

func TestConvertToDataFrame_TimeFields(t *testing.T) {
	ds := Datasource{}

	resp := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"timestamp": {Value: &pb.Value_IntValue{IntValue: time.Now().Unix()}},
					"value":     {Value: &pb.Value_FloatValue{FloatValue: 42.0}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			Columns: []*pb.ColumnInfo{
				{Name: "timestamp", Type: "TIMESTAMP"},
				{Name: "value", Type: "FLOAT64"},
			},
			TimeFields: []string{"timestamp"},
		},
	}

	frame, err := ds.convertToDataFrame(resp, "", backend.TimeRange{})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Find timestamp field
	var timestampField *data.Field
	for _, field := range frame.Fields {
		if field.Name == "timestamp" {
			timestampField = field
			break
		}
	}

	if timestampField == nil {
		t.Fatal("Timestamp field not found")
	}

	if timestampField.Config == nil {
		t.Fatal("Timestamp field missing config")
	}

	if timestampField.Config.Interval != 1000 {
		t.Errorf("Expected interval 1000, got %f", timestampField.Config.Interval)
	}
}

func TestCheckHealth_Success(t *testing.T) {
	mockClient := &mockGrqlClient{
		responses: map[string]*pb.QueryResponse{
			"SELECT 1": {
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"1": {Value: &pb.Value_IntValue{IntValue: 1}},
						},
					},
				},
			},
		},
	}

	ds := Datasource{
		client: mockClient,
	}

	result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{})

	if err != nil {
		t.Fatalf("CheckHealth error: %v", err)
	}

	if result.Status != backend.HealthStatusOk {
		t.Errorf("Expected Ok status, got %v", result.Status)
	}

	if result.Message != "Successfully connected to grql server" {
		t.Errorf("Unexpected message: %s", result.Message)
	}
}

func TestCheckHealth_ConnectionError(t *testing.T) {
	mockClient := &mockGrqlClient{
		errors: map[string]error{
			"SELECT 1": fmt.Errorf("connection refused"),
		},
	}

	ds := Datasource{
		client: mockClient,
	}

	result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{})

	if err != nil {
		t.Fatalf("CheckHealth error: %v", err)
	}

	if result.Status != backend.HealthStatusError {
		t.Errorf("Expected Error status, got %v", result.Status)
	}

	if result.Message == "" {
		t.Error("Expected error message")
	}
}

func TestCheckHealth_ServerError(t *testing.T) {
	mockClient := &mockGrqlClient{
		responses: map[string]*pb.QueryResponse{
			"SELECT 1": {
				Error: "server error",
			},
		},
	}

	ds := Datasource{
		client: mockClient,
	}

	result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{})

	if err != nil {
		t.Fatalf("CheckHealth error: %v", err)
	}

	if result.Status != backend.HealthStatusError {
		t.Errorf("Expected Error status, got %v", result.Status)
	}

	if result.Message != "grql server returned error: server error" {
		t.Errorf("Unexpected message: %s", result.Message)
	}
}

func TestDispose(t *testing.T) {
	mockClient := &mockGrqlClient{}

	ds := Datasource{
		client: mockClient,
	}

	ds.Dispose()

	if !mockClient.closed {
		t.Error("Expected client to be closed")
	}

	if ds.client != nil {
		t.Error("Expected client to be nil after dispose")
	}

	// Test dispose with nil client (should not panic)
	ds2 := Datasource{
		client: nil,
	}
	ds2.Dispose() // Should not panic
}

func TestConvertToDataFrame_NoColumnInfo(t *testing.T) {
	ds := Datasource{}

	resp := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"field1": {Value: &pb.Value_StringValue{StringValue: "test"}},
					"field2": {Value: &pb.Value_IntValue{IntValue: 42}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			// No columns info
		},
	}

	frame, err := ds.convertToDataFrame(resp, "", backend.TimeRange{})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should create columns from first result
	if len(frame.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(frame.Fields))
	}

	// Check that fields were created
	fieldNames := make(map[string]bool)
	for _, field := range frame.Fields {
		fieldNames[field.Name] = true
	}

	if !fieldNames["field1"] || !fieldNames["field2"] {
		t.Error("Expected fields not found")
	}
}
