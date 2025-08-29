//go:build integration
// +build integration

package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/cedricziel/grql/pkg/grql/v1"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockGRPCServer implements a mock GRQL server for testing
type mockGRPCServer struct {
	pb.UnimplementedQueryServiceServer
	responses map[string]*pb.QueryResponse
	errors    map[string]error
}

func (m *mockGRPCServer) ExecuteQuery(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if err, ok := m.errors[req.Query]; ok {
		return nil, err
	}
	if resp, ok := m.responses[req.Query]; ok {
		return resp, nil
	}
	// Default response
	return &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"value": {Value: &pb.Value_IntValue{IntValue: 42}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			Columns: []*pb.ColumnInfo{
				{Name: "value", Type: "INT64"},
			},
			RowsAffected:    1,
			ExecutionTimeMs: 10,
		},
	}, nil
}

func (m *mockGRPCServer) StreamQuery(req *pb.QueryRequest, stream pb.QueryService_StreamQueryServer) error {
	// Send 5 results for streaming
	for i := 0; i < 5; i++ {
		result := &pb.QueryResult{
			Fields: map[string]*pb.Value{
				"index": {Value: &pb.Value_IntValue{IntValue: int64(i)}},
				"value": {Value: &pb.Value_FloatValue{FloatValue: float64(i) * 10.5}},
			},
		}
		if err := stream.Send(result); err != nil {
			return err
		}
		time.Sleep(10 * time.Millisecond) // Simulate streaming delay
	}
	return nil
}

func setupMockGRPCServer(t *testing.T) (string, func()) {
	// Create a listener on a random port
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	mockServer := &mockGRPCServer{
		responses: make(map[string]*pb.QueryResponse),
		errors:    make(map[string]error),
	}

	// Add some test responses
	mockServer.responses["SELECT cpu FROM metrics"] = &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"timestamp": {Value: &pb.Value_IntValue{IntValue: time.Now().Unix()}},
					"cpu":       {Value: &pb.Value_FloatValue{FloatValue: 75.5}},
				},
			},
		},
		Metadata: &pb.QueryMetadata{
			DataType:   pb.DataType_TIME_SERIES,
			TimeFields: []string{"timestamp"},
			FieldUnits: map[string]string{"cpu": "percent"},
			Columns: []*pb.ColumnInfo{
				{Name: "timestamp", Type: "TIMESTAMP"},
				{Name: "cpu", Type: "FLOAT64"},
			},
		},
	}

	mockServer.responses["SELECT * FROM logs WHERE level = 'error'"] = &pb.QueryResponse{
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
	}

	// Add error response
	mockServer.errors["SELECT error"] = status.Error(codes.Internal, "simulated error")

	pb.RegisterQueryServiceServer(grpcServer, mockServer)

	// Start server
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Extract host and port
	addr := lis.Addr().String()
	host, port, _ := net.SplitHostPort(addr)

	// Return address and cleanup function
	cleanup := func() {
		grpcServer.Stop()
		lis.Close()
	}

	return fmt.Sprintf("%s:%s", host, port), cleanup
}

func TestDatasource_Integration_QueryData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	addr, cleanup := setupMockGRPCServer(t)
	defer cleanup()

	// Parse host and port
	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)

	// Create datasource with mock server settings
	settings := backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(fmt.Sprintf(`{"host": "%s", "port": %d}`, host, port)),
	}

	ds, err := NewDatasource(context.Background(), settings)
	if err != nil {
		t.Fatalf("Failed to create datasource: %v", err)
	}
	defer ds.(*Datasource).Dispose()

	tests := []struct {
		name        string
		query       string
		expectError bool
		validate    func(*testing.T, backend.DataResponse)
	}{
		{
			name:        "metrics query",
			query:       "SELECT cpu FROM metrics",
			expectError: false,
			validate: func(t *testing.T, resp backend.DataResponse) {
				if resp.Error != nil {
					t.Errorf("unexpected error: %v", resp.Error)
				}
				if len(resp.Frames) == 0 {
					t.Error("expected frames, got none")
				}
				frame := resp.Frames[0]
				if frame.Meta.PreferredVisualization != "graph" {
					t.Errorf("expected graph visualization, got %v", frame.Meta.PreferredVisualization)
				}
			},
		},
		{
			name:        "logs query",
			query:       "SELECT * FROM logs WHERE level = 'error'",
			expectError: false,
			validate: func(t *testing.T, resp backend.DataResponse) {
				if resp.Error != nil {
					t.Errorf("unexpected error: %v", resp.Error)
				}
				if len(resp.Frames) == 0 {
					t.Error("expected frames, got none")
				}
				frame := resp.Frames[0]
				if frame.Meta.PreferredVisualization != "logs" {
					t.Errorf("expected logs visualization, got %v", frame.Meta.PreferredVisualization)
				}
			},
		},
		{
			name:        "error query",
			query:       "SELECT error",
			expectError: true,
			validate: func(t *testing.T, resp backend.DataResponse) {
				if resp.Error == nil {
					t.Error("expected error, got nil")
				}
			},
		},
		{
			name:        "empty query",
			query:       "",
			expectError: true,
			validate: func(t *testing.T, resp backend.DataResponse) {
				if resp.Error == nil {
					t.Error("expected error for empty query")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryJSON, _ := json.Marshal(map[string]interface{}{
				"rawQuery": tt.query,
			})

			req := &backend.QueryDataRequest{
				Queries: []backend.DataQuery{
					{
						RefID: "A",
						JSON:  queryJSON,
					},
				},
			}

			resp, err := ds.(*Datasource).QueryData(context.Background(), req)
			if err != nil {
				t.Fatalf("QueryData error: %v", err)
			}

			if len(resp.Responses) != 1 {
				t.Fatalf("expected 1 response, got %d", len(resp.Responses))
			}

			dataResp := resp.Responses["A"]
			if tt.validate != nil {
				tt.validate(t, dataResp)
			}
		})
	}
}

func TestDatasource_Integration_StreamingQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	addr, cleanup := setupMockGRPCServer(t)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)

	// Create client
	client, err := NewGrqlClient(&models.PluginSettings{
		Host: host,
		Port: port,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test streaming
	ctx := context.Background()
	stream, err := client.StreamQuery(ctx, "SELECT * FROM metrics", nil)
	if err != nil {
		t.Fatalf("StreamQuery error: %v", err)
	}

	results := make([]*pb.QueryResult, 0)
	for {
		result, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Stream receive error: %v", err)
		}
		results = append(results, result)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 streaming results, got %d", len(results))
	}

	// Verify results are in order
	for i, result := range results {
		if index, ok := result.Fields["index"]; ok {
			if idx := index.GetIntValue(); idx != int64(i) {
				t.Errorf("result %d: expected index %d, got %d", i, i, idx)
			}
		}
	}
}

func TestDatasource_Integration_CheckHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	addr, cleanup := setupMockGRPCServer(t)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)

	settings := backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(fmt.Sprintf(`{"host": "%s", "port": %d}`, host, port)),
	}

	ds, err := NewDatasource(context.Background(), settings)
	if err != nil {
		t.Fatalf("Failed to create datasource: %v", err)
	}
	defer ds.(*Datasource).Dispose()

	// Test health check
	healthResp, err := ds.(*Datasource).CheckHealth(context.Background(), &backend.CheckHealthRequest{})
	if err != nil {
		t.Fatalf("CheckHealth error: %v", err)
	}

	if healthResp.Status != backend.HealthStatusOk {
		t.Errorf("expected healthy status, got %v: %s", healthResp.Status, healthResp.Message)
	}
}

func TestDatasource_Integration_ConcurrentQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	addr, cleanup := setupMockGRPCServer(t)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)

	settings := backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(fmt.Sprintf(`{"host": "%s", "port": %d}`, host, port)),
	}

	ds, err := NewDatasource(context.Background(), settings)
	if err != nil {
		t.Fatalf("Failed to create datasource: %v", err)
	}
	defer ds.(*Datasource).Dispose()

	// Run multiple queries concurrently
	numQueries := 10
	done := make(chan bool, numQueries)

	for i := 0; i < numQueries; i++ {
		go func(idx int) {
			queryJSON, _ := json.Marshal(map[string]interface{}{
				"rawQuery": "SELECT cpu FROM metrics",
			})

			req := &backend.QueryDataRequest{
				Queries: []backend.DataQuery{
					{
						RefID: fmt.Sprintf("Q%d", idx),
						JSON:  queryJSON,
					},
				},
			}

			resp, err := ds.(*Datasource).QueryData(context.Background(), req)
			if err != nil {
				t.Errorf("Query %d error: %v", idx, err)
			}
			if len(resp.Responses) != 1 {
				t.Errorf("Query %d: expected 1 response, got %d", idx, len(resp.Responses))
			}
			done <- true
		}(i)
	}

	// Wait for all queries to complete
	timeout := time.After(5 * time.Second)
	for i := 0; i < numQueries; i++ {
		select {
		case <-done:
			// Query completed
		case <-timeout:
			t.Fatal("Timeout waiting for concurrent queries")
		}
	}
}

func TestDatasource_Integration_ConnectionFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Try to connect to a non-existent server
	settings := backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(`{"host": "localhost", "port": 59999}`),
	}

	ds, err := NewDatasource(context.Background(), settings)
	if err != nil {
		t.Fatalf("Failed to create datasource: %v", err)
	}
	defer ds.(*Datasource).Dispose()

	// Query should fail
	queryJSON, _ := json.Marshal(map[string]interface{}{
		"rawQuery": "SELECT * FROM metrics",
	})

	req := &backend.QueryDataRequest{
		Queries: []backend.DataQuery{
			{
				RefID: "A",
				JSON:  queryJSON,
			},
		},
	}

	resp, err := ds.(*Datasource).QueryData(context.Background(), req)
	if err != nil {
		t.Fatalf("QueryData error: %v", err)
	}

	dataResp := resp.Responses["A"]
	if dataResp.Error == nil {
		t.Error("expected error for connection failure")
	}

	// Health check should also fail
	healthResp, err := ds.(*Datasource).CheckHealth(context.Background(), &backend.CheckHealthRequest{})
	if err != nil {
		t.Fatalf("CheckHealth error: %v", err)
	}

	if healthResp.Status != backend.HealthStatusError {
		t.Errorf("expected error status for failed connection, got %v", healthResp.Status)
	}
}
