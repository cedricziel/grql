package server

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	pb "github.com/cedricziel/grql/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type QueryServer struct {
	pb.UnimplementedQueryServiceServer
}

func New() *QueryServer {
	return &QueryServer{}
}

func (s *QueryServer) ExecuteQuery(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	startTime := time.Now()
	
	if req.Query == "" {
		return nil, status.Error(codes.InvalidArgument, "query cannot be empty")
	}
	
	log.Printf("Executing query: %s", req.Query)
	if len(req.Parameters) > 0 {
		log.Printf("Parameters: %v", req.Parameters)
	}
	
	// Parse and validate the query
	queryLower := strings.ToLower(strings.TrimSpace(req.Query))
	
	// Simple query validation
	if !isValidQuery(queryLower) {
		return &pb.QueryResponse{
			Error: "Invalid query syntax",
		}, nil
	}
	
	// Mock implementation - returns sample data
	results := generateMockResults(req)
	
	executionTime := time.Since(startTime).Milliseconds()
	
	// Build column info based on mock data
	var columns []*pb.ColumnInfo
	if len(results) > 0 {
		for key := range results[0].Fields {
			columns = append(columns, &pb.ColumnInfo{
				Name: key,
				Type: detectType(results[0].Fields[key]),
			})
		}
	}
	
	return &pb.QueryResponse{
		Results: results,
		Metadata: &pb.QueryMetadata{
			RowsAffected:    int64(len(results)),
			ExecutionTimeMs: executionTime,
			Columns:         columns,
		},
	}, nil
}

func (s *QueryServer) StreamQuery(req *pb.QueryRequest, stream pb.QueryService_StreamQueryServer) error {
	if req.Query == "" {
		return status.Error(codes.InvalidArgument, "query cannot be empty")
	}
	
	log.Printf("Streaming query: %s", req.Query)
	
	// Mock implementation - stream sample data
	results := generateMockResults(req)
	
	for _, result := range results {
		if err := stream.Send(result); err != nil {
			return err
		}
		// Simulate some processing time
		time.Sleep(10 * time.Millisecond)
	}
	
	return nil
}

func isValidQuery(query string) bool {
	// Simple validation - check if query starts with common SQL keywords
	validPrefixes := []string{"select", "insert", "update", "delete", "create", "drop", "alter"}
	for _, prefix := range validPrefixes {
		if strings.HasPrefix(query, prefix) {
			return true
		}
	}
	return false
}

func generateMockResults(req *pb.QueryRequest) []*pb.QueryResult {
	// Generate mock data based on query type
	queryLower := strings.ToLower(strings.TrimSpace(req.Query))
	
	if strings.HasPrefix(queryLower, "select") {
		// Return some sample rows
		limit := 10
		if req.Limit > 0 && req.Limit < 10 {
			limit = int(req.Limit)
		}
		
		results := make([]*pb.QueryResult, 0, limit)
		for i := 0; i < limit; i++ {
			results = append(results, &pb.QueryResult{
				Fields: map[string]*pb.Value{
					"id": {
						Value: &pb.Value_IntValue{IntValue: int64(i + 1)},
					},
					"name": {
						Value: &pb.Value_StringValue{StringValue: fmt.Sprintf("Item %d", i+1)},
					},
					"created_at": {
						Value: &pb.Value_StringValue{StringValue: time.Now().Format(time.RFC3339)},
					},
					"active": {
						Value: &pb.Value_BoolValue{BoolValue: i%2 == 0},
					},
				},
			})
		}
		return results
	}
	
	// For non-SELECT queries, return empty result
	return []*pb.QueryResult{}
}

func detectType(value *pb.Value) string {
	switch value.Value.(type) {
	case *pb.Value_StringValue:
		return "string"
	case *pb.Value_IntValue:
		return "integer"
	case *pb.Value_FloatValue:
		return "float"
	case *pb.Value_BoolValue:
		return "boolean"
	case *pb.Value_BytesValue:
		return "bytes"
	default:
		return "unknown"
	}
}