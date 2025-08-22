package server

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/cedricziel/grql/internal/engine"
	pb "github.com/cedricziel/grql/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type QueryServer struct {
	pb.UnimplementedQueryServiceServer
	executor *engine.Executor
}

func New() *QueryServer {
	// Configure executor with environment variables or config
	config := engine.ExecutorConfig{
		MimirURL:  getEnvOrDefault("MIMIR_URL", "http://localhost:9009"),
		LokiURL:   getEnvOrDefault("LOKI_URL", "http://localhost:3100"),
		TempoURL:  getEnvOrDefault("TEMPO_URL", "http://localhost:3200"),
		TenantID:  getEnvOrDefault("TENANT_ID", ""),
		CacheSize: 1000,
	}
	
	return &QueryServer{
		executor: engine.NewExecutor(config),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func (s *QueryServer) ExecuteQuery(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if req.Query == "" {
		return nil, status.Error(codes.InvalidArgument, "query cannot be empty")
	}
	
	log.Printf("Executing query: %s", req.Query)
	if len(req.Parameters) > 0 {
		log.Printf("Parameters: %v", req.Parameters)
	}
	
	// Execute query using the new engine
	response, err := s.executor.ExecuteQuery(ctx, req.Query, req.Parameters)
	if err != nil {
		log.Printf("Query execution error: %v", err)
		return &pb.QueryResponse{
			Error: fmt.Sprintf("Query execution failed: %v", err),
		}, nil
	}
	
	return response, nil
}

func (s *QueryServer) StreamQuery(req *pb.QueryRequest, stream pb.QueryService_StreamQueryServer) error {
	if req.Query == "" {
		return status.Error(codes.InvalidArgument, "query cannot be empty")
	}
	
	log.Printf("Streaming query: %s", req.Query)
	
	// Stream query using the new engine
	if err := s.executor.StreamQuery(stream.Context(), req.Query, req.Parameters, stream); err != nil {
		log.Printf("Stream query error: %v", err)
		return status.Error(codes.Internal, fmt.Sprintf("Stream query failed: %v", err))
	}
	
	return nil
}

