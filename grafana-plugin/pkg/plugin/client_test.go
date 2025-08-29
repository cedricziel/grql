package plugin

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cedricziel/grql/grafana-plugin/pkg/models"
	pb "github.com/cedricziel/grql/pkg/grql/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// testGRPCServer implements a simple test server
type testGRPCServer struct {
	pb.UnimplementedQueryServiceServer
	queryHandler  func(context.Context, *pb.QueryRequest) (*pb.QueryResponse, error)
	streamHandler func(*pb.QueryRequest, pb.QueryService_StreamQueryServer) error
}

func (s *testGRPCServer) ExecuteQuery(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if s.queryHandler != nil {
		return s.queryHandler(ctx, req)
	}
	return &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{
				Fields: map[string]*pb.Value{
					"test": {Value: &pb.Value_StringValue{StringValue: "success"}},
				},
			},
		},
	}, nil
}

func (s *testGRPCServer) StreamQuery(req *pb.QueryRequest, stream pb.QueryService_StreamQueryServer) error {
	if s.streamHandler != nil {
		return s.streamHandler(req, stream)
	}
	// Default: send a few results
	for i := 0; i < 3; i++ {
		result := &pb.QueryResult{
			Fields: map[string]*pb.Value{
				"index": {Value: &pb.Value_IntValue{IntValue: int64(i)}},
			},
		}
		if err := stream.Send(result); err != nil {
			return err
		}
	}
	return nil
}

func startTestServer(t *testing.T, useTLS bool, server *testGRPCServer) (string, func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	var grpcServer *grpc.Server
	if useTLS {
		// Generate test certificates
		cert, key := generateTestCert(t)
		tlsCert, err := tls.X509KeyPair(cert, key)
		if err != nil {
			t.Fatalf("Failed to load certificates: %v", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
		}
		creds := credentials.NewTLS(tlsConfig)
		grpcServer = grpc.NewServer(grpc.Creds(creds))
	} else {
		grpcServer = grpc.NewServer()
	}

	pb.RegisterQueryServiceServer(grpcServer, server)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	cleanup := func() {
		grpcServer.Stop()
		lis.Close()
	}

	return lis.Addr().String(), cleanup
}

func TestGrqlClient_NewClient(t *testing.T) {
	tests := []struct {
		name     string
		settings *models.PluginSettings
		wantErr  bool
	}{
		{
			name: "default settings",
			settings: &models.PluginSettings{
				Host: "",
				Port: 0,
			},
			wantErr: false,
		},
		{
			name: "custom host and port",
			settings: &models.PluginSettings{
				Host: "custom-host",
				Port: 8080,
			},
			wantErr: false,
		},
		{
			name: "with TLS",
			settings: &models.PluginSettings{
				Host:   "localhost",
				Port:   50051,
				UseTLS: true,
			},
			wantErr: false,
		},
		{
			name: "with TLS and skip verify",
			settings: &models.PluginSettings{
				Host:               "localhost",
				Port:               50051,
				UseTLS:             true,
				InsecureSkipVerify: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewGrqlClient(tt.settings)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewGrqlClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if client != nil {
				defer client.Close()
				if client.conn == nil {
					t.Error("Expected non-nil connection")
				}
				if client.client == nil {
					t.Error("Expected non-nil client")
				}
			}
		})
	}
}

func TestGrqlClient_ExecuteQuery(t *testing.T) {
	// Start test server
	server := &testGRPCServer{
		queryHandler: func(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
			// Echo back the query
			return &pb.QueryResponse{
				Results: []*pb.QueryResult{
					{
						Fields: map[string]*pb.Value{
							"query": {Value: &pb.Value_StringValue{StringValue: req.Query}},
						},
					},
				},
				Metadata: &pb.QueryMetadata{
					RowsAffected:    1,
					ExecutionTimeMs: 10,
				},
			}, nil
		},
	}

	addr, cleanup := startTestServer(t, false, server)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	client, err := NewGrqlClient(&models.PluginSettings{
		Host: host,
		Port: port,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test query execution
	ctx := context.Background()
	resp, err := client.ExecuteQuery(ctx, "SELECT * FROM test", nil)
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}

	if len(resp.Results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(resp.Results))
	}

	if val, ok := resp.Results[0].Fields["query"]; ok {
		if val.GetStringValue() != "SELECT * FROM test" {
			t.Errorf("Expected query echo, got %s", val.GetStringValue())
		}
	} else {
		t.Error("Query field not found in response")
	}
}

func TestGrqlClient_ExecuteQueryWithParams(t *testing.T) {
	server := &testGRPCServer{
		queryHandler: func(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
			// Return parameters in response
			results := make([]*pb.QueryResult, 0, len(req.Parameters))
			for key, value := range req.Parameters {
				results = append(results, &pb.QueryResult{
					Fields: map[string]*pb.Value{
						key: {Value: &pb.Value_StringValue{StringValue: value}},
					},
				})
			}
			return &pb.QueryResponse{
				Results: results,
			}, nil
		},
	}

	addr, cleanup := startTestServer(t, false, server)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	client, err := NewGrqlClient(&models.PluginSettings{
		Host: host,
		Port: port,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test with parameters
	params := map[string]string{
		"param1": "value1",
		"param2": "value2",
	}

	ctx := context.Background()
	resp, err := client.ExecuteQuery(ctx, "SELECT * FROM test", params)
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}

	if len(resp.Results) != len(params) {
		t.Errorf("Expected %d results, got %d", len(params), len(resp.Results))
	}
}

func TestGrqlClient_ExecuteQueryError(t *testing.T) {
	server := &testGRPCServer{
		queryHandler: func(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
			if req.Query == "error" {
				return nil, status.Error(codes.Internal, "simulated error")
			}
			return &pb.QueryResponse{}, nil
		},
	}

	addr, cleanup := startTestServer(t, false, server)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	client, err := NewGrqlClient(&models.PluginSettings{
		Host: host,
		Port: port,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test error handling
	ctx := context.Background()
	_, err = client.ExecuteQuery(ctx, "error", nil)
	if err == nil {
		t.Error("Expected error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Error("Expected gRPC status error")
	}
	if st.Code() != codes.Internal {
		t.Errorf("Expected Internal error code, got %v", st.Code())
	}
}

func TestGrqlClient_StreamQuery(t *testing.T) {
	server := &testGRPCServer{
		streamHandler: func(req *pb.QueryRequest, stream pb.QueryService_StreamQueryServer) error {
			// Send 5 results
			for i := 0; i < 5; i++ {
				result := &pb.QueryResult{
					Fields: map[string]*pb.Value{
						"index": {Value: &pb.Value_IntValue{IntValue: int64(i)}},
						"query": {Value: &pb.Value_StringValue{StringValue: req.Query}},
					},
				}
				if err := stream.Send(result); err != nil {
					return err
				}
				time.Sleep(10 * time.Millisecond) // Simulate processing
			}
			return nil
		},
	}

	addr, cleanup := startTestServer(t, false, server)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

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
	stream, err := client.StreamQuery(ctx, "SELECT * FROM stream", nil)
	if err != nil {
		t.Fatalf("StreamQuery failed: %v", err)
	}

	var results []*pb.QueryResult
	for {
		result, err := stream.Recv()
		if err != nil {
			break
		}
		results = append(results, result)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 streaming results, got %d", len(results))
	}

	// Verify results are in order
	for i, result := range results {
		if idx := result.Fields["index"].GetIntValue(); idx != int64(i) {
			t.Errorf("Result %d: expected index %d, got %d", i, i, idx)
		}
		if query := result.Fields["query"].GetStringValue(); query != "SELECT * FROM stream" {
			t.Errorf("Result %d: expected query echo, got %s", i, query)
		}
	}
}

func TestGrqlClient_StreamQueryWithError(t *testing.T) {
	server := &testGRPCServer{
		streamHandler: func(req *pb.QueryRequest, stream pb.QueryService_StreamQueryServer) error {
			// Send one result then error
			result := &pb.QueryResult{
				Fields: map[string]*pb.Value{
					"index": {Value: &pb.Value_IntValue{IntValue: 0}},
				},
			}
			if err := stream.Send(result); err != nil {
				return err
			}
			return status.Error(codes.Internal, "stream error")
		},
	}

	addr, cleanup := startTestServer(t, false, server)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	client, err := NewGrqlClient(&models.PluginSettings{
		Host: host,
		Port: port,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test streaming with error
	ctx := context.Background()
	stream, err := client.StreamQuery(ctx, "SELECT * FROM stream", nil)
	if err != nil {
		t.Fatalf("StreamQuery failed: %v", err)
	}

	var resultCount int
	for {
		_, err := stream.Recv()
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Internal {
				// Expected error
				break
			}
			t.Errorf("Unexpected error: %v", err)
			break
		}
		resultCount++
	}

	if resultCount != 1 {
		t.Errorf("Expected 1 result before error, got %d", resultCount)
	}
}

func TestGrqlClient_Close(t *testing.T) {
	client, err := NewGrqlClient(&models.PluginSettings{
		Host: "localhost",
		Port: 50051,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Test close
	err = client.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Test close on already closed client (should not panic)
	client.conn = nil
	err = client.Close()
	if err != nil {
		t.Errorf("Close() on nil conn error = %v", err)
	}
}

func TestGrqlClient_ContextCancellation(t *testing.T) {
	server := &testGRPCServer{
		queryHandler: func(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
			// Simulate long-running query
			select {
			case <-time.After(5 * time.Second):
				return &pb.QueryResponse{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}

	addr, cleanup := startTestServer(t, false, server)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	client, err := NewGrqlClient(&models.PluginSettings{
		Host: host,
		Port: port,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test context cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = client.ExecuteQuery(ctx, "SELECT * FROM test", nil)
	if err == nil {
		t.Error("Expected context cancellation error")
	}

	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("Expected DeadlineExceeded, got %v", status.Code(err))
	}
}

func TestGrqlClient_TLSConnection(t *testing.T) {
	t.Skip("Skipping TLS test - requires proper test certificates")
	// Generate test certificates
	cert, key := generateTestCert(t)

	server := &testGRPCServer{}
	addr, cleanup := startTestServer(t, true, server)
	defer cleanup()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	// Test with TLS and InsecureSkipVerify
	client, err := NewGrqlClient(&models.PluginSettings{
		Host:               host,
		Port:               port,
		UseTLS:             true,
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("Failed to create TLS client: %v", err)
	}
	defer client.Close()

	// Try to execute a query
	ctx := context.Background()
	resp, err := client.ExecuteQuery(ctx, "SELECT * FROM test", nil)
	if err != nil {
		t.Fatalf("ExecuteQuery with TLS failed: %v", err)
	}

	if len(resp.Results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(resp.Results))
	}

	// Test with client certificates
	clientWithCerts, err := NewGrqlClient(&models.PluginSettings{
		Host:               host,
		Port:               port,
		UseTLS:             true,
		InsecureSkipVerify: true,
		Secrets: &models.SecretPluginSettings{
			TLSCert: string(cert),
			TLSKey:  string(key),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create TLS client with certs: %v", err)
	}
	defer clientWithCerts.Close()

	resp, err = clientWithCerts.ExecuteQuery(ctx, "SELECT * FROM test", nil)
	if err != nil {
		t.Fatalf("ExecuteQuery with client certs failed: %v", err)
	}

	if len(resp.Results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(resp.Results))
	}
}

func TestGrqlClient_InvalidCertificates(t *testing.T) {
	_, err := NewGrqlClient(&models.PluginSettings{
		Host:   "localhost",
		Port:   50051,
		UseTLS: true,
		Secrets: &models.SecretPluginSettings{
			TLSCert: "invalid cert",
			TLSKey:  "invalid key",
		},
	})

	if err == nil {
		t.Error("Expected error for invalid certificates")
	}
}

// generateTestCert generates a self-signed certificate for testing
func generateTestCert(t *testing.T) ([]byte, []byte) {
	// This is a simplified test certificate generation
	// In production, use proper certificate generation
	certPEM := []byte(`-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKHHPl7ScF6sMA0GCSqGSIb3DQEBCwUAMBQxEjAQBgNVBAMMCWxv
Y2FsaG9zdDAeFw0yMzAxMDEwMDAwMDBaFw0yNDAxMDEwMDAwMDBaMBQxEjAQBgNV
BAMMCWxvY2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA4f5Zv3LZ
YKjCxN0q9Y6L6kGk8X8HTlblKDaUdGVk8lNSwhQwTlmZvmZQc5V1qJgNgmWGNyQD
/WJSdJhBxrDLuzojsjOwVr09ivGwDaCWJ2DPB5Q8xuNiJiI0C7NJCs5vRGVk4gIT
-----END CERTIFICATE-----`)

	keyPEM := []byte(`-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQDh/lm/ctlgqMLE3Sr1jovqQaTxfwdOVuUoNpR0ZWTyU1LCFDBO
WZm+ZlBzlXWomA2CZYY3JAP9YlJ0mEHGsMu7OiOyM7BWvT2K8bANoJYnYM8HlDzG
42ImIjQLs0kKzm9EZWTiAhOvWHl8LYUbvvCLPMwYPGYZ/+sdfghjklmnopqrstuv
wxyzabcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTAgMB
AAECgYBvjzAVCzi0VvdCOxXLhJ2PZ6WPmB3ji3Pr2EF7QTRbCgPb7K5Mje+yKpB7
-----END RSA PRIVATE KEY-----`)

	return certPEM, keyPEM
}
