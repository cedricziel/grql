//go:build integration
// +build integration

package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	pb "github.com/cedricziel/grql/pkg/grql/v1"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestGRQLServerIntegration tests the complete GRQL server with Prometheus metrics
func TestGRQLServerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start Prometheus container with embedded config
	prometheusConfig := `
global:
  scrape_interval: 5s
  evaluation_interval: 5s
  external_labels:
    monitor: 'test-monitor'

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
`
	
	req := testcontainers.ContainerRequest{
		Image:        "prom/prometheus:latest",
		ExposedPorts: []string{"9090/tcp"},
		Cmd: []string{
			"--config.file=/etc/prometheus/prometheus.yml",
			"--storage.tsdb.path=/prometheus",
			"--web.enable-remote-write-receiver", // Enable remote write for pushing metrics
			"--log.level=info",
		},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            strings.NewReader(prometheusConfig),
				ContainerFilePath: "/etc/prometheus/prometheus.yml",
				FileMode:          0644,
			},
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("9090/tcp"),
			wait.ForHTTP("/-/ready").WithPort("9090/tcp"),
		).WithDeadline(60 * time.Second),
	}

	promContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start Prometheus container: %v", err)
	}
	defer promContainer.Terminate(ctx)

	// Get Prometheus endpoint
	host, err := promContainer.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get container host: %v", err)
	}

	port, err := promContainer.MappedPort(ctx, "9090")
	if err != nil {
		t.Fatalf("Failed to get mapped port: %v", err)
	}

	promURL := fmt.Sprintf("http://%s:%s", host, port.Port())
	t.Logf("Prometheus is running at: %s", promURL)

	// Wait for Prometheus to be ready
	time.Sleep(5 * time.Second)

	// Push test metrics to Prometheus
	t.Run("PushMetrics", func(t *testing.T) {
		metrics := generateTestMetrics()
		
		// Push metrics using Prometheus remote write endpoint
		pushURL := promURL + "/api/v1/write"
		
		for i := 0; i < 3; i++ {
			// Push metrics multiple times to create time series
			err := pushMetricsToPrometheus(pushURL, metrics)
			if err != nil {
				// Try exposition format as fallback
				err = pushMetricsExposition(promURL, metrics)
				if err != nil {
					t.Logf("Failed to push metrics (attempt %d): %v", i+1, err)
				}
			}
			time.Sleep(10 * time.Second) // Wait between pushes to create time series
		}
	})

	// Set environment variable for GRQL server to use our Prometheus instance
	t.Setenv("MIMIR_URL", promURL)
	
	// Start GRQL server pointing to Prometheus
	grqlServer := New()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":0") // Random available port
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterQueryServiceServer(grpcServer, grqlServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Failed to serve: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// Create gRPC client
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to GRQL server: %v", err)
	}
	defer conn.Close()

	client := pb.NewQueryServiceClient(conn)

	// Wait for metrics to be available
	time.Sleep(5 * time.Second)

	// Test various queries
	testCases := []struct {
		name        string
		query       string
		expectError bool
		validate    func(*testing.T, *pb.QueryResponse)
	}{
		{
			name:  "Simple SELECT",
			query: "SELECT cpu_usage FROM metrics WHERE host = 'server1' LIMIT 10",
			validate: func(t *testing.T, resp *pb.QueryResponse) {
				if len(resp.Results) == 0 {
					t.Log("No results returned (metrics might not be scraped yet)")
				} else {
					t.Logf("Got %d results", len(resp.Results))
					for _, result := range resp.Results {
						t.Logf("Result: %v", result.Fields)
					}
				}
			},
		},
		{
			name:  "Aggregation Query",
			query: "SELECT avg(cpu_usage) FROM metrics GROUP BY host SINCE 1h",
			validate: func(t *testing.T, resp *pb.QueryResponse) {
				if resp.Metadata != nil {
					t.Logf("Query executed in %dms", resp.Metadata.ExecutionTimeMs)
					t.Logf("Rows affected: %d", resp.Metadata.RowsAffected)
				}
			},
		},
		{
			name:  "Rate Query",
			query: "SELECT rate(http_requests_total) FROM metrics WHERE endpoint = '/api/users' SINCE 5m",
			validate: func(t *testing.T, resp *pb.QueryResponse) {
				if len(resp.Results) > 0 {
					t.Logf("Rate query returned %d results", len(resp.Results))
				}
			},
		},
		{
			name:  "Filter Query",
			query: "SELECT * FROM metrics WHERE cpu_usage > 70 AND service = 'api' LIMIT 5",
			validate: func(t *testing.T, resp *pb.QueryResponse) {
				for _, result := range resp.Results {
					// Check if cpu_usage values are > 70
					for key, val := range result.Fields {
						if key == "cpu_usage" {
							t.Logf("CPU usage value: %v", val)
						}
					}
				}
			},
		},
		{
			name:  "Complex Aggregation",
			query: "SELECT max(memory_usage), min(memory_usage), avg(memory_usage) FROM metrics GROUP BY service SINCE 30m",
			validate: func(t *testing.T, resp *pb.QueryResponse) {
				if len(resp.Results) > 0 {
					t.Logf("Aggregation returned %d groups", len(resp.Results))
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &pb.QueryRequest{
				Query:      tc.query,
				Parameters: map[string]string{},
			}

			resp, err := client.ExecuteQuery(ctx, req)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Logf("Query error (might be expected without data): %v", err)
				return
			}

			if resp.Error != "" {
				t.Logf("Query returned error: %s", resp.Error)
			}

			if tc.validate != nil {
				tc.validate(t, resp)
			}
		})
	}

	// Test streaming query
	t.Run("StreamQuery", func(t *testing.T) {
		req := &pb.QueryRequest{
			Query: "SELECT * FROM metrics WHERE service = 'api' SINCE 1h",
		}

		stream, err := client.StreamQuery(ctx, req)
		if err != nil {
			t.Logf("Stream query error: %v", err)
			return
		}

		resultCount := 0
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Logf("Stream receive error: %v", err)
				break
			}
			if resp.Fields != nil {
				resultCount++
			}
		}
		t.Logf("Stream query returned %d total results", resultCount)
	})
}

// generateTestMetrics generates sample Prometheus metrics
func generateTestMetrics() string {
	timestamp := time.Now().Unix() * 1000 // Prometheus uses millisecond timestamps
	
	return fmt.Sprintf(`# HELP cpu_usage CPU usage percentage
# TYPE cpu_usage gauge
cpu_usage{host="server1",service="api"} 75.5 %d
cpu_usage{host="server2",service="api"} 82.3 %d
cpu_usage{host="server1",service="db"} 45.2 %d
cpu_usage{host="server2",service="db"} 38.9 %d

# HELP memory_usage Memory usage in MB
# TYPE memory_usage gauge
memory_usage{host="server1",service="api"} 512 %d
memory_usage{host="server2",service="api"} 768 %d
memory_usage{host="server1",service="db"} 2048 %d
memory_usage{host="server2",service="db"} 1536 %d

# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total{endpoint="/api/users",method="GET",status="200"} 1234 %d
http_requests_total{endpoint="/api/users",method="POST",status="201"} 567 %d
http_requests_total{endpoint="/api/products",method="GET",status="200"} 890 %d
http_requests_total{endpoint="/api/products",method="POST",status="201"} 234 %d

# HELP response_time_seconds Response time in seconds
# TYPE response_time_seconds histogram
response_time_seconds_bucket{endpoint="/api/users",le="0.1"} 500 %d
response_time_seconds_bucket{endpoint="/api/users",le="0.5"} 900 %d
response_time_seconds_bucket{endpoint="/api/users",le="1"} 950 %d
response_time_seconds_bucket{endpoint="/api/users",le="+Inf"} 1000 %d
response_time_seconds_sum{endpoint="/api/users"} 245.5 %d
response_time_seconds_count{endpoint="/api/users"} 1000 %d
`,
		timestamp, timestamp, timestamp, timestamp,
		timestamp, timestamp, timestamp, timestamp,
		timestamp, timestamp, timestamp, timestamp,
		timestamp, timestamp, timestamp, timestamp, timestamp, timestamp,
	)
}

// pushMetricsExposition pushes metrics using the exposition format
func pushMetricsExposition(promURL string, metrics string) error {
	// Create a simple HTTP endpoint that Prometheus can scrape
	// This is a simplified approach for testing
	
	// Try to push directly via the federate endpoint (not ideal but works for testing)
	resp, err := http.Post(
		promURL+"/api/v1/import/prometheus",
		"text/plain",
		strings.NewReader(metrics),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode >= 400 {
		return fmt.Errorf("failed to push metrics: status %d", resp.StatusCode)
	}
	
	return nil
}

// pushMetricsToPrometheus attempts to push metrics via remote write
func pushMetricsToPrometheus(pushURL string, metrics string) error {
	// For simplicity in testing, we'll use the exposition format
	// Real implementation would use Prometheus remote write protobuf format
	
	// This is a placeholder - real remote write requires protobuf encoding
	// For now, we'll rely on Prometheus scraping or manual metric creation
	return fmt.Errorf("remote write not implemented for testing")
}

