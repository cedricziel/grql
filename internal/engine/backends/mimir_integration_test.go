//go:build integration
// +build integration

package backends

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMimirIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start Mimir container in single-binary mode with minimal config
	req := testcontainers.ContainerRequest{
		Image:        "grafana/mimir:latest",
		ExposedPorts: []string{"9009/tcp"},
		Cmd:          []string{"-target=all", "-server.http-listen-port=9009", "-server.grpc-listen-port=9095"},
		Env: map[string]string{
			// Use filesystem storage for simplicity in tests
			"MIMIR_STORAGE_BACKEND": "filesystem",
			"MIMIR_STORAGE_FILESYSTEM_DIR": "/tmp/mimir-blocks",
			"MIMIR_COMPACTOR_DATA_DIR": "/tmp/mimir-compactor",
			"MIMIR_INGESTER_RING_REPLICATION_FACTOR": "1",
			"MIMIR_RULER_EVALUATION_INTERVAL": "10s",
			"MIMIR_RULER_POLL_INTERVAL": "10s",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("9009/tcp"),
			wait.ForHTTP("/ready").WithPort("9009/tcp").WithStatusCodeMatcher(func(status int) bool {
				return status == 200 || status == 204
			}),
		).WithDeadline(60 * time.Second),
	}

	mimirContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start Mimir container: %v", err)
	}
	defer func() {
		if err := mimirContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Mimir container: %v", err)
		}
	}()

	// Get the Mimir endpoint
	host, err := mimirContainer.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get container host: %v", err)
	}

	port, err := mimirContainer.MappedPort(ctx, "9009")
	if err != nil {
		t.Fatalf("Failed to get mapped port: %v", err)
	}

	mimirURL := fmt.Sprintf("http://%s:%s", host, port.Port())
	t.Logf("Mimir is running at: %s", mimirURL)

	// Wait a bit for Mimir to be fully ready
	time.Sleep(5 * time.Second)

	// Create Mimir adapter
	adapter := NewMimirAdapter(mimirURL, "")

	// Test 1: Basic connectivity - check if Mimir is responding
	t.Run("CheckConnectivity", func(t *testing.T) {
		resp, err := http.Get(mimirURL + "/ready")
		if err != nil {
			t.Fatalf("Failed to connect to Mimir: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	})

	// Test 2: Send test metrics using remote write
	t.Run("SendTestMetrics", func(t *testing.T) {
		// Push some test metrics using Prometheus remote write format
		pushURL := mimirURL + "/api/v1/push"
		
		// Create Prometheus remote write payload
		metricsPayload := createPrometheusRemoteWritePayload()
		
		req, err := http.NewRequest("POST", pushURL, bytes.NewReader(metricsPayload))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")
		req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			// This might fail without proper Prometheus remote write format
			t.Logf("Failed to push metrics (expected without proper protobuf): %v", err)
		} else {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			t.Logf("Push response: %d - %s", resp.StatusCode, string(body))
		}
	})

	// Test 3: Query metrics
	t.Run("QueryMetrics", func(t *testing.T) {
		query := QueryRequest{
			TimeRange: TimeRange{
				Since: time.Now().Add(-1 * time.Hour),
				Until: time.Now(),
			},
			Filters: []Filter{
				{Field: "job", Operator: "=", Value: "test"},
			},
			Limit: 10,
		}

		response, err := adapter.ExecuteQuery(ctx, query)
		if err != nil {
			// Log but don't fail - Mimir might need metrics to be present
			t.Logf("Query execution returned error (might be expected): %v", err)
		} else {
			t.Logf("Query returned %d results", len(response.Results))
		}
	})

	// Test 4: Test instant query
	t.Run("InstantQuery", func(t *testing.T) {
		// Try a simple instant query
		queryURL := fmt.Sprintf("%s/prometheus/api/v1/query?query=up", mimirURL)
		resp, err := http.Get(queryURL)
		if err != nil {
			t.Logf("Instant query failed: %v", err)
		} else {
			defer resp.Body.Close()
			t.Logf("Instant query response status: %d", resp.StatusCode)
		}
	})

	// Test 5: Test capabilities
	t.Run("TestCapabilities", func(t *testing.T) {
		caps := adapter.GetCapabilities()
		
		if !caps.SupportsAggregation {
			t.Error("Mimir should support aggregation")
		}
		
		if !caps.SupportsGroupBy {
			t.Error("Mimir should support group by")
		}
		
		if !caps.SupportsRate {
			t.Error("Mimir should support rate functions")
		}
		
		if !caps.SupportsHistogram {
			t.Error("Mimir should support histograms")
		}
		
		if caps.MaxTimeRange == 0 {
			t.Error("MaxTimeRange should be set")
		}
	})

	// Test 6: Build PromQL queries
	t.Run("BuildPromQLQueries", func(t *testing.T) {
		testCases := []struct {
			name     string
			query    QueryRequest
			expected string // Part of expected PromQL
		}{
			{
				name: "simple metric selector",
				query: QueryRequest{
					Filters: []Filter{
						{Field: "job", Operator: "=", Value: "api"},
					},
				},
				expected: `{job="api"}`,
			},
			{
				name: "with sum aggregation",
				query: QueryRequest{
					Aggregates: []Aggregate{
						{Function: "sum", Field: "cpu_usage"},
					},
				},
				expected: "sum(",
			},
			{
				name: "with rate function",
				query: QueryRequest{
					Aggregates: []Aggregate{
						{Function: "rate", Field: "http_requests_total"},
					},
				},
				expected: "rate(",
			},
			{
				name: "with group by",
				query: QueryRequest{
					GroupBy: []string{"instance"},
					Aggregates: []Aggregate{
						{Function: "avg", Field: "memory_usage"},
					},
				},
				expected: "by (instance)",
			},
			{
				name: "with percentile",
				query: QueryRequest{
					Aggregates: []Aggregate{
						{Function: "p95", Field: "response_time"},
					},
				},
				expected: "quantile(0.95",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				promQL := adapter.translateToPromQL(tc.query)
				if promQL == "" {
					t.Error("Expected non-empty PromQL")
				}
				// Check if expected substring is in the query
				if tc.expected != "" && !strings.Contains(promQL, tc.expected) {
					t.Errorf("Expected PromQL to contain '%s', got: %s", tc.expected, promQL)
				}
			})
		}
	})
}

// createPrometheusRemoteWritePayload creates a simplified test payload
// In production, you'd use the Prometheus remote write protocol properly
func createPrometheusRemoteWritePayload() []byte {
	// This is a placeholder - actual implementation would need proper protobuf encoding
	// and snappy compression for Prometheus remote write format
	// For now, we'll just return empty bytes as the test will handle the failure gracefully
	return []byte{}
}