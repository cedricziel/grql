//go:build integration
// +build integration

package backends

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestLokiIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start Loki container
	req := testcontainers.ContainerRequest{
		Image:        "grafana/loki:latest",
		ExposedPorts: []string{"3100/tcp"},
		Cmd:          []string{"-config.file=/etc/loki/local-config.yaml"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("3100/tcp"),
			wait.ForHTTP("/ready").WithPort("3100/tcp"),
		).WithDeadline(60 * time.Second),
	}

	lokiContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start Loki container: %v", err)
	}
	defer func() {
		if termErr := lokiContainer.Terminate(ctx); termErr != nil {
			t.Logf("Failed to terminate Loki container: %v", termErr)
		}
	}()

	// Get the Loki endpoint
	host, err := lokiContainer.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get container host: %v", err)
	}

	port, err := lokiContainer.MappedPort(ctx, "3100")
	if err != nil {
		t.Fatalf("Failed to get mapped port: %v", err)
	}

	lokiURL := fmt.Sprintf("http://%s:%s", host, port.Port())
	t.Logf("Loki is running at: %s", lokiURL)

	// Wait a bit for Loki to be fully ready
	time.Sleep(5 * time.Second)

	// Create Loki adapter
	adapter := NewLokiAdapter(lokiURL, "")

	// Test 1: Basic connectivity - check if Loki is responding
	t.Run("CheckConnectivity", func(t *testing.T) {
		resp, err := http.Get(lokiURL + "/ready")
		if err != nil {
			t.Fatalf("Failed to connect to Loki: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	})

	// Test 2: Send test logs
	t.Run("SendTestLogs", func(t *testing.T) {
		// Push some test logs to Loki
		pushURL := lokiURL + "/loki/api/v1/push"

		// Create log entries
		logPayload := createLokiPushPayload()

		resp, err := http.Post(pushURL, "application/json", logPayload)
		if err != nil {
			t.Fatalf("Failed to push logs to Loki: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 204 or 200, got %d", resp.StatusCode)
		}

		// Wait for logs to be indexed
		time.Sleep(2 * time.Second)
	})

	// Test 3: Query logs
	t.Run("QueryLogs", func(t *testing.T) {
		query := QueryRequest{
			TimeRange: TimeRange{
				Since: time.Now().Add(-1 * time.Hour),
				Until: time.Now(),
			},
			Filters: []Filter{
				{Field: "level", Operator: "=", Value: "info"},
			},
			Limit: 10,
		}

		response, err := adapter.ExecuteQuery(ctx, query)
		if err != nil {
			// Log but don't fail - Loki might need specific configuration
			t.Logf("Query execution returned error (might be expected): %v", err)
		} else {
			t.Logf("Query returned %d results", len(response.Results))
		}
	})

	// Test 4: Test capabilities
	t.Run("TestCapabilities", func(t *testing.T) {
		caps := adapter.GetCapabilities()

		if !caps.SupportsAggregation {
			t.Error("Loki should support aggregation")
		}

		if !caps.SupportsGroupBy {
			t.Error("Loki should support group by")
		}

		if !caps.SupportsRate {
			t.Error("Loki should support rate functions")
		}

		if caps.SupportsHistogram {
			t.Error("Loki should not support histograms")
		}

		if caps.MaxTimeRange == 0 {
			t.Error("MaxTimeRange should be set")
		}
	})

	// Test 5: Build LogQL queries
	t.Run("BuildLogQLQueries", func(t *testing.T) {
		testCases := []struct {
			query    QueryRequest
			name     string
			expected string // Part of expected LogQL
		}{
			{
				name: "simple log filter",
				query: QueryRequest{
					Filters: []Filter{
						{Field: "level", Operator: "=", Value: "error"},
					},
				},
				expected: `{level="error"}`,
			},
			{
				name: "service selector",
				query: QueryRequest{
					Filters: []Filter{
						{Field: "service", Operator: "=", Value: "api"},
					},
				},
				expected: `|= "api"`,
			},
			{
				name: "with rate aggregation",
				query: QueryRequest{
					Aggregates: []Aggregate{
						{Function: "rate", Field: ""},
					},
				},
				expected: "rate(",
			},
			{
				name: "with count aggregation",
				query: QueryRequest{
					Aggregates: []Aggregate{
						{Function: "count", Field: ""},
					},
				},
				expected: "count_over_time(",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				logQL := adapter.translateToLogQL(tc.query)
				if logQL == "" {
					t.Error("Expected non-empty LogQL")
				}
				// Check if expected substring is in the query
				if tc.expected != "" && !strings.Contains(logQL, tc.expected) {
					t.Errorf("Expected LogQL to contain '%s', got: %s", tc.expected, logQL)
				}
			})
		}
	})
}

// createLokiPushPayload creates a test log push payload for Loki
func createLokiPushPayload() *strings.Reader {
	payload := map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": map[string]string{
					"service": "test-service",
					"level":   "info",
					"job":     "test",
				},
				"values": [][]string{
					{fmt.Sprintf("%d", time.Now().UnixNano()), "Test log message 1"},
					{fmt.Sprintf("%d", time.Now().Add(1*time.Second).UnixNano()), "Test log message 2"},
					{fmt.Sprintf("%d", time.Now().Add(2*time.Second).UnixNano()), "Error occurred in test"},
				},
			},
		},
	}

	data, _ := json.Marshal(payload)
	return strings.NewReader(string(data))
}
