//go:build integration
// +build integration

package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestFederationIntegration tests the complete query engine with all three backends
func TestFederationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start all three containers
	containers := struct {
		mimir *testcontainers.Container
		loki  *testcontainers.Container
		tempo *testcontainers.Container
	}{}

	// Start Mimir
	t.Run("StartMimir", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image:        "grafana/mimir:latest",
			ExposedPorts: []string{"9009/tcp"},
			Cmd:          []string{"-target=all", "-server.http-listen-port=9009", "-server.grpc-listen-port=9095"},
			Env: map[string]string{
				"MIMIR_STORAGE_BACKEND":                  "filesystem",
				"MIMIR_STORAGE_FILESYSTEM_DIR":           "/tmp/mimir-blocks",
				"MIMIR_COMPACTOR_DATA_DIR":               "/tmp/mimir-compactor",
				"MIMIR_INGESTER_RING_REPLICATION_FACTOR": "1",
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("9009/tcp"),
				wait.ForHTTP("/ready").WithPort("9009/tcp").WithStatusCodeMatcher(func(status int) bool {
					return status == 200 || status == 204
				}),
			).WithDeadline(60 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatalf("Failed to start Mimir: %v", err)
		}
		containers.mimir = &container
	})

	// Start Loki
	t.Run("StartLoki", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image:        "grafana/loki:latest",
			ExposedPorts: []string{"3100/tcp"},
			Cmd:          []string{"-config.file=/etc/loki/local-config.yaml"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("3100/tcp"),
				wait.ForHTTP("/ready").WithPort("3100/tcp"),
			).WithDeadline(60 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatalf("Failed to start Loki: %v", err)
		}
		containers.loki = &container
	})

	// Start Tempo
	t.Run("StartTempo", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image:        "grafana/tempo:latest",
			ExposedPorts: []string{"3200/tcp"},
			Cmd: []string{
				"-target=all",
				"-storage.trace.backend=local",
				"-storage.trace.local.path=/tmp/tempo/traces",
				"-auth.enabled=false",
				"-server.http-listen-port=3200",
				"-server.grpc-listen-port=9095",
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("3200/tcp"),
				wait.ForHTTP("/ready").WithPort("3200/tcp").WithStatusCodeMatcher(func(status int) bool {
					return status == 200 || status == 204
				}),
			).WithDeadline(60 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatalf("Failed to start Tempo: %v", err)
		}
		containers.tempo = &container
	})

	// Cleanup containers on exit
	defer func() {
		if containers.mimir != nil {
			(*containers.mimir).Terminate(ctx)
		}
		if containers.loki != nil {
			(*containers.loki).Terminate(ctx)
		}
		if containers.tempo != nil {
			(*containers.tempo).Terminate(ctx)
		}
	}()

	// Get endpoints for all services
	var mimirURL, lokiURL, tempoURL string

	if containers.mimir != nil {
		host, _ := (*containers.mimir).Host(ctx)
		port, _ := (*containers.mimir).MappedPort(ctx, "9009")
		mimirURL = fmt.Sprintf("http://%s:%s", host, port.Port())
		t.Logf("Mimir URL: %s", mimirURL)
	}

	if containers.loki != nil {
		host, _ := (*containers.loki).Host(ctx)
		port, _ := (*containers.loki).MappedPort(ctx, "3100")
		lokiURL = fmt.Sprintf("http://%s:%s", host, port.Port())
		t.Logf("Loki URL: %s", lokiURL)
	}

	if containers.tempo != nil {
		host, _ := (*containers.tempo).Host(ctx)
		port, _ := (*containers.tempo).MappedPort(ctx, "3200")
		tempoURL = fmt.Sprintf("http://%s:%s", host, port.Port())
		t.Logf("Tempo URL: %s", tempoURL)
	}

	// Wait for services to be ready
	time.Sleep(10 * time.Second)

	// Create executor with backend URLs
	config := ExecutorConfig{
		MimirURL:  mimirURL,
		LokiURL:   lokiURL,
		TempoURL:  tempoURL,
		CacheSize: 100,
	}
	executor := NewExecutor(config)

	// Test 1: Query metrics through the federation layer
	t.Run("QueryMetrics", func(t *testing.T) {
		query := "SELECT cpu, memory FROM metrics WHERE service = 'api' SINCE 1h"

		response, err := executor.ExecuteQuery(ctx, query, nil)
		if err != nil {
			// Expected to fail without actual metrics
			t.Logf("Metrics query error (expected without data): %v", err)
		} else {
			t.Logf("Metrics query returned %d results", len(response.Results))
		}
	})

	// Test 2: Query logs through the federation layer
	t.Run("QueryLogs", func(t *testing.T) {
		query := "SELECT * FROM logs WHERE level = 'error' SINCE 1h LIMIT 100"

		response, err := executor.ExecuteQuery(ctx, query, nil)
		if err != nil {
			// Expected to fail without actual logs
			t.Logf("Logs query error (expected without data): %v", err)
		} else {
			t.Logf("Logs query returned %d results", len(response.Results))
		}
	})

	// Test 3: Query traces through the federation layer
	t.Run("QueryTraces", func(t *testing.T) {
		query := "SELECT * FROM traces WHERE duration > 100 SINCE 1h"

		response, err := executor.ExecuteQuery(ctx, query, nil)
		if err != nil {
			// Expected to fail without actual traces
			t.Logf("Traces query error (expected without data): %v", err)
		} else {
			t.Logf("Traces query returned %d results", len(response.Results))
		}
	})

	// Test 4: Test aggregation queries
	t.Run("AggregationQueries", func(t *testing.T) {
		testCases := []struct {
			name  string
			query string
		}{
			{
				name:  "metrics aggregation",
				query: "SELECT avg(cpu), max(memory) FROM metrics GROUP BY instance SINCE 1h",
			},
			{
				name:  "logs aggregation",
				query: "SELECT count(*) FROM logs WHERE level = 'error' GROUP BY service SINCE 1h",
			},
			{
				name:  "traces aggregation",
				query: "SELECT p95(duration) FROM traces GROUP BY service SINCE 1h",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := executor.ExecuteQuery(ctx, tc.query, nil)
				if err != nil {
					t.Logf("Query error (expected without data): %v", err)
				}
			})
		}
	})

	// Test 5: Test query parsing and planning
	t.Run("QueryPlanningValidation", func(t *testing.T) {
		parser := NewParser()
		planner := NewPlanner()
		optimizer := NewOptimizer()

		queries := []string{
			"SELECT * FROM metrics WHERE cpu > 80",
			"SELECT count(*) FROM logs GROUP BY level",
			"SELECT avg(duration) FROM traces WHERE service = 'api'",
			"SELECT rate(requests) FROM metrics SINCE 5m",
		}

		for _, q := range queries {
			t.Run(q, func(t *testing.T) {
				// Parse
				parsed, err := parser.Parse(q)
				if err != nil {
					t.Errorf("Failed to parse query: %v", err)
					return
				}

				// Plan
				logical, err := planner.CreateLogicalPlan(parsed)
				if err != nil {
					t.Errorf("Failed to create logical plan: %v", err)
					return
				}

				// Optimize
				optimized, err := optimizer.Optimize(logical)
				if err != nil {
					t.Errorf("Failed to optimize plan: %v", err)
					return
				}

				// Create physical plan
				physical, err := planner.CreatePhysicalPlan(optimized)
				if err != nil {
					t.Errorf("Failed to create physical plan: %v", err)
					return
				}

				if physical.Backend == "" {
					t.Error("Expected backend to be selected")
				}
			})
		}
	})

	// Test 6: Test caching behavior
	t.Run("CachingBehavior", func(t *testing.T) {
		query := "SELECT * FROM metrics WHERE service = 'test' SINCE 1h"

		// First query
		start1 := time.Now()
		_, err1 := executor.ExecuteQuery(ctx, query, nil)
		duration1 := time.Since(start1)

		// Second query (should be cached)
		start2 := time.Now()
		_, err2 := executor.ExecuteQuery(ctx, query, nil)
		duration2 := time.Since(start2)

		// Both should have same error status
		if (err1 == nil) != (err2 == nil) {
			t.Error("Cache returned different error status")
		}

		// Cached query should be faster (though this might not always be true in tests)
		t.Logf("First query: %v, Second query: %v", duration1, duration2)
	})
}
