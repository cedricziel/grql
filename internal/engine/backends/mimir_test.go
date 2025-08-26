package backends

import (
	"testing"
	"time"
)

func TestMimirAdapter_TranslateToPromQL(t *testing.T) {
	adapter := NewMimirAdapter("http://localhost:9009", "")

	tests := []struct {
		name   string
		query  QueryRequest
		wantQL string
	}{
		{
			name: "simple metric query",
			query: QueryRequest{
				MetricName: "up",
				TimeRange: TimeRange{
					Since: time.Now().Add(-1 * time.Hour),
					Until: time.Now(),
				},
			},
			wantQL: "up",
		},
		{
			name: "metric with filters",
			query: QueryRequest{
				MetricName: "http_requests_total",
				Filters: []Filter{
					{Field: "job", Operator: "=", Value: "api"},
					{Field: "status", Operator: "!=", Value: "500"},
				},
			},
			wantQL: `http_requests_total{job="api", status!="500"}`,
		},
		{
			name: "aggregation query",
			query: QueryRequest{
				MetricName: "cpu_usage",
				Aggregates: []Aggregate{
					{Function: "avg", Field: "value"},
				},
			},
			wantQL: "avg(cpu_usage)",
		},
		{
			name: "aggregation with grouping",
			query: QueryRequest{
				MetricName: "memory_usage",
				Aggregates: []Aggregate{
					{Function: "sum", Field: "value"},
				},
				GroupBy: []string{"instance", "job"},
			},
			wantQL: "sum(memory_usage) by (instance, job)",
		},
		{
			name: "rate query",
			query: QueryRequest{
				MetricName: "http_requests_total",
				Aggregates: []Aggregate{
					{Function: "rate", Field: "value"},
				},
			},
			wantQL: "rate(http_requests_total[5m])",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adapter.translateToPromQL(tt.query)
			if got != tt.wantQL {
				t.Errorf("translateToPromQL() = %v, want %v", got, tt.wantQL)
			}
		})
	}
}

func TestMimirAdapter_CalculateStep(t *testing.T) {
	adapter := NewMimirAdapter("http://localhost:9009", "")

	now := time.Now()
	tests := []struct {
		name      string
		timeRange TimeRange
		wantStep  string
	}{
		{
			name: "1 hour range",
			timeRange: TimeRange{
				Since: now.Add(-1 * time.Hour),
				Until: now,
			},
			wantStep: "15s",
		},
		{
			name: "6 hour range",
			timeRange: TimeRange{
				Since: now.Add(-6 * time.Hour),
				Until: now,
			},
			wantStep: "30s",
		},
		{
			name: "24 hour range",
			timeRange: TimeRange{
				Since: now.Add(-24 * time.Hour),
				Until: now,
			},
			wantStep: "1m",
		},
		{
			name: "7 day range",
			timeRange: TimeRange{
				Since: now.Add(-7 * 24 * time.Hour),
				Until: now,
			},
			wantStep: "5m",
		},
		{
			name: "30 day range",
			timeRange: TimeRange{
				Since: now.Add(-30 * 24 * time.Hour),
				Until: now,
			},
			wantStep: "1h",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adapter.calculateStep(tt.timeRange)
			if got != tt.wantStep {
				t.Errorf("calculateStep() = %v, want %v", got, tt.wantStep)
			}
		})
	}
}

func TestMimirAdapter_GetCapabilities(t *testing.T) {
	adapter := NewMimirAdapter("http://localhost:9009", "")

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

	if len(caps.SupportedFunctions) == 0 {
		t.Error("Should have supported functions")
	}

	// Check for specific functions
	expectedFuncs := []string{"count", "sum", "avg", "min", "max", "rate"}
	for _, expected := range expectedFuncs {
		found := false
		for _, fn := range caps.SupportedFunctions {
			if fn == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected function %s not found in capabilities", expected)
		}
	}
}
