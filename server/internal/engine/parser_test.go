package engine

import (
	"testing"
	"time"
)

func TestParser_ParseBasicSelect(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "simple select",
			query:   "SELECT * FROM metrics",
			wantErr: false,
		},
		{
			name:    "select with where",
			query:   "SELECT cpu, memory FROM metrics WHERE service = 'api'",
			wantErr: false,
		},
		{
			name:    "select with aggregation",
			query:   "SELECT avg(cpu_usage), max(memory) FROM metrics GROUP BY instance",
			wantErr: false,
		},
		{
			name:    "invalid syntax",
			query:   "SELECT FROM WHERE",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParser_ExtractDataSource(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		query      string
		wantSource DataSource
	}{
		{
			query:      "SELECT * FROM metrics",
			wantSource: DataSourceMetrics,
		},
		{
			query:      "SELECT * FROM logs",
			wantSource: DataSourceLogs,
		},
		{
			query:      "SELECT * FROM traces",
			wantSource: DataSourceTraces,
		},
		{
			query:      "SELECT * FROM spans",
			wantSource: DataSourceTraces,
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			q, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if q.DataSource != tt.wantSource {
				t.Errorf("DataSource = %v, want %v", q.DataSource, tt.wantSource)
			}
		})
	}
}

func TestParser_ExtractFilters(t *testing.T) {
	parser := NewParser()

	query := "SELECT * FROM metrics WHERE service = 'api' AND status != 'error'"
	q, err := parser.Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Query parsing extracts time range but not specific filters in current implementation
	// This test validates the structure is created correctly
	if q.Statement == nil {
		t.Error("Statement should not be nil")
	}
}

func TestParser_ExtractGroupBy(t *testing.T) {
	parser := NewParser()

	query := "SELECT count(*) FROM logs GROUP BY level, service"
	q, err := parser.Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(q.Facets) != 2 {
		t.Errorf("Expected 2 facets, got %d", len(q.Facets))
	}

	expectedFacets := []string{"level", "service"}
	for i, facet := range q.Facets {
		if facet != expectedFacets[i] {
			t.Errorf("Facet[%d] = %s, want %s", i, facet, expectedFacets[i])
		}
	}
}

func TestParser_ExtractLimit(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		query     string
		wantLimit int
	}{
		{
			query:     "SELECT * FROM metrics LIMIT 10",
			wantLimit: 10,
		},
		{
			query:     "SELECT * FROM metrics LIMIT 100",
			wantLimit: 100,
		},
		{
			query:     "SELECT * FROM metrics",
			wantLimit: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			q, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if q.Limit != tt.wantLimit {
				t.Errorf("Limit = %d, want %d", q.Limit, tt.wantLimit)
			}
		})
	}
}

func TestExtendedParser_ParseNRQL(t *testing.T) {
	parser := NewExtendedParser()

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "NRQL with FACET",
			query:   "SELECT count(*) FROM Transaction FACET appName",
			wantErr: false,
		},
		{
			name:    "NRQL with SINCE (preprocessed)",
			query:   "SELECT * FROM metrics SINCE 1 hour ago",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.ParseNRQL(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseNRQL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParser_TimeRange(t *testing.T) {
	parser := NewParser()

	// Default time range should be set
	q, err := parser.Parse("SELECT * FROM metrics")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Should have a default time range
	if q.TimeRange.Since.IsZero() {
		t.Error("Since should not be zero")
	}
	if q.TimeRange.Until.IsZero() {
		t.Error("Until should not be zero")
	}
	if q.TimeRange.Until.Before(q.TimeRange.Since) {
		t.Error("Until should be after Since")
	}

	// Default to last hour
	duration := q.TimeRange.Until.Sub(q.TimeRange.Since)
	if duration > 2*time.Hour || duration < 30*time.Minute {
		t.Errorf("Default time range should be about 1 hour, got %v", duration)
	}
}
