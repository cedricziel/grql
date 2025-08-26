package backends

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// TimeRange represents a time range
type TimeRange struct {
	Since time.Time
	Until time.Time
}

// Filter represents a filter condition
type Filter struct {
	Field    string
	Operator string
	Value    interface{}
}

// Aggregate represents an aggregation
type Aggregate struct {
	Function string
	Field    string
	Alias    string
}

// QueryRequest represents a query request to a backend
type QueryRequest struct {
	MetricName string
	TimeRange  TimeRange
	Filters    []Filter
	GroupBy    []string
	Aggregates []Aggregate
	Limit      int
}

// MimirAdapter handles queries to Grafana Mimir for metrics
type MimirAdapter struct {
	baseURL    string
	httpClient *http.Client
	tenantID   string
}

// NewMimirAdapter creates a new Mimir backend adapter
func NewMimirAdapter(baseURL string, tenantID string) *MimirAdapter {
	return &MimirAdapter{
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		tenantID:   tenantID,
	}
}

// ExecuteQuery executes a query against Mimir
func (m *MimirAdapter) ExecuteQuery(ctx context.Context, query QueryRequest) (*QueryResponse, error) {
	promQL := m.translateToPromQL(query)

	// Build the query URL
	queryURL := fmt.Sprintf("%s/api/v1/query_range", m.baseURL)

	params := url.Values{}
	params.Set("query", promQL)
	params.Set("start", fmt.Sprintf("%d", query.TimeRange.Since.Unix()))
	params.Set("end", fmt.Sprintf("%d", query.TimeRange.Until.Unix()))
	params.Set("step", m.calculateStep(query.TimeRange))

	req, err := http.NewRequestWithContext(ctx, "GET", queryURL+"?"+params.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add tenant header if configured
	if m.tenantID != "" {
		req.Header.Set("X-Scope-OrgID", m.tenantID)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	return m.parseResponse(resp.Body)
}

// translateToPromQL converts our query format to PromQL
func (m *MimirAdapter) translateToPromQL(query QueryRequest) string {
	// This is a simplified translation - would be more sophisticated in production
	var promQL strings.Builder

	// Handle aggregations
	if len(query.Aggregates) > 0 {
		for _, agg := range query.Aggregates {
			switch agg.Function {
			case "count":
				promQL.WriteString(fmt.Sprintf("count(%s)", query.MetricName))
			case "sum":
				promQL.WriteString(fmt.Sprintf("sum(%s)", query.MetricName))
			case "avg":
				promQL.WriteString(fmt.Sprintf("avg(%s)", query.MetricName))
			case "min":
				promQL.WriteString(fmt.Sprintf("min(%s)", query.MetricName))
			case "max":
				promQL.WriteString(fmt.Sprintf("max(%s)", query.MetricName))
			case "rate":
				promQL.WriteString(fmt.Sprintf("rate(%s[5m])", query.MetricName))
			case "p95":
				promQL.WriteString(fmt.Sprintf("quantile(0.95, %s)", query.MetricName))
			case "p99":
				promQL.WriteString(fmt.Sprintf("quantile(0.99, %s)", query.MetricName))
			default:
				promQL.WriteString(query.MetricName)
			}

			// Add grouping
			if len(query.GroupBy) > 0 {
				promQL.WriteString(fmt.Sprintf(" by (%s)", strings.Join(query.GroupBy, ", ")))
			}

			break // For now, handle only first aggregate
		}
	} else {
		// No aggregation, just the metric
		promQL.WriteString(query.MetricName)
	}

	// Add label filters
	if len(query.Filters) > 0 {
		promQL.WriteString("{")
		filters := make([]string, 0, len(query.Filters))
		for _, filter := range query.Filters {
			op := m.translateOperator(filter.Operator)
			filters = append(filters, fmt.Sprintf(`%s%s"%v"`, filter.Field, op, filter.Value))
		}
		promQL.WriteString(strings.Join(filters, ", "))
		promQL.WriteString("}")
	}

	return promQL.String()
}

// translateOperator converts our filter operators to PromQL operators
func (m *MimirAdapter) translateOperator(op string) string {
	switch op {
	case "=":
		return "="
	case "!=":
		return "!="
	case "regex":
		return "=~"
	default:
		return "="
	}
}

// calculateStep determines the query step based on time range
func (m *MimirAdapter) calculateStep(timeRange TimeRange) string {
	duration := timeRange.Until.Sub(timeRange.Since)

	// Auto-calculate step based on duration
	switch {
	case duration <= 1*time.Hour:
		return "15s"
	case duration <= 6*time.Hour:
		return "30s"
	case duration <= 24*time.Hour:
		return "1m"
	case duration <= 7*24*time.Hour:
		return "5m"
	default:
		return "1h"
	}
}

// parseResponse parses the Prometheus API response
func (m *MimirAdapter) parseResponse(body io.Reader) (*QueryResponse, error) {
	var promResp PrometheusResponse
	if err := json.NewDecoder(body).Decode(&promResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if promResp.Status != "success" {
		return nil, fmt.Errorf("query failed: %s", promResp.Error)
	}

	return m.convertToQueryResponse(promResp), nil
}

// convertToQueryResponse converts Prometheus response to our format
func (m *MimirAdapter) convertToQueryResponse(promResp PrometheusResponse) *QueryResponse {
	response := &QueryResponse{
		Results: make([]Result, 0),
	}

	for _, result := range promResp.Data.Result {
		// Convert each metric result
		r := Result{
			Labels: result.Metric,
			Values: make([]DataPoint, 0, len(result.Values)),
		}

		for _, value := range result.Values {
			timestamp := int64(value[0].(float64))
			val := value[1].(string)

			r.Values = append(r.Values, DataPoint{
				Timestamp: time.Unix(timestamp, 0),
				Value:     val,
			})
		}

		response.Results = append(response.Results, r)
	}

	return response
}

// QueryResponse represents a response from a backend
type QueryResponse struct {
	Results []Result
	Error   string
}

// Result represents a single time series result
type Result struct {
	Labels map[string]string
	Values []DataPoint
}

// DataPoint represents a single data point
type DataPoint struct {
	Timestamp time.Time
	Value     string
}

// PrometheusResponse represents the Prometheus API response format
type PrometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][]interface{}   `json:"values"`
		} `json:"result"`
	} `json:"data"`
	Error string `json:"error,omitempty"`
}

// Stream executes a streaming query against Mimir
func (m *MimirAdapter) Stream(ctx context.Context, query QueryRequest, ch chan<- Result) error {
	// For streaming, we can break the time range into smaller chunks
	chunkDuration := 1 * time.Hour
	current := query.TimeRange.Since

	for current.Before(query.TimeRange.Until) {
		end := current.Add(chunkDuration)
		if end.After(query.TimeRange.Until) {
			end = query.TimeRange.Until
		}

		chunkQuery := query
		chunkQuery.TimeRange = TimeRange{
			Since: current,
			Until: end,
		}

		resp, err := m.ExecuteQuery(ctx, chunkQuery)
		if err != nil {
			return err
		}

		for _, result := range resp.Results {
			select {
			case ch <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		current = end
	}

	return nil
}

// GetCapabilities returns the capabilities of the Mimir backend
func (m *MimirAdapter) GetCapabilities() Capabilities {
	return Capabilities{
		SupportsAggregation: true,
		SupportsGroupBy:     true,
		SupportsRate:        true,
		SupportsHistogram:   true,
		MaxTimeRange:        90 * 24 * time.Hour, // 90 days
		SupportedFunctions: []string{
			"count",
			"sum",
			"avg",
			"min",
			"max",
			"rate",
			"p95",
			"p99",
		},
	}
}

// Capabilities describes what a backend supports
type Capabilities struct {
	SupportsAggregation bool
	SupportsGroupBy     bool
	SupportsRate        bool
	SupportsHistogram   bool
	MaxTimeRange        time.Duration
	SupportedFunctions  []string
}
