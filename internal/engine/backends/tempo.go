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

// TempoAdapter handles queries to Grafana Tempo for traces
type TempoAdapter struct {
	baseURL    string
	httpClient *http.Client
	tenantID   string
}

// NewTempoAdapter creates a new Tempo backend adapter
func NewTempoAdapter(baseURL string, tenantID string) *TempoAdapter {
	return &TempoAdapter{
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		tenantID:   tenantID,
	}
}

// ExecuteQuery executes a query against Tempo
func (t *TempoAdapter) ExecuteQuery(ctx context.Context, query QueryRequest) (*QueryResponse, error) {
	traceQL := t.translateToTraceQL(query)

	// Build the query URL for search
	queryURL := fmt.Sprintf("%s/api/search", t.baseURL)

	params := url.Values{}
	params.Set("q", traceQL)
	params.Set("start", fmt.Sprintf("%d", query.TimeRange.Since.Unix()))
	params.Set("end", fmt.Sprintf("%d", query.TimeRange.Until.Unix()))

	if query.Limit > 0 {
		params.Set("limit", fmt.Sprintf("%d", query.Limit))
	} else {
		params.Set("limit", "100")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", queryURL+"?"+params.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add tenant header if configured
	if t.tenantID != "" {
		req.Header.Set("X-Scope-OrgID", t.tenantID)
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("query failed with status %d: failed to read error body: %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	return t.parseResponse(resp.Body)
}

// translateToTraceQL converts our query format to TraceQL
func (t *TempoAdapter) translateToTraceQL(query QueryRequest) string {
	var traceQL strings.Builder

	// TraceQL syntax: { span.attribute = "value" && resource.attribute = "value" }
	traceQL.WriteString("{")

	conditions := make([]string, 0)

	// Add filters
	for _, filter := range query.Filters {
		condition := t.buildTraceCondition(filter)
		if condition != "" {
			conditions = append(conditions, condition)
		}
	}

	// Add duration filters if present
	if t.hasDurationFilter(query) {
		// Example: duration > 100ms
		conditions = append(conditions, "duration > 100ms")
	}

	if len(conditions) > 0 {
		traceQL.WriteString(strings.Join(conditions, " && "))
	} else {
		// Default to all traces
		traceQL.WriteString(".service.name != \"\"")
	}

	traceQL.WriteString("}")

	// Add aggregations if present
	if len(query.Aggregates) > 0 {
		for _, agg := range query.Aggregates {
			aggregation := t.buildAggregation(agg)
			if aggregation != "" {
				traceQL.WriteString(" | ")
				traceQL.WriteString(aggregation)
			}
		}
	}

	// Add grouping
	if len(query.GroupBy) > 0 {
		traceQL.WriteString(" | by(")
		traceQL.WriteString(strings.Join(query.GroupBy, ", "))
		traceQL.WriteString(")")
	}

	return traceQL.String()
}

// buildTraceCondition builds a TraceQL condition from a filter
func (t *TempoAdapter) buildTraceCondition(filter Filter) string {
	// Map common fields to TraceQL attributes
	field := t.mapFieldToTraceQL(filter.Field)

	switch filter.Operator {
	case "=":
		return fmt.Sprintf(`%s = "%v"`, field, filter.Value)
	case "!=":
		return fmt.Sprintf(`%s != "%v"`, field, filter.Value)
	case ">":
		return fmt.Sprintf(`%s > %v`, field, filter.Value)
	case "<":
		return fmt.Sprintf(`%s < %v`, field, filter.Value)
	case ">=":
		return fmt.Sprintf(`%s >= %v`, field, filter.Value)
	case "<=":
		return fmt.Sprintf(`%s <= %v`, field, filter.Value)
	case "regex":
		return fmt.Sprintf(`%s =~ "%v"`, field, filter.Value)
	case "contains":
		// TraceQL doesn't have direct contains, use regex
		return fmt.Sprintf(`%s =~ ".*%v.*"`, field, filter.Value)
	default:
		return ""
	}
}

// mapFieldToTraceQL maps common field names to TraceQL attributes
func (t *TempoAdapter) mapFieldToTraceQL(field string) string {
	// Map common fields to TraceQL namespaces
	switch strings.ToLower(field) {
	case "service", "service_name", "servicename":
		return ".service.name"
	case "operation", "operation_name", "operationname":
		return ".name"
	case "status", "status_code":
		return ".status.code"
	case "duration":
		return "duration"
	case "trace_id", "traceid":
		return ".trace.id"
	case "span_id", "spanid":
		return ".span.id"
	case "parent_id", "parentid":
		return ".parent.span.id"
	case "kind":
		return ".kind"
	default:
		// Check if it's already prefixed
		if strings.HasPrefix(field, ".") || strings.HasPrefix(field, "span.") ||
			strings.HasPrefix(field, "resource.") {
			return field
		}
		// Default to span attribute
		return fmt.Sprintf("span.%s", field)
	}
}

// hasDurationFilter checks if query has duration-related filters
func (t *TempoAdapter) hasDurationFilter(query QueryRequest) bool {
	for _, filter := range query.Filters {
		if strings.Contains(strings.ToLower(filter.Field), "duration") {
			return true
		}
	}
	return false
}

// buildAggregation builds TraceQL aggregation expression
func (t *TempoAdapter) buildAggregation(agg Aggregate) string {
	switch agg.Function {
	case "count":
		return "count()"
	case "avg":
		if agg.Field == "duration" || agg.Field == "" {
			return "avg(duration)"
		}
		return fmt.Sprintf("avg(%s)", t.mapFieldToTraceQL(agg.Field))
	case "min":
		if agg.Field == "duration" || agg.Field == "" {
			return "min(duration)"
		}
		return fmt.Sprintf("min(%s)", t.mapFieldToTraceQL(agg.Field))
	case "max":
		if agg.Field == "duration" || agg.Field == "" {
			return "max(duration)"
		}
		return fmt.Sprintf("max(%s)", t.mapFieldToTraceQL(agg.Field))
	case "sum":
		return fmt.Sprintf("sum(%s)", t.mapFieldToTraceQL(agg.Field))
	default:
		return ""
	}
}

// parseResponse parses the Tempo API response
func (t *TempoAdapter) parseResponse(body io.Reader) (*QueryResponse, error) {
	var tempoResp TempoResponse
	if err := json.NewDecoder(body).Decode(&tempoResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return t.convertToQueryResponse(tempoResp), nil
}

// convertToQueryResponse converts Tempo response to our format
func (t *TempoAdapter) convertToQueryResponse(tempoResp TempoResponse) *QueryResponse {
	response := &QueryResponse{
		Results: make([]Result, 0),
	}

	for _, trace := range tempoResp.Traces {
		// Convert each trace to a result
		r := Result{
			Labels: make(map[string]string),
			Values: make([]DataPoint, 0),
		}

		// Extract trace metadata as labels
		r.Labels["trace_id"] = trace.TraceID
		r.Labels["root_service"] = trace.RootServiceName
		r.Labels["root_trace_name"] = trace.RootTraceName

		// Duration as a data point
		r.Values = append(r.Values, DataPoint{
			Timestamp: time.Unix(0, trace.StartTimeUnixNano),
			Value:     fmt.Sprintf("%d", trace.DurationMs),
		})

		// Add span details if available
		for _, span := range trace.Spans {
			spanLabels := make(map[string]string)
			for k, v := range r.Labels {
				spanLabels[k] = v
			}
			spanLabels["span_id"] = span.SpanID
			spanLabels["span_name"] = span.Name
			spanLabels["service_name"] = span.ServiceName

			spanResult := Result{
				Labels: spanLabels,
				Values: []DataPoint{{
					Timestamp: time.Unix(0, span.StartTimeUnixNano),
					Value:     fmt.Sprintf("%d", span.DurationNano/1000000), // Convert to ms
				}},
			}
			response.Results = append(response.Results, spanResult)
		}

		// If no spans, add the trace itself
		if len(trace.Spans) == 0 {
			response.Results = append(response.Results, r)
		}
	}

	return response
}

// TempoResponse represents the Tempo API response format
type TempoResponse struct {
	Traces []TraceResult `json:"traces"`
}

// TraceResult represents a single trace result
type TraceResult struct {
	TraceID           string       `json:"traceID"`
	RootServiceName   string       `json:"rootServiceName"`
	RootTraceName     string       `json:"rootTraceName"`
	StartTimeUnixNano int64        `json:"startTimeUnixNano"`
	DurationMs        int          `json:"durationMs"`
	Spans             []SpanResult `json:"spans,omitempty"`
}

// SpanResult represents a single span result
type SpanResult struct {
	SpanID            string `json:"spanID"`
	Name              string `json:"name"`
	ServiceName       string `json:"serviceName"`
	StartTimeUnixNano int64  `json:"startTimeUnixNano"`
	DurationNano      int64  `json:"durationNano"`
}

// GetTraceByID retrieves a specific trace by ID
func (t *TempoAdapter) GetTraceByID(ctx context.Context, traceID string) (*TraceDetail, error) {
	queryURL := fmt.Sprintf("%s/api/traces/%s", t.baseURL, traceID)

	req, err := http.NewRequestWithContext(ctx, "GET", queryURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if t.tenantID != "" {
		req.Header.Set("X-Scope-OrgID", t.tenantID)
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get trace: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to get trace with status %d: failed to read error body: %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("failed to get trace with status %d: %s", resp.StatusCode, string(body))
	}

	var traceDetail TraceDetail
	if err := json.NewDecoder(resp.Body).Decode(&traceDetail); err != nil {
		return nil, fmt.Errorf("failed to decode trace: %w", err)
	}

	return &traceDetail, nil
}

// TraceDetail represents detailed trace information
type TraceDetail struct {
	TraceID string                   `json:"traceID"`
	Spans   []map[string]interface{} `json:"spans"`
}

// Stream executes a streaming query against Tempo
func (t *TempoAdapter) Stream(ctx context.Context, query QueryRequest, ch chan<- Result) error {
	// Tempo doesn't have native streaming like Loki
	// We can simulate it by polling in intervals
	pollInterval := 5 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	lastQuery := query.TimeRange.Since

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Query for new traces since last check
			currentQuery := query
			currentQuery.TimeRange = TimeRange{
				Since: lastQuery,
				Until: time.Now(),
			}

			resp, err := t.ExecuteQuery(ctx, currentQuery)
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

			lastQuery = time.Now()

			// Stop if we've reached the end time
			if lastQuery.After(query.TimeRange.Until) {
				break
			}
		}
	}

	return nil
}

// GetCapabilities returns the capabilities of the Tempo backend
func (t *TempoAdapter) GetCapabilities() Capabilities {
	return Capabilities{
		SupportsAggregation: true,
		SupportsGroupBy:     true,
		SupportsRate:        false,
		SupportsHistogram:   true,
		MaxTimeRange:        7 * 24 * time.Hour, // 7 days (typical Tempo retention)
		SupportedFunctions: []string{
			"count",
			"avg",
			"min",
			"max",
			"sum",
			"p95",
			"p99",
		},
	}
}
