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

// LokiAdapter handles queries to Grafana Loki for logs
type LokiAdapter struct {
	baseURL    string
	httpClient *http.Client
	tenantID   string
}

// NewLokiAdapter creates a new Loki backend adapter
func NewLokiAdapter(baseURL string, tenantID string) *LokiAdapter {
	return &LokiAdapter{
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		tenantID:   tenantID,
	}
}

// ExecuteQuery executes a query against Loki
func (l *LokiAdapter) ExecuteQuery(ctx context.Context, query QueryRequest) (*QueryResponse, error) {
	logQL := l.translateToLogQL(query)
	
	// Build the query URL
	queryURL := fmt.Sprintf("%s/loki/api/v1/query_range", l.baseURL)
	
	params := url.Values{}
	params.Set("query", logQL)
	params.Set("start", fmt.Sprintf("%d", query.TimeRange.Since.UnixNano()))
	params.Set("end", fmt.Sprintf("%d", query.TimeRange.Until.UnixNano()))
	params.Set("direction", "forward")
	
	if query.Limit > 0 {
		params.Set("limit", fmt.Sprintf("%d", query.Limit))
	} else {
		params.Set("limit", "1000")
	}
	
	req, err := http.NewRequestWithContext(ctx, "GET", queryURL+"?"+params.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	// Add tenant header if configured
	if l.tenantID != "" {
		req.Header.Set("X-Scope-OrgID", l.tenantID)
	}
	
	resp, err := l.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}
	
	return l.parseResponse(resp.Body)
}

// translateToLogQL converts our query format to LogQL
func (l *LokiAdapter) translateToLogQL(query QueryRequest) string {
	var logQL strings.Builder
	
	// Start with label selector
	logQL.WriteString("{")
	
	// Add label filters
	labelFilters := make([]string, 0)
	lineFilters := make([]string, 0)
	
	for _, filter := range query.Filters {
		// Determine if this is a label or line filter
		if l.isLabelFilter(filter.Field) {
			op := l.translateLabelOperator(filter.Operator)
			labelFilters = append(labelFilters, fmt.Sprintf(`%s%s"%v"`, filter.Field, op, filter.Value))
		} else {
			// Line filter
			lineFilters = append(lineFilters, l.buildLineFilter(filter))
		}
	}
	
	// Add default job label if no labels specified
	if len(labelFilters) == 0 {
		labelFilters = append(labelFilters, `job=~".+"`)
	}
	
	logQL.WriteString(strings.Join(labelFilters, ", "))
	logQL.WriteString("}")
	
	// Add line filters
	for _, lineFilter := range lineFilters {
		logQL.WriteString(" ")
		logQL.WriteString(lineFilter)
	}
	
	// Add parsing stage if needed
	if l.needsParsing(query) {
		logQL.WriteString(" | json") // Default to JSON parsing
	}
	
	// Add aggregations
	if len(query.Aggregates) > 0 {
		for _, agg := range query.Aggregates {
			aggregation := l.buildAggregation(agg, query.GroupBy)
			if aggregation != "" {
				// For LogQL, aggregations are typically wrapped around the selector
				return fmt.Sprintf("%s(%s)", aggregation, logQL.String())
			}
		}
	}
	
	return logQL.String()
}

// isLabelFilter determines if a field is a label filter
func (l *LokiAdapter) isLabelFilter(field string) bool {
	// Common Loki labels
	commonLabels := []string{"job", "app", "env", "environment", "namespace", 
		"pod", "container", "instance", "level", "severity"}
	
	for _, label := range commonLabels {
		if strings.EqualFold(field, label) {
			return true
		}
	}
	return false
}

// translateLabelOperator converts our filter operators to LogQL label operators
func (l *LokiAdapter) translateLabelOperator(op string) string {
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

// buildLineFilter builds a LogQL line filter
func (l *LokiAdapter) buildLineFilter(filter Filter) string {
	switch filter.Operator {
	case "contains":
		return fmt.Sprintf(`|= "%v"`, filter.Value)
	case "not_contains":
		return fmt.Sprintf(`!= "%v"`, filter.Value)
	case "regex":
		return fmt.Sprintf(`|~ "%v"`, filter.Value)
	default:
		return fmt.Sprintf(`|= "%v"`, filter.Value)
	}
}

// needsParsing determines if query needs parsing stage
func (l *LokiAdapter) needsParsing(query QueryRequest) bool {
	// Check if any field references parsed fields
	for _, filter := range query.Filters {
		if !l.isLabelFilter(filter.Field) && strings.Contains(filter.Field, ".") {
			return true
		}
	}
	for _, groupBy := range query.GroupBy {
		if !l.isLabelFilter(groupBy) {
			return true
		}
	}
	return false
}

// buildAggregation builds LogQL aggregation expression
func (l *LokiAdapter) buildAggregation(agg Aggregate, groupBy []string) string {
	var aggFunc string
	
	switch agg.Function {
	case "count":
		aggFunc = "count_over_time"
	case "rate":
		aggFunc = "rate"
	case "sum":
		aggFunc = "sum"
	case "avg":
		aggFunc = "avg_over_time"
	default:
		return ""
	}
	
	// Add grouping if specified
	if len(groupBy) > 0 {
		return fmt.Sprintf("%s by (%s)", aggFunc, strings.Join(groupBy, ", "))
	}
	
	return aggFunc
}

// parseResponse parses the Loki API response
func (l *LokiAdapter) parseResponse(body io.Reader) (*QueryResponse, error) {
	var lokiResp LokiResponse
	if err := json.NewDecoder(body).Decode(&lokiResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	
	if lokiResp.Status != "success" {
		return nil, fmt.Errorf("query failed: %s", lokiResp.Error)
	}
	
	return l.convertToQueryResponse(lokiResp), nil
}

// convertToQueryResponse converts Loki response to our format
func (l *LokiAdapter) convertToQueryResponse(lokiResp LokiResponse) *QueryResponse {
	response := &QueryResponse{
		Results: make([]Result, 0),
	}
	
	switch lokiResp.Data.ResultType {
	case "streams":
		for _, stream := range lokiResp.Data.Result {
			r := Result{
				Labels: stream.Stream,
				Values: make([]DataPoint, 0, len(stream.Values)),
			}
			
			for _, value := range stream.Values {
				// Loki returns [timestamp_nano, log_line]
				timestampNano, _ := value[0].(string)
				logLine, _ := value[1].(string)
				
				nano, _ := fmt.Sscanf(timestampNano, "%d", new(int64))
				
				r.Values = append(r.Values, DataPoint{
					Timestamp: time.Unix(0, int64(nano)),
					Value:     logLine,
				})
			}
			
			response.Results = append(response.Results, r)
		}
	case "matrix":
		// Handle metric queries from Loki
		for _, series := range lokiResp.Data.Result {
			r := Result{
				Labels: series.Metric,
				Values: make([]DataPoint, 0, len(series.Values)),
			}
			
			for _, value := range series.Values {
				timestamp := int64(value[0].(float64))
				val := value[1].(string)
				
				r.Values = append(r.Values, DataPoint{
					Timestamp: time.Unix(timestamp, 0),
					Value:     val,
				})
			}
			
			response.Results = append(response.Results, r)
		}
	}
	
	return response
}

// LokiResponse represents the Loki API response format
type LokiResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string `json:"stream,omitempty"`
			Metric map[string]string `json:"metric,omitempty"`
			Values [][]interface{}   `json:"values"`
		} `json:"result"`
	} `json:"data"`
	Error string `json:"error,omitempty"`
}

// Stream executes a streaming query against Loki
func (l *LokiAdapter) Stream(ctx context.Context, query QueryRequest, ch chan<- Result) error {
	logQL := l.translateToLogQL(query)
	
	// Use tail endpoint for streaming
	queryURL := fmt.Sprintf("%s/loki/api/v1/tail", l.baseURL)
	
	params := url.Values{}
	params.Set("query", logQL)
	params.Set("start", fmt.Sprintf("%d", query.TimeRange.Since.UnixNano()))
	
	req, err := http.NewRequestWithContext(ctx, "GET", queryURL+"?"+params.Encode(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	
	if l.tenantID != "" {
		req.Header.Set("X-Scope-OrgID", l.tenantID)
	}
	
	resp, err := l.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("tail failed with status %d: %s", resp.StatusCode, string(body))
	}
	
	// Read streaming response
	decoder := json.NewDecoder(resp.Body)
	for {
		var streamResp struct {
			Streams []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"streams"`
		}
		
		if err := decoder.Decode(&streamResp); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		
		for _, stream := range streamResp.Streams {
			result := Result{
				Labels: stream.Stream,
				Values: make([]DataPoint, 0, len(stream.Values)),
			}
			
			for _, value := range stream.Values {
				timestampNano, _ := fmt.Sscanf(value[0], "%d", new(int64))
				result.Values = append(result.Values, DataPoint{
					Timestamp: time.Unix(0, int64(timestampNano)),
					Value:     value[1],
				})
			}
			
			select {
			case ch <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	
	return nil
}

// GetCapabilities returns the capabilities of the Loki backend
func (l *LokiAdapter) GetCapabilities() Capabilities {
	return Capabilities{
		SupportsAggregation: true,
		SupportsGroupBy:     true,
		SupportsRate:        true,
		SupportsHistogram:   false,
		MaxTimeRange:        30 * 24 * time.Hour, // 30 days
		SupportedFunctions: []string{
			"count",
			"rate",
			"sum",
			"avg",
		},
	}
}