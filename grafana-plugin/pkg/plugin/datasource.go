package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	pb "github.com/cedricziel/grql/pkg/grql/v1"
	"github.com/cedricziel/grql/grafana-plugin/pkg/models"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces - only those which are required for a particular task.
var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// NewDatasource creates a new datasource instance.
func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	config, err := models.LoadPluginSettings(settings)
	if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}

	// Log the settings to debug
	backend.Logger.Debug("Datasource settings", "host", config.Host, "port", config.Port)

	client, err := NewGrqlClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create grql client: %w", err)
	}

	return &Datasource{
		client:   client,
		settings: config,
	}, nil
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	client   *GrqlClient
	settings *models.PluginSettings
	mu       sync.Mutex
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.client != nil {
		_ = d.client.Close()
		d.client = nil
	}
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	// create response struct
	response := backend.NewQueryDataResponse()

	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, q)

		// save the response in a hashmap
		// based on with RefID as identifier
		response.Responses[q.RefID] = res
	}

	return response, nil
}

type queryModel struct {
	RawQuery string `json:"rawQuery"`
	Format   string `json:"format"`
}

func (d *Datasource) query(ctx context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	var response backend.DataResponse

	// Unmarshal the JSON into our queryModel.
	var qm queryModel
	err := json.Unmarshal(query.JSON, &qm)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
	}

	if qm.RawQuery == "" {
		return backend.ErrDataResponse(backend.StatusBadRequest, "query is empty")
	}

	// Execute the query against grql server
	grqlResp, err := d.client.ExecuteQuery(ctx, qm.RawQuery, nil)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("query execution failed: %v", err.Error()))
	}

	if grqlResp.Error != "" {
		return backend.ErrDataResponse(backend.StatusInternal, grqlResp.Error)
	}

	// Convert grql response to Grafana data frame
	frame, err := d.convertToDataFrame(grqlResp, qm.Format, query.TimeRange)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("failed to convert response: %v", err.Error()))
	}

	// add the frames to the response.
	response.Frames = append(response.Frames, frame)

	return response
}

func (d *Datasource) convertToDataFrame(resp *pb.QueryResponse, format string, timeRange backend.TimeRange) (*data.Frame, error) {
	if resp == nil || resp.Metadata == nil {
		return nil, fmt.Errorf("invalid response: missing metadata")
	}

	frame := data.NewFrame("response")

	// Add metadata
	frame.Meta = &data.FrameMeta{}
	
	// Set preferred visualization based on data type
	if resp.Metadata.DataType == pb.DataType_TIME_SERIES {
		frame.Meta.PreferredVisualization = data.VisTypeGraph
	} else if resp.Metadata.DataType == pb.DataType_LOGS {
		frame.Meta.PreferredVisualization = data.VisTypeLogs
	} else if resp.Metadata.DataType == pb.DataType_TRACES {
		frame.Meta.PreferredVisualization = data.VisTypeTrace
	} else {
		frame.Meta.PreferredVisualization = data.VisTypeTable
	}

	// If no results, return empty frame
	if len(resp.Results) == 0 {
		return frame, nil
	}

	// Get column info
	columns := resp.Metadata.Columns
	if len(columns) == 0 {
		// If no column info, create columns from first result
		if len(resp.Results) > 0 && len(resp.Results[0].Fields) > 0 {
			for key := range resp.Results[0].Fields {
				columns = append(columns, &pb.ColumnInfo{
					Name: key,
					Type: "STRING", // Default type
				})
			}
		} else {
			return frame, nil
		}
	}

	// Create fields based on columns
	fields := make([]*data.Field, len(columns))
	for i, col := range columns {
		// Initialize field values slice
		var values interface{}

		// Determine field type based on column type
		switch col.Type {
		case "INT64", "INTEGER", "INT":
			values = make([]*int64, 0, len(resp.Results))
		case "FLOAT64", "DOUBLE", "FLOAT":
			values = make([]*float64, 0, len(resp.Results))
		case "BOOL", "BOOLEAN":
			values = make([]*bool, 0, len(resp.Results))
		case "TIMESTAMP", "TIME":
			values = make([]*time.Time, 0, len(resp.Results))
		default:
			values = make([]*string, 0, len(resp.Results))
		}

		fields[i] = data.NewField(col.Name, nil, values)
		
		// Apply field unit from metadata if available
		if unit, ok := resp.Metadata.FieldUnits[col.Name]; ok && unit != "" {
			fields[i].Config = &data.FieldConfig{
				Unit: unit,
			}
		}
		
		// Mark time fields if identified in metadata
		for _, timeField := range resp.Metadata.TimeFields {
			if col.Name == timeField {
				fields[i].Config = &data.FieldConfig{
					Interval: 1000, // Default to 1 second interval
				}
				break
			}
		}
	}

	// Populate field values from results (using Fields map structure)
	for _, row := range resp.Results {
		for i, col := range columns {
			val, exists := row.Fields[col.Name]
			if !exists || val == nil {
				// Add nil to the appropriate field
				switch fields[i].Type() {
				case data.FieldTypeNullableInt64:
					fields[i].Append((*int64)(nil))
				case data.FieldTypeNullableFloat64:
					fields[i].Append((*float64)(nil))
				case data.FieldTypeNullableBool:
					fields[i].Append((*bool)(nil))
				case data.FieldTypeNullableTime:
					fields[i].Append((*time.Time)(nil))
				default:
					fields[i].Append((*string)(nil))
				}
				continue
			}

			// Append the actual value based on its type
			switch v := val.Value.(type) {
			case *pb.Value_StringValue:
				if col.Type == "TIMESTAMP" || col.Type == "TIME" {
					// Parse timestamp string
					t, err := time.Parse(time.RFC3339, v.StringValue)
					if err != nil {
						fields[i].Append((*time.Time)(nil))
					} else {
						fields[i].Append(&t)
					}
				} else {
					fields[i].Append(&v.StringValue)
				}
			case *pb.Value_IntValue:
				intVal := int64(v.IntValue)
				fields[i].Append(&intVal)
			case *pb.Value_FloatValue:
				floatVal := float64(v.FloatValue)
				fields[i].Append(&floatVal)
			case *pb.Value_BoolValue:
				fields[i].Append(&v.BoolValue)
			default:
				// Handle unknown types as nil
				fields[i].Append(nil)
			}
		}
	}

	frame.Fields = fields

	return frame, nil
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *Datasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	// Try to execute a simple query to test connectivity
	testQuery := "SELECT 1"

	resp, err := d.client.ExecuteQuery(ctx, testQuery, nil)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("Failed to connect to grql server: %v", err),
		}, nil
	}

	if resp.Error != "" {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("grql server returned error: %s", resp.Error),
		}, nil
	}

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Successfully connected to grql server",
	}, nil
}
