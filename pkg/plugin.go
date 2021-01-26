package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"google.golang.org/api/iterator"
)

type queryResult struct {
	Time   time.Time `bigquery:"time"`
	Values int64     `bigquery:"metric"`
}

// TransformedResults contains the results from BigQuery for Grafana
type TransformedResults struct {
	Time   []time.Time
	Values []int64
}
type queryModel struct {
	Format string `json:"format"`
	// Constant         string `json:"constant"`
	Dataset          string   `json:"dataset"`
	Group            []string `json:"group"`
	MetricColumn     string   `json:"metricColumn"`
	OrderByCol       string/*int32*/ `json:"orderByCol"`
	OrderBySort      string/*int32*/ `json:"orderBySort"`
	Partitioned      bool   `json:"partitioned"`
	PartitionedField string `json:"partitionedField"`
	Project          string `json:"project"`
	RawQuery         bool   `json:"rawQuery"`
	RawSQL           string `json:"rawSql"`
	RefID            string `json:"refId"`
	// Select           []string `json:"select"`
	Sharded        bool   `json:"sharded"`
	Table          string `json:"table"`
	TimeColumn     string `json:"timeColumn"`
	TimeColumnType string `json:"timeColumnType"`
	Location       string `json:"location"`
	// Where            []string `json:"where"`
}

type instanceSettings struct {
	httpClient *http.Client
}

// SampleDatasource is an example datasource used to scaffold
// new datasource plugins with an backend.
type SampleDatasource struct {
	// The instance manager can help with lifecycle management
	// of datasource instances in plugins. It's not a requirements
	// but a best practice that we recommend that you follow.
	im instancemgmt.InstanceManager
}

// newDatasource returns datasource.ServeOpts.
func newDatasource() datasource.ServeOpts {
	// creates a instance manager for your plugin. The function passed
	// into `NewInstanceManger` is called when the instance is created
	// for the first time or when a datasource configuration changed.
	im := datasource.NewInstanceManager(newDataSourceInstance)
	ds := &SampleDatasource{
		im: im,
	}

	return datasource.ServeOpts{
		QueryDataHandler:   ds,
		CheckHealthHandler: ds,
	}
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (td *SampleDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()
	results := make(chan func(backend.Responses), len(req.Queries))

	// Execute the queries in parallel and collect the responses.
	for _, q := range req.Queries {
		go func(dq backend.DataQuery) {
			res := doQuery(ctx, dq)
			results <- func(r backend.Responses) {
				r[dq.RefID] = res
			}
		}(q)
	}

	// Update the underlying response map with the results of each query.
	for fn := range results {
		fn(response.Responses)
	}

	return response, nil
}

func doQuery(ctx context.Context, query backend.DataQuery) backend.DataResponse {
	// Unmarshal the json into our queryModel
	var qm queryModel

	response := backend.DataResponse{}

	response.Error = json.Unmarshal(query.JSON, &qm)
	if response.Error != nil {
		return response
	}

	rows, err := BigQueryRun(ctx, qm)
	if err != nil {
		log.DefaultLogger.Error("query BigQueryRun error %v", err)
		response.Error = err
		return response
	}
	// Log a warning if `Format` is empty.
	if qm.Format == "" {
		log.DefaultLogger.Warn("format is empty. defaulting to time series")
	}

	// create data frame response
	frame := data.NewFrame("response")

	// add the time dimension
	frame.Fields = append(frame.Fields,
		data.NewField("Time", nil, rows.Time),
	)

	// add values
	frame.Fields = append(frame.Fields,
		data.NewField("Values", nil, rows.Values),
	)

	// add the frames to the response
	response.Frames = append(response.Frames, frame)

	return response
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (td *SampleDatasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var status = backend.HealthStatusOk
	var message = "Data source is working"

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func newDataSourceInstance(_ backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	return &instanceSettings{
		httpClient: &http.Client{},
	}, nil
}

// Dispose is called before creating a new instance to allow plugin authors to cleanup
func (s *instanceSettings) Dispose() {}

// BigQueryRun runs the query against BigQuery
func BigQueryRun(ctx context.Context, query queryModel) (*TransformedResults, error) {
	projectID := query.Project
	var tr TransformedResults

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	q := client.Query(query.RawSQL)
	// Location must match that of the dataset(s) referenced in the query.
	q.Location = query.Location
	// Run the query and print results when the query job is completed.
	job, err := q.Run(ctx)
	if err != nil {
		log.DefaultLogger.Info("Query run error: %v\n", err)
		return nil, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		log.DefaultLogger.Info("Query wait", "error: %v\n", err)
		return nil, err
	}
	if err := status.Err(); err != nil {
		log.DefaultLogger.Info("Query status error: %v\n", err)
		return nil, err
	}
	it, err := job.Read(ctx)

	for {
		var row queryResult
		err = it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.DefaultLogger.Info("Rows iterator err", "Query", err)
			return nil, err
		}
		log.DefaultLogger.Info("Rows", "Query", row)

		tr.Time = append(tr.Time, row.Time)
		tr.Values = append(tr.Values, row.Values)
	}

	return &tr, nil
}
