package engine

import "time"

// Common types used across the engine

// FilterOperator represents filter operation types
type FilterOperator string

const (
	FilterOpEqual       FilterOperator = "="
	FilterOpNotEqual    FilterOperator = "!="
	FilterOpGreater     FilterOperator = ">"
	FilterOpLess        FilterOperator = "<"
	FilterOpGreaterEq   FilterOperator = ">="
	FilterOpLessEq      FilterOperator = "<="
	FilterOpContains    FilterOperator = "contains"
	FilterOpNotContains FilterOperator = "not_contains"
	FilterOpRegex       FilterOperator = "regex"
)

// Filter represents a filter condition
type Filter struct {
	Value    interface{}
	Field    string
	Operator FilterOperator
}

// AggregateFunction represents aggregation function types
type AggregateFunction string

const (
	AggFuncCount  AggregateFunction = "count"
	AggFuncSum    AggregateFunction = "sum"
	AggFuncAvg    AggregateFunction = "avg"
	AggFuncMin    AggregateFunction = "min"
	AggFuncMax    AggregateFunction = "max"
	AggFuncStdDev AggregateFunction = "stddev"
	AggFuncRate   AggregateFunction = "rate"
	AggFuncP95    AggregateFunction = "p95"
	AggFuncP99    AggregateFunction = "p99"
)

// Aggregate represents an aggregation function
type Aggregate struct {
	Function AggregateFunction
	Field    string
	Alias    string
}

// TimeRange represents a time range for queries
type TimeRange struct {
	Since time.Time
	Until time.Time
}
