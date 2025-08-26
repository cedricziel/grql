package engine

import (
	"context"
)

// PlanNode represents a node in the query execution plan
type PlanNode interface {
	Execute(ctx context.Context) (ResultSet, error)
	Children() []PlanNode
	Cost() int64
}

// ResultSet represents the results from executing a plan node
type ResultSet interface {
	Next() bool
	Current() Row
	Close() error
	Schema() Schema
}

// Row represents a single row of data
type Row struct {
	Values map[string]interface{}
}

// Schema describes the structure of a result set
type Schema struct {
	Columns []Column
}

// Column describes a single column in the result set
type Column struct {
	Name string
	Type DataType
}

// DataType represents the data type of a column
type DataType string

const (
	DataTypeString    DataType = "string"
	DataTypeInt       DataType = "int"
	DataTypeFloat     DataType = "float"
	DataTypeBool      DataType = "bool"
	DataTypeTimestamp DataType = "timestamp"
)

// LogicalPlan represents the logical query plan
type LogicalPlan struct {
	Root PlanNode
}

// PhysicalPlan represents the physical query plan
type PhysicalPlan struct {
	Root     PlanNode
	Backend  Backend
	Parallel bool
}

// Backend represents a data source backend
type Backend string

const (
	BackendMimir Backend = "mimir"
	BackendLoki  Backend = "loki"
	BackendTempo Backend = "tempo"
)

// ScanNode represents a table/data source scan
type ScanNode struct {
	DataSource DataSource
	Backend    Backend
	Filters    []Filter
	TimeRange  TimeRange
	Columns    []string
}

func (s *ScanNode) Execute(ctx context.Context) (ResultSet, error) {
	// Implementation will delegate to appropriate backend adapter
	return nil, nil
}

func (s *ScanNode) Children() []PlanNode {
	return nil
}

func (s *ScanNode) Cost() int64 {
	// Estimate cost based on time range and filters
	hours := s.TimeRange.Until.Sub(s.TimeRange.Since).Hours()
	return int64(hours * 100) // Simplified cost model
}

// FilterNode represents a filter operation
type FilterNode struct {
	Child   PlanNode
	Filters []Filter
}

func (f *FilterNode) Execute(ctx context.Context) (ResultSet, error) {
	childResult, err := f.Child.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return &FilteredResultSet{
		source:  childResult,
		filters: f.Filters,
	}, nil
}

func (f *FilterNode) Children() []PlanNode {
	return []PlanNode{f.Child}
}

func (f *FilterNode) Cost() int64 {
	return f.Child.Cost() + int64(len(f.Filters)*10)
}

// AggregateNode represents an aggregation operation
type AggregateNode struct {
	Child      PlanNode
	GroupBy    []string
	Aggregates []Aggregate
}

func (a *AggregateNode) Execute(ctx context.Context) (ResultSet, error) {
	childResult, err := a.Child.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return &AggregatedResultSet{
		source:     childResult,
		groupBy:    a.GroupBy,
		aggregates: a.Aggregates,
	}, nil
}

func (a *AggregateNode) Children() []PlanNode {
	return []PlanNode{a.Child}
}

func (a *AggregateNode) Cost() int64 {
	return a.Child.Cost() + int64(len(a.Aggregates)*20)
}

// JoinNode represents a join between two data sources
type JoinNode struct {
	Left      PlanNode
	Right     PlanNode
	JoinType  JoinType
	Condition JoinCondition
}

func (j *JoinNode) Execute(ctx context.Context) (ResultSet, error) {
	// Execute both children in parallel
	leftCh := make(chan resultOrError)
	rightCh := make(chan resultOrError)

	go func() {
		result, err := j.Left.Execute(ctx)
		leftCh <- resultOrError{result, err}
	}()

	go func() {
		result, err := j.Right.Execute(ctx)
		rightCh <- resultOrError{result, err}
	}()

	leftRes := <-leftCh
	rightRes := <-rightCh

	if leftRes.err != nil {
		return nil, leftRes.err
	}
	if rightRes.err != nil {
		return nil, rightRes.err
	}

	return &JoinedResultSet{
		left:      leftRes.result,
		right:     rightRes.result,
		joinType:  j.JoinType,
		condition: j.Condition,
	}, nil
}

func (j *JoinNode) Children() []PlanNode {
	return []PlanNode{j.Left, j.Right}
}

func (j *JoinNode) Cost() int64 {
	return j.Left.Cost() + j.Right.Cost() + 100 // Join overhead
}

// UnionNode represents a union of multiple result sets
type UnionNode struct {
	ChildNodes []PlanNode
}

func (u *UnionNode) Execute(ctx context.Context) (ResultSet, error) {
	results := make([]ResultSet, 0, len(u.ChildNodes))
	for _, child := range u.ChildNodes {
		result, err := child.Execute(ctx)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return &UnionResultSet{sources: results}, nil
}

func (u *UnionNode) Children() []PlanNode {
	return u.ChildNodes
}

func (u *UnionNode) Cost() int64 {
	var totalCost int64
	for _, child := range u.ChildNodes {
		totalCost += child.Cost()
	}
	return totalCost
}

// JoinType represents the type of join
type JoinType string

const (
	JoinTypeInner JoinType = "inner"
	JoinTypeLeft  JoinType = "left"
	JoinTypeRight JoinType = "right"
	JoinTypeFull  JoinType = "full"
	JoinTypeCross JoinType = "cross"
)

// JoinCondition represents a join condition
type JoinCondition struct {
	LeftField  string
	RightField string
	Operator   FilterOperator
}

// Helper types for result sets
type resultOrError struct {
	result ResultSet
	err    error
}

// FilteredResultSet implements ResultSet with filtering
type FilteredResultSet struct {
	source  ResultSet
	filters []Filter
	current Row
}

func (f *FilteredResultSet) Next() bool {
	for f.source.Next() {
		row := f.source.Current()
		if f.matchesFilters(row) {
			f.current = row
			return true
		}
	}
	return false
}

func (f *FilteredResultSet) Current() Row {
	return f.current
}

func (f *FilteredResultSet) Close() error {
	return f.source.Close()
}

func (f *FilteredResultSet) Schema() Schema {
	return f.source.Schema()
}

func (f *FilteredResultSet) matchesFilters(row Row) bool {
	for _, filter := range f.filters {
		if !f.matchesFilter(row, filter) {
			return false
		}
	}
	return true
}

func (f *FilteredResultSet) matchesFilter(row Row, filter Filter) bool {
	value, ok := row.Values[filter.Field]
	if !ok {
		return false
	}

	// Simplified filter matching - would be more comprehensive in production
	switch filter.Operator {
	case FilterOpEqual:
		return value == filter.Value
	case FilterOpNotEqual:
		return value != filter.Value
	default:
		return true
	}
}

// AggregatedResultSet implements ResultSet with aggregation
type AggregatedResultSet struct {
	source     ResultSet
	groupBy    []string
	aggregates []Aggregate
	current    Row
	index      int
	results    []Row
}

func (a *AggregatedResultSet) Next() bool {
	// Simplified implementation
	if a.results == nil {
		a.computeAggregates()
	}
	if a.index < len(a.results) {
		a.current = a.results[a.index]
		a.index++
		return true
	}
	return false
}

func (a *AggregatedResultSet) Current() Row {
	return a.current
}

func (a *AggregatedResultSet) Close() error {
	return a.source.Close()
}

func (a *AggregatedResultSet) Schema() Schema {
	// Build schema from group by and aggregate columns
	columns := make([]Column, 0)
	for _, gb := range a.groupBy {
		columns = append(columns, Column{Name: gb, Type: DataTypeString})
	}
	for _, agg := range a.aggregates {
		columns = append(columns, Column{Name: agg.Alias, Type: DataTypeFloat})
	}
	return Schema{Columns: columns}
}

func (a *AggregatedResultSet) computeAggregates() {
	// Simplified aggregation - would be more sophisticated in production
	a.results = make([]Row, 0)
	// Group rows and compute aggregates
}

// JoinedResultSet implements ResultSet for joins
type JoinedResultSet struct {
	left      ResultSet
	right     ResultSet
	joinType  JoinType
	condition JoinCondition
	current   Row
}

func (j *JoinedResultSet) Next() bool {
	// Simplified join implementation
	return false
}

func (j *JoinedResultSet) Current() Row {
	return j.current
}

func (j *JoinedResultSet) Close() error {
	if err := j.left.Close(); err != nil {
		return err
	}
	return j.right.Close()
}

func (j *JoinedResultSet) Schema() Schema {
	// Merge schemas from both sides
	leftSchema := j.left.Schema()
	rightSchema := j.right.Schema()
	columns := append(leftSchema.Columns, rightSchema.Columns...)
	return Schema{Columns: columns}
}

// UnionResultSet implements ResultSet for unions
type UnionResultSet struct {
	sources []ResultSet
	current Row
	index   int
}

func (u *UnionResultSet) Next() bool {
	for u.index < len(u.sources) {
		if u.sources[u.index].Next() {
			u.current = u.sources[u.index].Current()
			return true
		}
		u.index++
	}
	return false
}

func (u *UnionResultSet) Current() Row {
	return u.current
}

func (u *UnionResultSet) Close() error {
	for _, source := range u.sources {
		if err := source.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (u *UnionResultSet) Schema() Schema {
	if len(u.sources) > 0 {
		return u.sources[0].Schema()
	}
	return Schema{}
}
