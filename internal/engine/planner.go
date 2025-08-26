package engine

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// Planner creates query execution plans from parsed queries
type Planner struct {
	backends map[Backend]BackendAdapter
}

// BackendAdapter interface for backend-specific operations
type BackendAdapter interface {
	CanHandle(dataSource DataSource) bool
	EstimateCost(query *Query) int64
	CreateScanNode(query *Query) *ScanNode
}

// NewPlanner creates a new query planner
func NewPlanner() *Planner {
	return &Planner{
		backends: make(map[Backend]BackendAdapter),
	}
}

// RegisterBackend registers a backend adapter
func (p *Planner) RegisterBackend(backend Backend, adapter BackendAdapter) {
	p.backends[backend] = adapter
}

// CreateLogicalPlan creates a logical plan from a parsed query
func (p *Planner) CreateLogicalPlan(query *Query) (*LogicalPlan, error) {
	switch stmt := query.Statement.(type) {
	case *sqlparser.Select:
		return p.createSelectPlan(query, stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// createSelectPlan creates a logical plan for SELECT statements
func (p *Planner) createSelectPlan(query *Query, stmt *sqlparser.Select) (*LogicalPlan, error) {
	// Start with scan node
	backend := p.selectBackend(query.DataSource)
	if backend == "" {
		return nil, fmt.Errorf("no backend available for data source: %s", query.DataSource)
	}

	scanNode := &ScanNode{
		DataSource: query.DataSource,
		Backend:    backend,
		TimeRange:  query.TimeRange,
		Columns:    p.extractColumns(stmt.SelectExprs.Exprs),
	}

	var root PlanNode = scanNode

	// Add filters from WHERE clause
	if stmt.Where != nil {
		filters := p.extractFilters(stmt.Where.Expr)
		if len(filters) > 0 {
			// Push down filters to scan if possible
			pushDownFilters, remainingFilters := p.pushDownFilters(filters, backend)
			scanNode.Filters = pushDownFilters

			if len(remainingFilters) > 0 {
				root = &FilterNode{
					Child:   root,
					Filters: remainingFilters,
				}
			}
		}
	}

	// Add aggregations and grouping
	if stmt.GroupBy != nil || p.hasAggregates(stmt.SelectExprs.Exprs) {
		aggregates := p.extractAggregates(stmt.SelectExprs.Exprs)
		var groupBy []string
		if stmt.GroupBy != nil {
			groupBy = p.extractGroupBy(stmt.GroupBy.Exprs)
		}

		root = &AggregateNode{
			Child:      root,
			GroupBy:    groupBy,
			Aggregates: aggregates,
		}
	}

	// Handle JOIN operations
	if len(stmt.From) > 1 {
		root = p.createJoinPlan(stmt.From, root)
	}

	// Handle UNION operations (if present in query)
	// This would be extended to handle UNION queries

	return &LogicalPlan{Root: root}, nil
}

// selectBackend selects the appropriate backend for a data source
func (p *Planner) selectBackend(dataSource DataSource) Backend {
	switch dataSource {
	case DataSourceMetrics:
		return BackendMimir
	case DataSourceLogs:
		return BackendLoki
	case DataSourceTraces:
		return BackendTempo
	default:
		return ""
	}
}

// extractColumns extracts column names from SELECT expressions
func (p *Planner) extractColumns(selectExprs []sqlparser.SelectExpr) []string {
	columns := make([]string, 0)

	for _, expr := range selectExprs {
		switch e := expr.(type) {
		case *sqlparser.AliasedExpr:
			if colName, ok := e.Expr.(*sqlparser.ColName); ok {
				columns = append(columns, colName.Name.String())
			}
		case *sqlparser.StarExpr:
			// SELECT * - return empty to indicate all columns
			return nil
		}
	}

	return columns
}

// extractFilters extracts filter conditions from WHERE clause
func (p *Planner) extractFilters(expr sqlparser.Expr) []Filter {
	filters := make([]Filter, 0)

	//nolint:errcheck // Walk errors are not recoverable in this context
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch n := node.(type) {
		case *sqlparser.ComparisonExpr:
			filter := p.extractComparisonFilter(n)
			if filter != nil {
				filters = append(filters, *filter)
			}
		}
		return true, nil
	}, expr)

	return filters
}

// extractComparisonFilter extracts a filter from a comparison expression
func (p *Planner) extractComparisonFilter(comp *sqlparser.ComparisonExpr) *Filter {
	// Extract field name
	var field string
	if colName, ok := comp.Left.(*sqlparser.ColName); ok {
		field = colName.Name.String()
	} else {
		return nil
	}

	// Extract value
	var value interface{}
	switch v := comp.Right.(type) {
	case *sqlparser.Literal:
		value = string(v.Val)
	case *sqlparser.ColName:
		// Column comparison - not yet supported
		return nil
	default:
		return nil
	}

	// Map operator
	var operator FilterOperator
	switch comp.Operator {
	case sqlparser.EqualOp:
		operator = FilterOpEqual
	case sqlparser.NotEqualOp:
		operator = FilterOpNotEqual
	case sqlparser.GreaterThanOp:
		operator = FilterOpGreater
	case sqlparser.LessThanOp:
		operator = FilterOpLess
	case sqlparser.GreaterEqualOp:
		operator = FilterOpGreaterEq
	case sqlparser.LessEqualOp:
		operator = FilterOpLessEq
	case sqlparser.LikeOp:
		operator = FilterOpContains
	case sqlparser.NotLikeOp:
		operator = FilterOpNotContains
	case sqlparser.RegexpOp:
		operator = FilterOpRegex
	default:
		return nil
	}

	return &Filter{
		Field:    field,
		Operator: operator,
		Value:    value,
	}
}

// pushDownFilters determines which filters can be pushed to the backend
func (p *Planner) pushDownFilters(filters []Filter, backend Backend) (pushDown, remaining []Filter) {
	pushDown = make([]Filter, 0)
	remaining = make([]Filter, 0)

	for _, filter := range filters {
		// Simple heuristic: push down all filters for now
		// In production, would check backend capabilities
		if p.canPushDownFilter(filter, backend) {
			pushDown = append(pushDown, filter)
		} else {
			remaining = append(remaining, filter)
		}
	}

	return pushDown, remaining
}

// canPushDownFilter checks if a filter can be pushed to the backend
func (p *Planner) canPushDownFilter(filter Filter, backend Backend) bool {
	// All backends support basic filtering for now
	// In production, would check specific backend capabilities
	return true
}

// hasAggregates checks if SELECT has aggregate functions
func (p *Planner) hasAggregates(selectExprs []sqlparser.SelectExpr) bool {
	hasAgg := false

	for _, expr := range selectExprs {
		//nolint:errcheck // Walk errors are not recoverable in this context
		sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node.(type) {
			case *sqlparser.FuncExpr, *sqlparser.CountStar, *sqlparser.Count,
				*sqlparser.Sum, *sqlparser.Avg, *sqlparser.Max, *sqlparser.Min:
				hasAgg = true
				return false, nil
			}
			return true, nil
		}, expr)
	}

	return hasAgg
}

// extractAggregates extracts aggregate functions from SELECT
func (p *Planner) extractAggregates(selectExprs []sqlparser.SelectExpr) []Aggregate {
	aggregates := make([]Aggregate, 0)

	for _, expr := range selectExprs {
		if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
			var agg *Aggregate
			alias := aliased.As.String()

			switch e := aliased.Expr.(type) {
			case *sqlparser.FuncExpr:
				agg = p.extractAggregate(e, alias)
			case *sqlparser.CountStar:
				if alias == "" {
					alias = "count_*"
				}
				agg = &Aggregate{
					Function: AggFuncCount,
					Field:    "*",
					Alias:    alias,
				}
			case *sqlparser.Count:
				field := "*"
				if len(e.Args) > 0 {
					field = p.extractExprField(e.Args[0])
				}
				if alias == "" {
					alias = fmt.Sprintf("count_%s", field)
				}
				agg = &Aggregate{
					Function: AggFuncCount,
					Field:    field,
					Alias:    alias,
				}
			case *sqlparser.Sum:
				field := p.extractExprField(e.Arg)
				if alias == "" {
					alias = fmt.Sprintf("sum_%s", field)
				}
				agg = &Aggregate{
					Function: AggFuncSum,
					Field:    field,
					Alias:    alias,
				}
			case *sqlparser.Avg:
				field := p.extractExprField(e.Arg)
				if alias == "" {
					alias = fmt.Sprintf("avg_%s", field)
				}
				agg = &Aggregate{
					Function: AggFuncAvg,
					Field:    field,
					Alias:    alias,
				}
			case *sqlparser.Max:
				field := p.extractExprField(e.Arg)
				if alias == "" {
					alias = fmt.Sprintf("max_%s", field)
				}
				agg = &Aggregate{
					Function: AggFuncMax,
					Field:    field,
					Alias:    alias,
				}
			case *sqlparser.Min:
				field := p.extractExprField(e.Arg)
				if alias == "" {
					alias = fmt.Sprintf("min_%s", field)
				}
				agg = &Aggregate{
					Function: AggFuncMin,
					Field:    field,
					Alias:    alias,
				}
			}

			if agg != nil {
				aggregates = append(aggregates, *agg)
			}
		}
	}

	return aggregates
}

// extractExprField extracts field name from an expression
func (p *Planner) extractExprField(expr sqlparser.Expr) string {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return e.Name.String()
	default:
		return "*"
	}
}

// extractAggregate extracts a single aggregate function
func (p *Planner) extractAggregate(funcExpr *sqlparser.FuncExpr, alias string) *Aggregate {
	funcName := strings.ToLower(funcExpr.Name.String())

	var aggFunc AggregateFunction
	switch funcName {
	case "count":
		aggFunc = AggFuncCount
	case "sum":
		aggFunc = AggFuncSum
	case "avg", "average":
		aggFunc = AggFuncAvg
	case "min":
		aggFunc = AggFuncMin
	case "max":
		aggFunc = AggFuncMax
	case "stddev":
		aggFunc = AggFuncStdDev
	case "rate":
		aggFunc = AggFuncRate
	case "p95", "percentile95":
		aggFunc = AggFuncP95
	case "p99", "percentile99":
		aggFunc = AggFuncP99
	default:
		return nil
	}

	// Extract field from function arguments
	var field string
	if len(funcExpr.Exprs) > 0 {
		// Check the type of the first expression
		switch expr := funcExpr.Exprs[0].(type) {
		case *sqlparser.ColName:
			field = expr.Name.String()
		default:
			// For count(*) and similar cases
			field = "*"
		}
	}

	if alias == "" {
		alias = fmt.Sprintf("%s_%s", funcName, field)
	}

	return &Aggregate{
		Function: aggFunc,
		Field:    field,
		Alias:    alias,
	}
}

// extractGroupBy extracts GROUP BY columns
func (p *Planner) extractGroupBy(groupBy []sqlparser.Expr) []string {
	columns := make([]string, 0)

	for _, expr := range groupBy {
		if colName, ok := expr.(*sqlparser.ColName); ok {
			columns = append(columns, colName.Name.String())
		}
	}

	return columns
}

// createJoinPlan creates a join plan for multiple tables
func (p *Planner) createJoinPlan(tables sqlparser.TableExprs, leftNode PlanNode) PlanNode {
	// Simplified join planning - would be more sophisticated in production
	if len(tables) < 2 {
		return leftNode
	}

	// For now, create a simple cross join
	// In production, would analyze join conditions
	for i := 1; i < len(tables); i++ {
		rightTable := p.extractTableName(tables[i])
		rightDataSource := p.mapTableToDataSource(rightTable)
		rightBackend := p.selectBackend(rightDataSource)

		rightNode := &ScanNode{
			DataSource: rightDataSource,
			Backend:    rightBackend,
			TimeRange:  TimeRange{}, // Would extract from query
		}

		leftNode = &JoinNode{
			Left:     leftNode,
			Right:    rightNode,
			JoinType: JoinTypeCross,
		}
	}

	return leftNode
}

func (p *Planner) extractTableName(tableExpr sqlparser.TableExpr) string {
	switch t := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		if name, ok := t.Expr.(sqlparser.TableName); ok {
			return name.Name.String()
		}
	}
	return ""
}

func (p *Planner) mapTableToDataSource(tableName string) DataSource {
	lower := strings.ToLower(tableName)
	switch {
	case strings.Contains(lower, "metric"):
		return DataSourceMetrics
	case strings.Contains(lower, "log"):
		return DataSourceLogs
	case strings.Contains(lower, "trace") || strings.Contains(lower, "span"):
		return DataSourceTraces
	default:
		return DataSourceMetrics
	}
}

// Optimizer optimizes logical plans
type Optimizer struct {
	rules []OptimizationRule
}

// OptimizationRule represents a plan optimization rule
type OptimizationRule interface {
	Apply(plan *LogicalPlan) (*LogicalPlan, bool)
}

// NewOptimizer creates a new query optimizer
func NewOptimizer() *Optimizer {
	return &Optimizer{
		rules: []OptimizationRule{
			&PredicatePushDown{},
			&ProjectionPushDown{},
			&JoinReordering{},
			&AggregationPushDown{},
		},
	}
}

// Optimize applies optimization rules to a logical plan
func (o *Optimizer) Optimize(plan *LogicalPlan) (*LogicalPlan, error) {
	optimized := plan
	changed := true
	iterations := 0
	maxIterations := 10

	// Apply rules until no more changes or max iterations reached
	for changed && iterations < maxIterations {
		changed = false
		iterations++

		for _, rule := range o.rules {
			newPlan, applied := rule.Apply(optimized)
			if applied {
				optimized = newPlan
				changed = true
			}
		}
	}

	return optimized, nil
}

// PredicatePushDown pushes filters closer to data source
type PredicatePushDown struct{}

func (r *PredicatePushDown) Apply(plan *LogicalPlan) (*LogicalPlan, bool) {
	// Walk the plan tree and push filters down
	// This is a simplified implementation
	return plan, false
}

// ProjectionPushDown pushes column selection closer to data source
type ProjectionPushDown struct{}

func (r *ProjectionPushDown) Apply(plan *LogicalPlan) (*LogicalPlan, bool) {
	// Push column projections down to reduce data transfer
	return plan, false
}

// JoinReordering reorders joins for better performance
type JoinReordering struct{}

func (r *JoinReordering) Apply(plan *LogicalPlan) (*LogicalPlan, bool) {
	// Reorder joins based on estimated cardinality
	return plan, false
}

// AggregationPushDown pushes aggregations to backends when possible
type AggregationPushDown struct{}

func (r *AggregationPushDown) Apply(plan *LogicalPlan) (*LogicalPlan, bool) {
	// Push aggregations to backends that support them
	return plan, false
}

// CreatePhysicalPlan converts a logical plan to a physical plan
func (p *Planner) CreatePhysicalPlan(logical *LogicalPlan) (*PhysicalPlan, error) {
	// Convert logical operators to physical operators
	// Add parallelism hints
	// Select specific algorithms (hash join vs nested loop, etc.)

	physical := &PhysicalPlan{
		Root:     logical.Root, // For now, use same nodes
		Parallel: p.shouldParallelize(logical),
	}

	// Determine primary backend
	if scanNode, ok := logical.Root.(*ScanNode); ok {
		physical.Backend = scanNode.Backend
	}

	return physical, nil
}

// shouldParallelize determines if query should be parallelized
func (p *Planner) shouldParallelize(plan *LogicalPlan) bool {
	// Check if plan has operations that benefit from parallelism
	cost := plan.Root.Cost()
	return cost > 1000 // Arbitrary threshold
}
