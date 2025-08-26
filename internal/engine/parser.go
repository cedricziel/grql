package engine

import (
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"
)

type Parser struct {
	parser *sqlparser.Parser
}

func NewParser() *Parser {
	return &Parser{
		parser: sqlparser.NewTestParser(),
	}
}

type Query struct {
	Raw        string
	Statement  sqlparser.Statement
	DataSource DataSource
	TimeRange  TimeRange
	Facets     []string
	Limit      int
}

type DataSource string

const (
	DataSourceMetrics DataSource = "metrics"
	DataSourceLogs    DataSource = "logs"
	DataSourceTraces  DataSource = "traces"
)

func (p *Parser) Parse(query string) (*Query, error) {
	stmt, err := p.parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	q := &Query{
		Raw:       query,
		Statement: stmt,
	}

	if err := p.extractQueryInfo(q); err != nil {
		return nil, err
	}

	return q, nil
}

func (p *Parser) extractQueryInfo(q *Query) error {
	switch stmt := q.Statement.(type) {
	case *sqlparser.Select:
		return p.extractSelectInfo(q, stmt)
	default:
		return fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (p *Parser) extractSelectInfo(q *Query, stmt *sqlparser.Select) error {
	// Extract data source from FROM clause
	if len(stmt.From) > 0 {
		tableName := p.extractTableName(stmt.From[0])
		q.DataSource = p.mapTableToDataSource(tableName)
	}

	// Extract time range from WHERE clause (NRQL-style SINCE/UNTIL)
	if stmt.Where != nil {
		p.extractTimeRange(q, stmt.Where.Expr)
	} else {
		// Set default time range if no WHERE clause
		q.TimeRange = TimeRange{
			Since: time.Now().Add(-1 * time.Hour),
			Until: time.Now(),
		}
	}

	// Extract FACET/GROUP BY clauses
	if stmt.GroupBy != nil {
		q.Facets = p.extractFacets(*stmt.GroupBy)
	}

	// Extract LIMIT
	if stmt.Limit != nil {
		if count, ok := stmt.Limit.Rowcount.(*sqlparser.Literal); ok {
			fmt.Sscanf(string(count.Val), "%d", &q.Limit)
		}
	}

	return nil
}

func (p *Parser) extractTableName(tableExpr sqlparser.TableExpr) string {
	switch t := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		if name, ok := t.Expr.(sqlparser.TableName); ok {
			return name.Name.String()
		}
	}
	return ""
}

func (p *Parser) mapTableToDataSource(tableName string) DataSource {
	lower := strings.ToLower(tableName)
	switch {
	case strings.Contains(lower, "metric"):
		return DataSourceMetrics
	case strings.Contains(lower, "log"):
		return DataSourceLogs
	case strings.Contains(lower, "trace") || strings.Contains(lower, "span"):
		return DataSourceTraces
	default:
		// Default based on common patterns
		if strings.Contains(lower, "transaction") || strings.Contains(lower, "event") {
			return DataSourceMetrics
		}
		return DataSourceLogs
	}
}

func (p *Parser) extractTimeRange(q *Query, expr sqlparser.Expr) {
	// Default to last hour if no time range specified
	if q.TimeRange.Since.IsZero() {
		q.TimeRange = TimeRange{
			Since: time.Now().Add(-1 * time.Hour),
			Until: time.Now(),
		}
	}

	// Walk the expression tree looking for time-related conditions
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if comp, ok := node.(*sqlparser.ComparisonExpr); ok {
			p.extractTimeComparison(q, comp)
		}
		return true, nil
	}, expr)
}

func (p *Parser) extractTimeComparison(q *Query, comp *sqlparser.ComparisonExpr) {
	// Look for timestamp/time comparisons
	if colName, ok := comp.Left.(*sqlparser.ColName); ok {
		name := strings.ToLower(colName.Name.String())
		if strings.Contains(name, "time") || strings.Contains(name, "timestamp") {
			// Extract time value from right side
			if literal, ok := comp.Right.(*sqlparser.Literal); ok {
				// Parse the time value (simplified)
				_ = literal // Would parse this in production
			}
		}
	}
}

func (p *Parser) extractFacets(groupBy sqlparser.GroupBy) []string {
	facets := make([]string, 0)
	for _, expr := range groupBy.Exprs {
		if colName, ok := expr.(*sqlparser.ColName); ok {
			facets = append(facets, colName.Name.String())
		}
	}
	return facets
}

// ExtendedParser adds NRQL-specific parsing capabilities
type ExtendedParser struct {
	*Parser
}

func NewExtendedParser() *ExtendedParser {
	return &ExtendedParser{
		Parser: NewParser(),
	}
}

func (ep *ExtendedParser) ParseNRQL(query string) (*Query, error) {
	// Pre-process NRQL-specific syntax before standard SQL parsing
	processed := ep.preprocessNRQL(query)
	return ep.Parse(processed)
}

func (ep *ExtendedParser) preprocessNRQL(query string) string {
	// Convert NRQL-specific keywords to SQL equivalents
	result := query

	// Convert FACET to GROUP BY
	result = strings.ReplaceAll(result, " FACET ", " GROUP BY ")

	// Remove SINCE/UNTIL clauses for now (they're handled separately)
	// In production, this would be more sophisticated
	upperQuery := strings.ToUpper(result)
	if idx := strings.Index(upperQuery, " SINCE "); idx > 0 {
		result = result[:idx]
	}

	upperQuery = strings.ToUpper(result)
	if idx := strings.Index(upperQuery, " UNTIL "); idx > 0 {
		result = result[:idx]
	}

	if strings.Contains(strings.ToUpper(result), " TIMESERIES ") {
		// Would handle TIMESERIES conversion
	}

	return result
}
