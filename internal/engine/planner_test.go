package engine

import (
	"testing"
)

func TestPlanner_CreateLogicalPlan(t *testing.T) {
	parser := NewParser()
	planner := NewPlanner()

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "simple scan",
			query:   "SELECT * FROM metrics",
			wantErr: false,
		},
		{
			name:    "scan with filter",
			query:   "SELECT cpu FROM metrics WHERE service = 'api'",
			wantErr: false,
		},
		{
			name:    "aggregation query",
			query:   "SELECT avg(cpu), max(memory) FROM metrics GROUP BY instance",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			plan, err := planner.CreateLogicalPlan(q)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateLogicalPlan() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && plan == nil {
				t.Error("Expected plan to be non-nil")
			}

			if !tt.wantErr && plan.Root == nil {
				t.Error("Expected plan root to be non-nil")
			}
		})
	}
}

func TestPlanner_SelectBackend(t *testing.T) {
	planner := NewPlanner()

	tests := []struct {
		dataSource DataSource
		want       Backend
	}{
		{
			dataSource: DataSourceMetrics,
			want:       BackendMimir,
		},
		{
			dataSource: DataSourceLogs,
			want:       BackendLoki,
		},
		{
			dataSource: DataSourceTraces,
			want:       BackendTempo,
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.dataSource), func(t *testing.T) {
			backend := planner.selectBackend(tt.dataSource)
			if backend != tt.want {
				t.Errorf("selectBackend() = %v, want %v", backend, tt.want)
			}
		})
	}
}

func TestPlanner_PlanStructure(t *testing.T) {
	parser := NewParser()
	planner := NewPlanner()

	// Test scan node creation
	q, _ := parser.Parse("SELECT * FROM metrics")
	plan, _ := planner.CreateLogicalPlan(q)

	if _, ok := plan.Root.(*ScanNode); !ok {
		t.Errorf("Expected root to be ScanNode, got %T", plan.Root)
	}

	// Test aggregation node creation
	q, _ = parser.Parse("SELECT count(*) FROM logs GROUP BY level")
	plan, _ = planner.CreateLogicalPlan(q)

	if _, ok := plan.Root.(*AggregateNode); !ok {
		t.Errorf("Expected root to be AggregateNode, got %T", plan.Root)
	}
}

func TestOptimizer_Optimize(t *testing.T) {
	parser := NewParser()
	planner := NewPlanner()
	optimizer := NewOptimizer()

	q, err := parser.Parse("SELECT * FROM metrics WHERE service = 'api'")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	logicalPlan, err := planner.CreateLogicalPlan(q)
	if err != nil {
		t.Fatalf("CreateLogicalPlan() error = %v", err)
	}

	optimizedPlan, err := optimizer.Optimize(logicalPlan)
	if err != nil {
		t.Fatalf("Optimize() error = %v", err)
	}

	if optimizedPlan == nil {
		t.Fatal("Expected optimized plan to be non-nil")
	}

	if optimizedPlan.Root == nil {
		t.Error("Expected optimized plan root to be non-nil")
	}
}

func TestPlanner_CreatePhysicalPlan(t *testing.T) {
	parser := NewParser()
	planner := NewPlanner()

	q, err := parser.Parse("SELECT * FROM metrics")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	logicalPlan, err := planner.CreateLogicalPlan(q)
	if err != nil {
		t.Fatalf("CreateLogicalPlan() error = %v", err)
	}

	physicalPlan, err := planner.CreatePhysicalPlan(logicalPlan)
	if err != nil {
		t.Fatalf("CreatePhysicalPlan() error = %v", err)
	}

	if physicalPlan == nil {
		t.Fatal("Expected physical plan to be non-nil")
	}

	if physicalPlan.Root == nil {
		t.Error("Expected physical plan root to be non-nil")
	}

	// Check backend is set
	if physicalPlan.Backend == "" {
		t.Error("Expected backend to be set in physical plan")
	}
}

func TestPlanner_ExtractAggregates(t *testing.T) {
	parser := NewParser()
	planner := NewPlanner()

	tests := []struct {
		wantFuncs []AggregateFunction
		query     string
		wantCount int
	}{
		{
			query:     "SELECT count(*) FROM metrics",
			wantCount: 1,
			wantFuncs: []AggregateFunction{AggFuncCount},
		},
		{
			query:     "SELECT avg(cpu), max(memory) FROM metrics",
			wantCount: 2,
			wantFuncs: []AggregateFunction{AggFuncAvg, AggFuncMax},
		},
		{
			query:     "SELECT cpu FROM metrics",
			wantCount: 0,
			wantFuncs: []AggregateFunction{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			q, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			plan, err := planner.CreateLogicalPlan(q)
			if err != nil {
				t.Fatalf("CreateLogicalPlan() error = %v", err)
			}

			// Check if aggregation node was created when expected
			if tt.wantCount > 0 {
				aggNode, ok := plan.Root.(*AggregateNode)
				if !ok {
					t.Errorf("Expected AggregateNode for query with aggregates")
				} else if len(aggNode.Aggregates) != tt.wantCount {
					t.Errorf("Expected %d aggregates, got %d", tt.wantCount, len(aggNode.Aggregates))
				}
			}
		})
	}
}
