# cost_seqscan

## Location
[src/backend/optimizer/path/costsize.c:284-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L284-L360)

## Overview
Determines and calculates the cost of scanning a relation sequentially, including startup costs, CPU costs, and disk I/O costs.

## Definition
```c
void cost_seqscan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info)
```

## Detailed Description
This function calculates the complete cost estimation for sequential table scans, which is fundamental to PostgreSQL's cost-based query optimizer. The costing model considers multiple factors:

1. **Disk I/O costs**: Based on sequential page access cost and the number of pages in the relation
2. **CPU processing costs**: Includes tuple processing cost and qualification evaluation cost
3. **Target list evaluation**: Costs for computing the output expressions
4. **Parallelism adjustments**: When parallel workers are used, costs are adjusted accordingly
5. **Restriction qualifications**: WHERE clause evaluation costs
6. **Startup costs**: One-time initialization costs

The function also handles parameterized paths (used in nested loops) by using different row estimates. For parallel plans, it adjusts both CPU costs (divided among workers) and row estimates (per worker), while keeping disk costs unchanged due to limited I/O parallelization benefits in most operating systems.

## Parameters / Member Variables
- `path`: The Path node to store the computed costs and row estimates
- `root`: PlannerInfo containing global planning context and configuration
- `baserel`: RelOptInfo for the relation being scanned, containing statistics and metadata
- `param_info`: ParamPathInfo for parameterized paths, NULL for regular paths

## Dependencies
- Functions called/Symbols referenced:
  - [get_tablespace_page_costs](../g/get_tablespace_page_costs.md)(): Gets tablespace-specific page access costs
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)(): Calculates WHERE clause evaluation costs
  - [get_parallel_divisor](../g/get_parallel_divisor.md)(): Computes parallelism adjustment factor
  - [clamp_row_est](clamp_row_est.md)(): Ensures row estimates are within valid ranges
  - `RTE_RELATION`: Constant indicating relation table entry type
  - [ParamPathInfo](../P/ParamPathInfo.md), `Cost`, `QualCost`: Type definitions for cost structures

- Called from (representative examples):
  - [create_seqscan_path](create_seqscan_path.md)(): Creates sequential scan path nodes

## Notes and Other Information
- Located in src/backend/optimizer/path/costsize.c:284-360
- Only applicable to base relations (not joins or subqueries)
- Honors enable_seqscan configuration by adding disable_cost when disabled
- CPU costs are divided among parallel workers, but disk costs are not
- Disk cost assumes sequential I/O pattern with tablespace-specific page costs  
- Target list evaluation costs are applied per output row, not per scanned tuple
- Critical component of PostgreSQL's cost-based optimization for table access methods
- Row estimates are adjusted for parameterized paths and parallel execution

## Simplified Source

```c
void cost_seqscan(Path *path, PlannerInfo *root,
                  RelOptInfo *baserel, ParamPathInfo *param_info)
{
    Cost startup_cost = 0;
    Cost cpu_run_cost;
    Cost disk_run_cost;
    double spc_seq_page_cost;
    QualCost qpqual_cost;
    Cost cpu_per_tuple;

    // Set row estimate based on parameterization
    if (param_info)
        path->rows = param_info->ppi_rows;
    else
        path->rows = baserel->rows;

    // Add penalty cost if sequential scans are disabled
    if (!enable_seqscan)
        startup_cost += disable_cost;

    // Get tablespace-specific page cost
    get_tablespace_page_costs(baserel->reltablespace, NULL, &spc_seq_page_cost);

    // Calculate disk I/O costs (pages * cost per page)
    disk_run_cost = spc_seq_page_cost * baserel->pages;

    // Calculate CPU costs for tuple processing and WHERE clause evaluation
    get_restriction_qual_cost(root, baserel, param_info, &qpqual_cost);
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    cpu_run_cost = cpu_per_tuple * baserel->tuples;

    // Add target list evaluation costs (per output row)
    startup_cost += path->pathtarget->cost.startup;
    cpu_run_cost += path->pathtarget->cost.per_tuple * path->rows;

    // Adjust costs for parallel execution
    if (path->parallel_workers > 0) {
        double parallel_divisor = get_parallel_divisor(path);

        // CPU cost is divided among workers
        cpu_run_cost /= parallel_divisor;

        // Adjust row count per worker
        path->rows = clamp_row_est(path->rows / parallel_divisor);
    }

    // Set final costs
    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + cpu_run_cost + disk_run_cost;
}
```