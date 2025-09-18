# cost_samplescan

## Location
[src/backend/optimizer/path/costsize.c:361-435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L361-L435)

## Overview
Determines and calculates the cost of scanning a relation using table sampling methods (TABLESAMPLE clause).

## Definition
```c
void cost_samplescan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info)
```

## Detailed Description
This function calculates the cost estimation for table sampling scans, which are used when a TABLESAMPLE clause is specified in a query. Table sampling allows retrieving a subset of rows from a table using various sampling algorithms (like SYSTEM or BERNOULLI).

The costing model adapts based on the specific sampling method being used:

1. **Page access pattern**: Determines whether to use random or sequential page costs based on whether the sampling method's NextSampleBlock function exists
2. **Disk I/O costs**: Uses the appropriate page cost (random vs. sequential) multiplied by the estimated pages to visit
3. **CPU processing costs**: Based on the estimated number of tuples that will be sampled, not the total tuples in the table
4. **Sampling method overhead**: Ignores the internal calculations of sampling methods and parameter expression evaluation costs
5. **Qualification costs**: Standard WHERE clause evaluation costs
6. **Target list evaluation**: Costs for computing output expressions per result row

The function assumes that baserel->pages and baserel->tuples have already been adjusted by the sampling method to reflect the expected sampling output, not the full table size.

## Parameters / Member Variables
- `path`: The Path node to store the computed costs and row estimates
- `root`: PlannerInfo containing global planning context and planner tree
- `baserel`: RelOptInfo for the relation being sampled, with pre-adjusted statistics
- `param_info`: ParamPathInfo for parameterized paths, NULL for regular paths

## Dependencies
- Functions called/Symbols referenced:
  - `planner_rt_fetch()`: Retrieves range table entry for the relation
  - [GetTsmRoutine](../G/GetTsmRoutine.md)(): Gets the table sampling method routine
  - [get_tablespace_page_costs](../g/get_tablespace_page_costs.md)(): Gets tablespace-specific page access costs
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)(): Calculates WHERE clause evaluation costs
  - [TableSampleClause](../T/TableSampleClause.md), `TsmRoutine`: Types for table sampling infrastructure
  - `RTE_RELATION`: Constant for relation table entry type

- Called from (representative examples):
  - [create_samplescan_path](create_samplescan_path.md)(): Creates table sample scan path nodes

## Notes and Other Information
- Located in src/backend/optimizer/path/costsize.c:361-435
- Only applicable to base relations with TABLESAMPLE clauses  
- Chooses between random and sequential page costs based on sampling method behavior
- Assumes baserel statistics are pre-adjusted by sampling method for estimated sample size
- TABLESAMPLE parameter expressions are evaluated once per scan, so their cost is ignored
- Does not charge for internal sampling method computations
- Target list costs apply per output row, not per sampled tuple
- Part of PostgreSQL's statistical sampling functionality for large table analysis