# btcostestimate

## Location
[src/backend/utils/adt/selfuncs.c:6854-7196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6854-L7196)

## Overview
A specialized cost estimation function for B-tree index access paths that provides accurate cost calculations considering B-tree specific optimizations like index ordering correlation and boundary qualification analysis.

## Definition

```c
void
btcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
			   Cost *indexStartupCost, Cost *indexTotalCost,
			   Selectivity *indexSelectivity, double *indexCorrelation,
			   double *indexPages)
```
## Detailed Description
The  function provides specialized cost estimation for B-tree index scans, building upon the generic cost estimation framework while adding B-tree specific optimizations and considerations.

Key features include:
- **Boundary Qualification Analysis**: Identifies which index qualifiers actually determine scan boundaries (leading equality clauses plus the first inequality clause) versus those that only provide heap filtering
- **Unique Index Optimization**: For unique indexes with complete equality qualifiers, assumes exactly one tuple will be found
- **ScalarArrayOpExpr Handling**: Estimates the number of index descents for array operations and applies intelligent clamping to avoid unrealistic estimates
- **Index Correlation Calculation**: Uses statistics from the first indexed column to estimate how well the index ordering matches the table's physical ordering
- **B-tree Descent Costing**: Adds CPU costs for traversing the B-tree from root to leaf, accounting for both comparison costs and page access costs

The function performs sophisticated analysis of index clauses to determine which contribute to selectivity versus which only provide filtering, enabling more accurate cost estimates than generic approaches.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and statistics
- : IndexPath structure describing the specific B-tree index access path being costed
- : Expected number of times this index scan will be executed (for nested loops)
- : Output parameter for one-time startup cost of the index scan
- : Output parameter for total cost including per-tuple processing
- : Output parameter for estimated fraction of table rows that will be returned
- : Output parameter for correlation between index and table ordering
- : Output parameter for estimated number of index pages to be accessed

## Dependencies
- Functions called/Symbols referenced:
  - [genericcostestimate](../g/genericcostestimate.md)
  - [add_predicate_to_index_quals](../a/add_predicate_to_index_quals.md)
  - [get_op_opfamily_strategy](../g/get_op_opfamily_strategy.md)
  - [estimate_array_length](../e/estimate_array_length.md)
  - [clauselist_selectivity](../c/clauselist_selectivity.md)
  - planner_rt_fetch
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [SearchSysCache3](../S/SearchSysCache3.md)
  - ReleaseVariableStats
- Called from (representative examples):
  - [bthandler](bthandler.md) (B-tree access method handler)

## Notes and Other Information
- Implements intelligent clamping of ScalarArrayOpExpr scan estimates to at most 1/3 of total index pages
- Charges logarithmic CPU cost for B-tree descent (log2(N) comparisons for N leaf tuples)
- Adds fixed CPU cost per page traversed during descent to account for bloated indexes
- Uses statistics correlation with adjustment factor (0.75) for multi-column indexes
- Handles both simple variables and expression indexes for correlation calculation
- Optimizes for the common case of unique indexes with complete equality conditions
- Distinguishes between boundary qualifiers (affecting selectivity) and filter qualifiers (affecting heap access only)