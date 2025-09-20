# set_function_size_estimates

## Location
[src/backend/optimizer/path/costsize.c:5875-5912](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5875-L5912)

## Overview
Sets the size estimates for a base relation that represents a function call, calculating the expected number of rows the function will return.

## Definition

```c
void
set_function_size_estimates(PlannerInfo *root, RelOptInfo *rel)
```
## Detailed Description
This function estimates the cardinality (number of rows) for a relation that represents a function call in the FROM clause of a query. It handles functions that return sets (table-valued functions) by analyzing each function in the RTE_FUNCTION range table entry and determining the maximum row count among all functions. The function operates specifically on base relations where the range table entry kind is RTE_FUNCTION.

The estimation process involves iterating through all functions listed in the range table entry, calling  to estimate each function's row output, and selecting the maximum count as the relation's tuple estimate. After establishing the base row count, it delegates to  to complete the size estimation process including selectivity calculations.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and global information
- : RelOptInfo structure representing the relation being sized, must be a base relation with relid > 0

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - RTE_FUNCTION  
  - [RangeTblFunction](../R/RangeTblFunction.md)
  - [expression_returns_set_rows](../e/expression_returns_set_rows.md)
  - [set_baserel_size_estimates](set_baserel_size_estimates.md)
- Called from (representative examples):
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- Should only be applied to base relations that are functions (RTE_FUNCTION type)
- The function sets rel->tuples to the maximum row estimate among all functions in the RTE
- Uses assertions to verify the relation is a valid base relation with function range table entry
- Part of PostgreSQL's cost-based query optimizer infrastructure for estimating execution costs