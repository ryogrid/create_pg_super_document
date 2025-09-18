# find_minmax_agg_replacement_param

## Location
src/backend/optimizer/plan/setrefs.c: 3439 - 3471

## Overview
Determines if a given aggregate function reference should be replaced with a parameter reference as part of min/max aggregate optimization, and returns the replacement parameter if applicable.

## Definition
```c
Param *
find_minmax_agg_replacement_param(PlannerInfo *root, Aggref *aggref)
```

## Detailed Description
This function is part of PostgreSQL's min/max aggregate optimization mechanism (implemented in planagg.c). When the planner determines that certain MIN/MAX aggregates can be optimized by using index scans to fetch the minimum or maximum value directly, rather than scanning all rows, it creates MinMaxAggInfo structures to track these optimizations.

The function checks if a given aggregate function reference (Aggref) matches one of the aggregates that has been marked for optimization. It compares the aggregate function OID and the target expression against the stored MinMaxAggInfo entries. If a match is found, it returns the parameter that should replace the original aggregate reference in the query plan.

This optimization is particularly effective for queries like "SELECT MIN(column) FROM table" or "SELECT MAX(column) FROM table" where an appropriate index exists, allowing the database to retrieve the result with a single index lookup instead of a full table scan.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the list of min/max aggregates marked for optimization in root->minmax_aggs
- `aggref`: The aggregate function reference to check for potential replacement

## Dependencies
- Functions called/Symbols referenced:
  - MinMaxAggInfo (structure type)
  - [equal](../e/equal.md) (comparison function)
  - linitial (list access function)
- Called from (representative examples):
  - [fix_scan_expr_mutator](fix_scan_expr_mutator.md)
  - [fix_upper_expr_mutator](fix_upper_expr_mutator.md)  
  - [finalize_primnode](finalize_primnode.md)

## Notes and Other Information
- This function is exported (not static) so that SS_finalize_plan can use it before setrefs.c runs
- The function only considers single-argument aggregates (list_length(aggref->args) == 1)
- Returns NULL if no matching optimization is found or if root->minmax_aggs is empty
- The MinMaxAggInfo list is populated only when a Plan is built from a MinMaxAggPath
- Critical for the min/max aggregate optimization that can significantly improve query performance for appropriate queries
- Part of the broader query optimization framework that transforms high-level SQL constructs into efficient execution plans