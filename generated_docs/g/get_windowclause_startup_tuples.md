# get_windowclause_startup_tuples

## Location
[src/backend/optimizer/path/costsize.c:2854-3067](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L2854-L3067)

## Overview
Estimates how many tuples a WindowAgg node needs to fetch from its subnode before it can output the first tuple, based on the window clause specifications including partitioning, ordering, and frame options.

## Definition
static double get_windowclause_startup_tuples(PlannerInfo *root, WindowClause *wc, double input_tuples)

## Detailed Description
The get_windowclause_startup_tuples function analyzes a WindowClause to determine how many input tuples must be read before a WindowAgg node can produce its first output tuple. This depends heavily on the window specification:

- **No PARTITION BY, no ORDER BY**: All input tuples must be read and aggregated before any output
- **With PARTITION BY**: Only tuples from the first partition need to be considered
- **Frame specifications**: Different frame options (ROWS, RANGE, GROUPS) with various bounds (UNBOUNDED, CURRENT ROW, PRECEDING, FOLLOWING) affect how many tuples are needed

The function performs a multi-step analysis:
1. Estimates partition size by calculating the number of partitions using estimate_num_groups
2. Estimates peer group size within partitions based on ORDER BY expressions
3. Analyzes frame options to determine the specific number of tuples required
4. Handles various frame ending conditions including offset calculations for numeric constants

For OFFSET FOLLOWING frames, the function attempts to extract exact values from Const nodes (INT2, INT4, INT8) or falls back to selectivity-based estimates using DEFAULT_INEQ_SEL when the offset is not a constant.

## Parameters / Member Variables
- : PlannerInfo structure containing parse tree and planner context
-       0       0       0: WindowClause containing partition, order, and frame specifications
- : Total number of input tuples from the subnode

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgrouplist_exprs](get_sortgrouplist_exprs.md) (extracts expressions from sort/group lists)
  - [estimate_num_groups](../e/estimate_num_groups.md) (estimates distinct groups in expressions)
  - [list_free](../l/list_free.md) (memory management for expression lists)
  - [clamp_row_est](../c/clamp_row_est.md) (ensures row estimates are within reasonable bounds)
  - [DatumGetInt16](../D/DatumGetInt16.md), DatumGetInt32, DatumGetInt64 (extract values from Datum)
  - Various FRAMEOPTION constants (END_UNBOUNDED_FOLLOWING, END_CURRENT_ROW, etc.)
  - DEFAULT_INEQ_SEL (default selectivity for inequality conditions)
- Called from (representative examples):
  - [cost_windowagg](../c/cost_windowagg.md) (in costsize.c:3146)

## Notes and Other Information
- Function is static, indicating internal use within costsize.c only
- Adds +1 tuple when partitioning/ordering is present to account for WindowAgg needing to read ahead to confirm partition/group boundaries
- EXCLUDE options in window frames don't affect tuple reading count, only aggregation
- Handles unsupported frame options gracefully with assertions and fallback to 1.0
- For NULL constants in OFFSET clauses, assumes only first row/range/group is needed
- Return value is capped to never exceed the estimated partition size
- Uses DEFAULT_INEQ_SEL heuristic when offset values cannot be determined from non-constant expressions
- Considers peer groups (tuples with identical ORDER BY values) for RANGE and GROUPS frame modes