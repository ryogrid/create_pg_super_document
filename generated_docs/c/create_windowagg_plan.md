# create_windowagg_plan

## Location
src/backend/optimizer/plan/createplan.c: 2617 - 2719

## Overview
Creates a WindowAgg plan node for WindowAggPath operations that implement SQL window functions like ROW_NUMBER(), RANK(), and aggregate functions with OVER clauses.

## Definition
```c
static WindowAgg *create_windowagg_plan(PlannerInfo *root, WindowAggPath *best_path)
```

## Detailed Description
The `create_windowagg_plan` function constructs a WindowAgg plan node that implements SQL window functions. It processes the window specification (PARTITION BY and ORDER BY clauses) from the WindowClause, converts SortGroupClause lists into arrays of column indexes, equality operators, and collations needed by the executor. The function creates arrays for both partition columns (which define window boundaries) and ordering columns (which determine row sequence within partitions). It uses CP_SMALL_TLIST flag when creating the subplan to minimize memory usage since WindowAgg stores input rows in a tuplestore for window frame processing.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: WindowAggPath structure containing the window clause, run conditions, qualifications, and subpath information

## Dependencies
- Functions called/Symbols referenced:
  - create_plan_recurse
  - build_path_tlist
  - get_sortgroupclause_tle
  - exprCollation
  - make_windowagg
  - copy_generic_path_info
- Called from (representative examples):
  - create_plan_recurse

## Notes and Other Information
- The function is static, used only within createplan.c
- Uses CP_SMALL_TLIST flag to request a compact target list since WindowAgg stores input rows in tuplestores
- Converts PARTITION BY and ORDER BY clauses into executor-friendly arrays of column indexes, operators, and collations
- Handles window frame specifications including frame options, start/end offsets, and range functions
- Supports complex window frame definitions with ROWS, RANGE, and GROUPS modes
- The created plan includes run conditions for optimized window function execution and qualifications for filtering
- Essential for implementing SQL:2003 window function standards including analytical functions and frame-aware aggregates