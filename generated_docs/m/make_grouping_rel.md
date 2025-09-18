# make_grouping_rel

## Location
src/backend/optimizer/plan/planner.c: 3933 - 3985

## Overview
Creates a new grouping relation and sets its basic properties for the GROUP BY/aggregation planning phase.

## Definition


## Detailed Description
This function creates and initializes a new RelOptInfo structure specifically for representing grouped/aggregated results. It handles the setup of a new upper relation that will contain paths for performing GROUP BY operations and aggregation functions.

The function determines whether to create a regular upper relation or a specialized "other upper relation" based on the input relation type. It preserves important properties from the input relation including parallel execution capabilities, foreign data wrapper information, and server/user context.

The parallel safety evaluation combines multiple factors:
- Input relation must support parallel execution
- Target expressions must be parallel-safe
- HAVING clause must be parallel-safe

The function also maintains Foreign Data Wrapper (FDW) context by copying server ID, user ID, and FDW routine information from the input relation.

## Parameters / Member Variables
- : PlannerInfo containing the query planning context and configuration
- : RelOptInfo representing the underlying scan/join relation that provides input data
- : PathTarget specifying the output columns and expressions for the grouping relation
- : Boolean indicating whether the target list can be computed safely in parallel
- : Node representing the HAVING clause conditions (can be NULL if no HAVING clause)

## Dependencies
- Functions called/Symbols referenced:
  - fetch_upper_rel
  - IS_OTHER_REL (macro)
  - [is_parallel_safe](../i/is_parallel_safe.md)
- Constants used:
  - UPPERREL_GROUP_AGG
  - RELOPT_OTHER_UPPER_REL
- Called from:
  - standard_qp_extra
  - [create_grouping_paths](../c/create_grouping_paths.md)
  - [create_partitionwise_grouping_paths](../c/create_partitionwise_grouping_paths.md)

## Notes and Other Information
- By tradition, the main grouping relation uses NULL for relids, though this could theoretically be changed
- The function preserves FDW context, enabling push-down of grouping operations to foreign servers when possible
- Parallel safety is determined conservatively - all components must be parallel-safe for the grouped relation to support parallel execution
- This is a foundational function in the grouping planning process, setting up the relation that will hold various grouping execution strategies