# make_partial_grouping_target

## Location
src/backend/optimizer/plan/planner.c: 5609 - 5711

## Overview
Generates the appropriate PathTarget for output of partial aggregate nodes by including grouping columns as-is and converting aggregate functions to partial aggregates with AGGSPLIT_INITIAL_SERIAL mode.

## Definition


## Detailed Description
This function creates the target list for partial aggregation nodes in parallel query execution. Partial aggregation is a key optimization technique where aggregation is split into multiple phases - partial aggregation on each worker followed by final aggregation to combine results.

The function handles several critical aspects of partial aggregation:
- Preserves all grouping columns exactly as they appear to enable upper-level grouping
- Converts regular Aggref nodes to partial aggregates marked with AGGSPLIT_INITIAL_SERIAL
- Includes variables and PlaceHolderVars used outside of aggregates in both target list and HAVING clause
- Extracts all aggregates used in HAVING clauses even if not in the main target list
- Ensures comprehensive coverage of variables needed for ORDER BY and window specifications

The transformation is essential for parallel aggregation because partial aggregates produce intermediate results that must be combined by a final aggregation step, rather than producing final aggregate values directly.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and processed grouping information
- : PathTarget representing the tlist to be emitted by the topmost aggregation step
- : Node representing the HAVING clause which may contain additional aggregates and variables

## Dependencies
- Functions called/Symbols referenced:
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md)
  - get_pathtarget_sortgroupref
  - [get_sortgroupref_clause_noerr](../g/get_sortgroupref_clause_noerr.md)
  - [add_column_to_pathtarget](../a/add_column_to_pathtarget.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - [add_new_columns_to_pathtarget](../a/add_new_columns_to_pathtarget.md)
  - [mark_partial_aggref](mark_partial_aggref.md)
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md)
- Called from:
  - [create_partial_grouping_paths](../c/create_partial_grouping_paths.md)

## Notes and Other Information
- Uses PVC_INCLUDE_AGGREGATES, PVC_RECURSE_WINDOWFUNCS, and PVC_INCLUDE_PLACEHOLDERS flags for comprehensive expression extraction
- All Aggrefs are converted to partial mode using AGGSPLIT_INITIAL_SERIAL, assuming serialization is required
- Maintains sortgroupref values for grouping columns to preserve their identity across aggregation phases
- Performs flat copying of Aggref nodes to avoid damaging other expression trees
- Essential for two-phase and multi-phase aggregation strategies in parallel query execution
- Results in some redundant cost calculation as noted in the code comment
- Works in conjunction with final aggregation steps that combine partial results into final aggregate values