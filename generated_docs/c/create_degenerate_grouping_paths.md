# create_degenerate_grouping_paths

## Location
src/backend/optimizer/plan/planner.c: 4007 - 4070

## Overview
Creates execution paths for degenerate grouping cases where no actual grouping computation is needed and the FROM table can be discarded.

## Definition


## Detailed Description
This function handles the special optimization case of degenerate grouping, where the query has grouping sets or HAVING clauses but no actual variables that require data from the base tables. In such cases, PostgreSQL can completely bypass the input relation and generate results using a Result node.

The function implements a key optimization: since there are no variables in either the HAVING clause or target list that reference the FROM table, the entire scan/join plan can be discarded. Instead, it creates paths using Result nodes that can determine the output purely based on constants and expressions.

The function handles two scenarios:
1. **Multiple grouping sets**: Creates multiple Result node paths (one per grouping set) and combines them using an Append node. Each path may or may not produce output depending on whether the HAVING clause succeeds.
2. **Single or no grouping sets**: Creates a single Result node path.

This optimization is particularly valuable because it can turn potentially expensive table scans into trivial constant-time operations.

## Parameters / Member Variables
- : PlannerInfo containing query planning context and configuration
- : RelOptInfo representing the input relation (which will be discarded in this optimization)
- : RelOptInfo representing the target grouping relation where the new paths will be added

## Dependencies
- Functions called/Symbols referenced:
  - [create_group_result_path](create_group_result_path.md)
  - [create_append_path](create_append_path.md)
  - [add_path](../a/add_path.md)
  - list_length
  - lappend
- Called from:
  - standard_qp_extra
  - [create_grouping_paths](create_grouping_paths.md)

## Notes and Other Information
- This function represents a significant query optimization that can eliminate table access entirely
- The comment notes that generating the earlier (unnecessary) paths is acceptable because this is a sufficiently rare corner case
- With volatile HAVING clauses and multiple grouping sets, the output can range from 0 to N rows, which is the intended behavior
- The function demonstrates PostgreSQL's approach to aggressive optimization in special cases
- This optimization is only safe because degenerate grouping guarantees no table variables are referenced
- The use of Result nodes makes this one of the most efficient possible query execution strategies