# create_limit_plan

## Location
src/backend/optimizer/plan/createplan.c: 2856 - 2916

## Overview
Creates a Limit plan node for implementing LIMIT and OFFSET clauses, including support for WITH TIES functionality that requires additional sorting information.

## Definition


## Detailed Description
This function constructs a Limit plan node that implements SQL's LIMIT and OFFSET functionality for restricting the number of rows returned by a query. The function handles both simple LIMIT operations and the more complex LIMIT WITH TIES variant. For WITH TIES operations, it extracts sorting information from the query's ORDER BY clause to determine which rows are considered "tied" with the last row that would normally be returned by the LIMIT. This requires building arrays of column indices, equality operators, and collations to properly compare rows during execution.

The function allocates memory for the uniqueness comparison arrays only when needed (WITH TIES case) and properly configures the Limit node with all necessary parameters for execution.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context, including the parse tree with sort clause information
- : LimitPath representing the chosen execution strategy for the limit operation, containing offset, count, and limit option parameters
- : Integer flags controlling plan creation behavior, passed through unchanged to the subplan

## Dependencies
- Functions called/Symbols referenced:
  - create_plan_recurse
  - get_sortgroupclause_tle
  - exprCollation
  - make_limit
  - copy_generic_path_info
  - LIMIT_OPTION_WITH_TIES (constant)
- Called from (representative examples):
  - create_plan_recurse

## Notes and Other Information
- The function is static, indicating it's only used within the createplan.c module
- Limit operations don't project new columns, so target list requirements pass through unchanged
- Special handling for LIMIT WITH TIES requires:
  - Extracting sort clause information from the parse tree
  - Building arrays of column indices (uniqColIdx), equality operators (uniqOperators), and collations (uniqCollations)
  - These arrays enable the executor to determine which rows are "tied" with the boundary row
- Memory allocation using palloc for the uniqueness arrays occurs only when WITH TIES is specified
- Essential for implementing SQL standard LIMIT/OFFSET functionality
- The WITH TIES feature allows returning additional rows that have the same sort key values as the last row that would be included by a plain LIMIT