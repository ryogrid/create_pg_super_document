# make_group_input_target

## Location
[src/backend/optimizer/plan/planner.c:5521-5608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L5521-L5608)

## Overview
Generates the appropriate PathTarget for initial input to grouping nodes by including all grouping columns as-is and extracting variables from non-grouping expressions including HAVING clauses.

## Definition

```c
static PathTarget *
make_group_input_target(PlannerInfo *root, PathTarget *final_target)
```
## Detailed Description
This function creates the correct target list for the scan/join subplan when there is grouping or aggregation in the query. The subplan cannot emit the query's final targetlist directly because it may contain aggregate function calls and other expressions that must be computed by upper plan nodes.

The function implements a sophisticated target list transformation:
- Preserves GROUP BY expressions exactly as they appear (with sortgroupref intact)
- Extracts individual variables from non-grouping expressions rather than computing the full expressions
- Includes variables from HAVING clauses which may not appear in the target list
- Handles variables within aggregate functions and window functions
- Covers requirements for ORDER BY and window specifications through resjunk items

For example, given , the function generates the subplan target:  where  will be used by Sort/Group steps and  will be used for computing the final aggregated results.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and processed grouping information
- : PathTarget representing the query's final target list that needs to be transformed for subplan use

## Dependencies
- Functions called/Symbols referenced:
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md)
  - get_pathtarget_sortgroupref
  - [get_sortgroupref_clause_noerr](../g/get_sortgroupref_clause_noerr.md)
  - [add_column_to_pathtarget](../a/add_column_to_pathtarget.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - [add_new_columns_to_pathtarget](../a/add_new_columns_to_pathtarget.md)
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md)
- Called from:
  - [grouping_planner](../g/grouping_planner.md)

## Notes and Other Information
- The parser-generated target list already contains ORDER BY and GROUP BY expressions but lacks HAVING variables
- Uses PVC_RECURSE_AGGREGATES, PVC_RECURSE_WINDOWFUNCS, and PVC_INCLUDE_PLACEHOLDERS flags to ensure comprehensive variable extraction
- Maintains sortgroupref values for grouping columns to preserve their identity for later grouping operations
- The function results in some redundant cost calculation as noted in the code comment
- Essential for proper query plan structure when queries contain both grouping and non-grouping elements
- Handles complex expressions by flattening them into component variables for subplan computation