# expression_returns_set_rows

## Location
[src/backend/optimizer/util/clauses.c:289-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L289-L329)

## Overview
Estimates the number of rows returned by a set-returning expression, returning 1.0 if the expression is not set-returning.

## Definition

```c
double
expression_returns_set_rows(PlannerInfo *root, Node *clause)
```
## Detailed Description
This function analyzes a given expression to determine how many rows it will return when executed. It specifically handles set-returning functions (SRFs) and operators. The function only examines the top-level function or operator and does not recurse into nested expressions, as the multipliers for inner SRFs are accounted for separately in the PostgreSQL query planner.

The function checks two main types of expressions:
1. **FuncExpr**: Function expressions with the  flag indicating they return sets
2. **OpExpr**: Operator expressions with the  flag indicating they return sets

For both types, it delegates to  to obtain the estimated row count and applies  to ensure the result is within reasonable bounds.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and statistics
- : The expression node to analyze for set-returning behavior

## Dependencies
- Functions called/Symbols referenced:
  - [clamp_row_est](../c/clamp_row_est.md)
  - [get_function_rows](../g/get_function_rows.md)
  - [set_opfuncid](../s/set_opfuncid.md)
  - [FuncExpr](../F/FuncExpr.md) (node type check)
  - [OpExpr](../O/OpExpr.md) (node type check)
- Called from (representative examples):
  - [set_function_size_estimates](../s/set_function_size_estimates.md)
  - [create_set_projection_path](../c/create_set_projection_path.md)
  - [estimate_num_groups](estimate_num_groups.md)

## Notes and Other Information
- This function should be kept in sync with  in 
- Returns 1.0 for non-set-returning expressions as the default case
- Only examines top-level expressions to avoid double-counting nested SRF multipliers
- Uses  to ensure row estimates remain within sane bounds
- Part of PostgreSQL's cost estimation framework for query optimization