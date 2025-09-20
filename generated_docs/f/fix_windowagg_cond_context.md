# fix_windowagg_cond_context

## Location
[src/backend/optimizer/plan/setrefs.c:90-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L90-L96)

## Overview
A specialized context structure used for fixing expressions in WindowAgg node run conditions, providing the minimal context needed for variable reference resolution.

## Definition

```c
typedef struct
{
	PlannerGlobal *glob;
	Query	   *query;
} flatten_rtes_walker_context;
```
## Detailed Description
The  structure is a specialized context used specifically for fixing expressions in WindowAgg node run conditions. This structure is a simplified version of the upper expression context, containing only the essential fields needed for resolving variable references in window function run conditions.

WindowAgg nodes use run conditions to optimize window function execution by skipping computation when certain conditions are not met. The expressions in these run conditions need to be properly fixed to reference the correct variables from the subplan's target list.

This context is used by specialized expression mutator functions that handle the specific requirements of WindowAgg run condition expressions, ensuring that variable references are correctly mapped to the subplan's output.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo structure containing global planner state and context
- `subplan_itlist`: Indexed target list for the subplan providing input to the WindowAgg node
- `newvarno`: New variable number to assign to variables in the WindowAgg context

## Dependencies
- Functions called/Symbols referenced:
  - [PlannerInfo](../P/PlannerInfo.md) (planner's main state structure)
  - [indexed_tlist](../i/indexed_tlist.md) (indexed target list structure)
- Called from (representative examples):
  - [fix_windowagg_condition_expr_mutator](fix_windowagg_condition_expr_mutator.md)
  - [fix_windowagg_condition_expr](fix_windowagg_condition_expr.md)

## Notes and Other Information
- Specialized for WindowAgg run condition expressions specifically
- Simplified compared to fix_upper_expr_context, containing only essential fields
- Part of the window function optimization framework in PostgreSQL
- Used to ensure proper variable binding in window function run conditions
- The context enables efficient execution of window functions by allowing condition-based skipping