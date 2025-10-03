# JsonTablePlanJoinNextRow

## Location
[src/backend/utils/adt/jsonpath_exec.c:4411-4437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4411-L4437)

## Overview
JsonTablePlanJoinNextRow implements a UNION-style iteration over two sibling JSON table plans, fetching rows sequentially from left and right child plans.

## Definition

```c
static bool
JsonTablePlanJoinNextRow(JsonTablePlanState *planstate)
```
## Detailed Description
This function implements the row iteration logic for JsonTableSiblingJoin plans by performing a sequential UNION operation between left and right sibling plans. It first attempts to fetch a row from the left sibling plan, and only when the left sibling is exhausted does it begin fetching from the right sibling plan. The function maintains the UNION semantics by ensuring that all rows from the left plan are returned before any rows from the right plan, effectively concatenating the result sets from both sibling plans.

## Parameters / Member Variables
- `*planstate`: Pointer to JsonTablePlanState structure representing the sibling join plan, which contains references to left and right child plans (planstate->left and planstate->right)
## Dependencies
- Functions called/Symbols referenced:
  - [JsonTablePlanNextRow](JsonTablePlanNextRow.md) (called recursively on left and right sibling plans)
- Called from (representative examples):
  - [JsonTablePlanNextRow](JsonTablePlanNextRow.md) (dispatcher function for different plan types)

## Notes and Other Information
- Returns true if a row was successfully retrieved from either sibling, false when both siblings are exhausted
- Implements a strict left-to-right processing order: right sibling is only accessed after left sibling is completely exhausted
- The function creates a UNION ALL semantic (no deduplication of rows between siblings)
- Comment indicates "Right sibling ran out of row, so there are more rows" but the logic correctly returns false when both siblings are exhausted
- This is one of the simpler plan execution functions, as it delegates all the complex logic to the recursive JsonTablePlanNextRow calls on child plans