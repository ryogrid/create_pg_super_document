# build_base_rel_tlists

## Location
[src/backend/optimizer/plan/initsplan.c:234-278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L234-L278)

## Overview
Adds targetlist entries for each variable needed in the query's final target list and HAVING clause to the appropriate base relations.

## Definition
```c
void build_base_rel_tlists(PlannerInfo *root, List *final_tlist)
```

## Detailed Description
This function is responsible for analyzing the query's final target list and HAVING clause to identify all variables that will be needed in the final output. It then ensures these variables are marked as needed by "relation 0" so they will propagate up through all join plan steps.

The function operates in two main phases:
1. **Final target list processing**: Extracts all variables from the final target list using pull_var_clause() with flags to include aggregates, window functions, and placeholders
2. **HAVING clause processing**: If present, extracts variables from the HAVING clause (which can contain aggregates but not window functions)

For each set of variables found, it calls add_vars_to_targetlist() with a bitmapset containing only relation 0, ensuring these variables are considered essential and will be available at all levels of the join tree.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and state
- `final_tlist`: List representing the query's final target list

## Dependencies
- Functions called/Symbols referenced:
  - [pull_var_clause](../p/pull_var_clause.md)
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md)
  - [bms_make_singleton](bms_make_singleton.md)
  - [list_free](../l/list_free.md)
- Constants used:
  - PVC_RECURSE_AGGREGATES
  - PVC_RECURSE_WINDOWFUNCS
  - PVC_INCLUDE_PLACEHOLDERS
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)
  - [distribute_row_identity_vars](../d/distribute_row_identity_vars.md)

## Notes and Other Information
- [Variables](../V/Variables.md) are marked as needed by "relation 0" to ensure propagation through all join steps
- HAVING clause processing excludes window functions (only includes aggregates)
- Uses different flag combinations for final_tlist vs HAVING clause variable extraction
- Memory management includes explicit list_free() calls for extracted variable lists
- Part of the target list management section in the query planner
- Located in src/backend/optimizer/plan/initsplan.c at lines 234-278