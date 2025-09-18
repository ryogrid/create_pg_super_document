# find_placeholder_info

## Location
[src/backend/optimizer/util/placeholder.c:83-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/placeholder.c#L83-L184)

## Overview
Retrieves or creates a PlaceHolderInfo structure for a given PlaceHolderVar, managing the metadata needed for proper evaluation placement and optimization of placeholder expressions in query plans.

## Definition
```c
PlaceHolderInfo *find_placeholder_info(PlannerInfo *root, PlaceHolderVar *phv)
```

## Detailed Description
The `find_placeholder_info` function is responsible for finding or creating PlaceHolderInfo structures, which contain essential metadata about where and how PlaceHolderVars should be evaluated during query execution. The function first attempts to locate an existing PlaceHolderInfo using a fast array lookup. If not found, it creates a new one, analyzing the expressions referenced variables to determine evaluation placement, lateral references, and other optimization parameters.

The function performs sophisticated analysis to separate LATERAL references (variables outside the PHVs syntactic scope) from evaluation requirements, and handles dynamic memory allocation for the placeholder_array as needed. It also recursively processes any nested PlaceHolderVars within the expression.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and placeholder management structures
- `phv`: PlaceHolderVar for which to find or create the corresponding PlaceHolderInfo

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating PlaceHolderInfo)
  - copyObject (for copying PlaceHolderVar)
  - [pull_varnos](../p/pull_varnos.md) (for extracting variable references)
  - [bms_difference](../b/bms_difference.md), bms_int_members, bms_is_empty, bms_copy (bitmap set operations)
  - [get_typavgwidth](../g/get_typavgwidth.md), exprType, exprTypmod (type analysis functions)
  - repalloc0_array, palloc0_array (memory management)
  - [find_placeholders_in_expr](find_placeholders_in_expr.md) (recursive placeholder processing)
- Called from (representative examples):
  - [set_rel_width](../s/set_rel_width.md) (in costsize.c)
  - [replace_nestloop_params_mutator](../r/replace_nestloop_params_mutator.md) (in createplan.c)
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md) (in initsplan.c)
  - build_joinrel_tlist (in relnode.c)

## Notes and Other Information
- Only callable after query_planner() has started due to placeholder freezing constraints
- Uses both placeholder_list (for iteration) and placeholder_array (for fast lookup) data structures
- Dynamically expands placeholder_array using exponential growth when needed
- Separates ph_lateral (LATERAL references) from ph_eval_at (evaluation requirements)
- Forces evaluation at syntactic location if no contained variables are found within scope
- Recursively processes nested PlaceHolderVars in the expression
- Throws error if called after placeholders are frozen (too late in planning process)