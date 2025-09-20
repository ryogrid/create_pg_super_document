# replace_nestloop_params

## Location
[src/backend/optimizer/plan/createplan.c:4936-4942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L4936-L4942)

## Overview
Replaces outer-relation Vars and PlaceHolderVars in expressions with nestloop Params, facilitating parameter passing between nested loop joins.

## Definition

```c
static Node *
replace_nestloop_params(PlannerInfo *root, Node *expr)
```
## Detailed Description
This function serves as a wrapper for the nested loop parameter replacement mechanism in PostgreSQL's query planner. It takes an expression tree and replaces all Vars and PlaceHolderVars that belong to outer relations (identified by root->curOuterRels) with Params. This transformation is essential for nested loop joins where inner relations need to reference values from outer relations through parameters rather than direct variable references.

The function delegates the actual tree walking and transformation work to , following PostgreSQL's common pattern of having a simple wrapper function that calls a more complex mutator function to perform recursive tree transformations.

When a Var or PlaceHolderVar is replaced with a Param, corresponding entries are added to root->curOuterParams if they don't already exist, ensuring proper parameter management during query execution.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : Node tree representing the expression to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - [replace_nestloop_params_mutator](replace_nestloop_params_mutator.md)
- Called from (representative examples):
  - [build_path_tlist](../b/build_path_tlist.md)
  - [create_append_plan](../c/create_append_plan.md)  
  - [create_memoize_plan](../c/create_memoize_plan.md)
  - [create_seqscan_plan](../c/create_seqscan_plan.md)
  - [create_samplescan_plan](../c/create_samplescan_plan.md)
  - [create_indexscan_plan](../c/create_indexscan_plan.md)
  - [create_bitmap_scan_plan](../c/create_bitmap_scan_plan.md)
  - [create_tidscan_plan](../c/create_tidscan_plan.md)
  - [create_tidrangescan_plan](../c/create_tidrangescan_plan.md)
  - [create_subqueryscan_plan](../c/create_subqueryscan_plan.md)
  - [create_functionscan_plan](../c/create_functionscan_plan.md)
  - [create_tablefuncscan_plan](../c/create_tablefuncscan_plan.md)
  - [create_valuesscan_plan](../c/create_valuesscan_plan.md)
  - [create_ctescan_plan](../c/create_ctescan_plan.md)
  - [create_namedtuplestorescan_plan](../c/create_namedtuplestorescan_plan.md)
  - [create_resultscan_plan](../c/create_resultscan_plan.md)
  - [create_worktablescan_plan](../c/create_worktablescan_plan.md)
  - [create_foreignscan_plan](../c/create_foreignscan_plan.md)
  - [create_customscan_plan](../c/create_customscan_plan.md)
  - [create_nestloop_plan](../c/create_nestloop_plan.md)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md)
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md)
  - [fix_indexqual_clause](../f/fix_indexqual_clause.md)

## Notes and Other Information
This function is part of PostgreSQL's supporting routines for plan creation and is used extensively throughout the plan creation process for various scan and join operations. It's a critical component in the nested loop join implementation, ensuring that outer relation references are properly parameterized for efficient execution. The function is defined in src/backend/optimizer/plan/createplan.c at lines 4936-4942.