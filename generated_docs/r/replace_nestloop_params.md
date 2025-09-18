# replace_nestloop_params

## Location
src/backend/optimizer/plan/createplan.c: 4936 - 4942

## Overview
Replaces outer-relation Vars and PlaceHolderVars in expressions with nestloop Params, facilitating parameter passing between nested loop joins.

## Definition


## Detailed Description
This function serves as a wrapper for the nested loop parameter replacement mechanism in PostgreSQL's query planner. It takes an expression tree and replaces all Vars and PlaceHolderVars that belong to outer relations (identified by root->curOuterRels) with Params. This transformation is essential for nested loop joins where inner relations need to reference values from outer relations through parameters rather than direct variable references.

The function delegates the actual tree walking and transformation work to , following PostgreSQL's common pattern of having a simple wrapper function that calls a more complex mutator function to perform recursive tree transformations.

When a Var or PlaceHolderVar is replaced with a Param, corresponding entries are added to root->curOuterParams if they don't already exist, ensuring proper parameter management during query execution.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : Node tree representing the expression to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - replace_nestloop_params_mutator
- Called from (representative examples):
  - build_path_tlist
  - create_append_plan  
  - create_memoize_plan
  - create_seqscan_plan
  - create_samplescan_plan
  - create_indexscan_plan
  - create_bitmap_scan_plan
  - create_tidscan_plan
  - create_tidrangescan_plan
  - create_subqueryscan_plan
  - create_functionscan_plan
  - create_tablefuncscan_plan
  - create_valuesscan_plan
  - create_ctescan_plan
  - create_namedtuplestorescan_plan
  - create_resultscan_plan
  - create_worktablescan_plan
  - create_foreignscan_plan
  - create_customscan_plan
  - create_nestloop_plan
  - create_mergejoin_plan
  - create_hashjoin_plan
  - fix_indexqual_clause

## Notes and Other Information
This function is part of PostgreSQL's supporting routines for plan creation and is used extensively throughout the plan creation process for various scan and join operations. It's a critical component in the nested loop join implementation, ensuring that outer relation references are properly parameterized for efficient execution. The function is defined in src/backend/optimizer/plan/createplan.c at lines 4936-4942.