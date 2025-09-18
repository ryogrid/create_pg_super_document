# fix_scan_expr

## Location
src/backend/optimizer/plan/setrefs.c: 2160 - 2194

## Overview
Performs set_plan_references processing on scan-level expressions by adjusting variable references, replacing parameters, and updating operator information.

## Definition


## Detailed Description
The fix_scan_expr function is a core component of PostgreSQL's query plan reference fixing mechanism. It processes expressions at the scan level by performing several transformations:

1. **Variable Reference Adjustment**: Increments all Vars' varnos by rtoffset to adjust for range table changes
2. **Parameter Replacement**: Replaces PARAM_MULTIEXPR Params with appropriate substitutions
3. **PlaceHolderVar Expansion**: Expands PlaceHolderVars to their actual expressions
4. **Aggregate Reference Handling**: Replaces Aggref nodes that should be substituted with initplan output parameters
5. **AlternativeSubPlan Processing**: Chooses the best implementation for AlternativeSubPlans
6. **Operator Information**: Looks up operator opcode information for OpExpr and related nodes
7. **OID Collection**: Adds OIDs from regclass Const nodes into root->glob->relationOids

The function optimizes performance by choosing between two processing paths: if no transformations are needed (rtoffset == 0 and no special parameters/placeholders), it processes the tree in-place using fix_scan_expr_walker. Otherwise, it creates a copy using fix_scan_expr_mutator.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information and context
- : The expression tree node to be processed and potentially modified
- : Integer offset to add to variable numbers (varnos) for range table adjustment
- : Estimated number of times this expression will be executed (for optimization decisions)

## Dependencies
- Functions called/Symbols referenced:
  - [fix_scan_expr_context](fix_scan_expr_context.md)
  - [fix_scan_expr_mutator](fix_scan_expr_mutator.md)
  - [fix_scan_expr_walker](fix_scan_expr_walker.md)
- Called from (representative examples):
  - fix_scan_list
  - [set_plan_refs](../s/set_plan_refs.md)

## Notes and Other Information
- The function includes an important optimization: when no transformations are required, it modifies the input tree in-place rather than copying it, which provides measurable performance benefits on trivial queries
- The decision logic checks multiple conditions (rtoffset, multiexpr_params, placeholders, minmax aggregates, and alternative subplans) to determine the processing path
- This function is part of the broader set_plan_references mechanism that ensures all expression references are correctly adjusted after query planning
- Located in src/backend/optimizer/plan/setrefs.c:2160-2194