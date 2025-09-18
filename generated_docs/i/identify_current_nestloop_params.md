# identify_current_nestloop_params

## Location
src/backend/optimizer/util/paramassign.c: 582 - 636

## Overview
Identifies NestLoopParams that should be supplied by a NestLoop plan node with specified lefthand relations and removes them from the active parameter list.

## Definition
List *identify_current_nestloop_params(PlannerInfo *root, Relids leftrelids)

## Detailed Description
This function scans through the current outer parameters list (root->curOuterParams) to identify parameters that can be supplied by the lefthand relations of a NestLoop operation. It handles both regular Vars and PlaceHolderVars (PHVs), ensuring proper nullingrel handling when outer join identity transformations have been applied.

The function performs a critical optimization step by adjusting nullingrel sets in the returned parameters to match what is actually available from the outer side of the join. This is necessary when outer join identity 3 has been applied, where lateral references may have been created with different nullingrel specifications than what will be available at execution time.

When parameters are identified as suitable, they are removed from the root->curOuterParams list and added to the result list. The function also modifies the Var and PHV nodes in-place to adjust their nullingrel sets using bms_intersect with leftrelids.

## Parameters / Member Variables
- : PlannerInfo structure containing the current planning context and outer parameters list
- : Relids bitmap representing the relations available from the left/outer side of the NestLoop

## Dependencies
- Functions called/Symbols referenced:
  - NestLoopParam
  - bms_is_member
  - foreach_delete_current
  - bms_intersect
  - PlaceHolderVar
  - bms_is_subset
  - find_placeholder_info
- Called from (representative examples):
  - create_nestloop_plan

## Notes and Other Information
This function contains important logic for handling outer join identity transformations, specifically identity 3. The nullingrel adjustment is a workaround for cases where the parser creates lateral references with different nullingrel specifications than what will be available at execution. The comments note this weakens setrefs.c's cross-checking capabilities but avoids the expense of generating multiple versions of laterally-parameterized subqueries.