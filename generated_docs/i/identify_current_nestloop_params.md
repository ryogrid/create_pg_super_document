# identify_current_nestloop_params

## Location
[src/backend/optimizer/util/paramassign.c:582-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L582-L636)

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
  - [NestLoopParam](../N/NestLoopParam.md)
  - [bms_is_member](../b/bms_is_member.md)
  - foreach_delete_current
  - [bms_intersect](../b/bms_intersect.md)
  - [PlaceHolderVar](../P/PlaceHolderVar.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [find_placeholder_info](../f/find_placeholder_info.md)
- Called from (representative examples):
  - [create_nestloop_plan](../c/create_nestloop_plan.md)

## Notes and Other Information
This function contains important logic for handling outer join identity transformations, specifically identity 3. The nullingrel adjustment is a workaround for cases where the parser creates lateral references with different nullingrel specifications than what will be available at execution. The comments note this weakens setrefs.c's cross-checking capabilities but avoids the expense of generating multiple versions of laterally-parameterized subqueries.