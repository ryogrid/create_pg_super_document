# assign_special_exec_param

## Location
src/backend/optimizer/util/paramassign.c: 664 - 671

## Overview
Assigns a unique execution parameter ID for special runtime signaling purposes without creating an actual Param node.

## Definition
int assign_special_exec_param(PlannerInfo *root)

## Detailed Description
This function allocates a special parameter ID that is used for internal runtime signaling mechanisms rather than carrying actual data values. These special parameters serve purposes such as connecting recursive union nodes to their worktable scan nodes or forcing plan re-evaluation within the EvalPlanQual mechanism.

Unlike regular execution parameters, no actual Param node is created with this ID. The function reserves a slot in the paramExecTypes array by appending InvalidOid, indicating that no specific data type is associated with this parameter since it doesn't carry actual values.

The returned parameter ID can be used by execution nodes to coordinate special runtime behaviors without the overhead of full parameter value passing.

## Parameters / Member Variables
- : PlannerInfo structure containing the global planning context with paramExecTypes list

## Dependencies
- Functions called/Symbols referenced:
  - lappend_oid
  - InvalidOid (implicitly referenced)
- Called from (representative examples):
  - create_gather_plan
  - create_gather_merge_plan
  - subquery_planner
  - grouping_planner
  - SS_process_ctes

## Notes and Other Information
This function is primarily used in parallel query execution and recursive query processing. The special parameters created by this function enable coordination between different execution nodes without the overhead of full parameter value management. The InvalidOid type marker clearly distinguishes these signaling parameters from data-carrying parameters.