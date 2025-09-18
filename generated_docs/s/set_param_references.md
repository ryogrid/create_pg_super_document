# set_param_references

## Location
src/backend/optimizer/plan/setrefs.c: 2497 - 2551

## Overview
Initializes the initParam list in Gather or GatherMerge nodes to contain references to all parameters that need to be evaluated before execution of the node.

## Definition


## Detailed Description
This function is responsible for setting up parameter references in parallel execution nodes (Gather and GatherMerge). It identifies all initplan parameters that are being passed to the plan nodes below the parallel execution boundary and stores them in the node's initParam field. This ensures that worker processes in parallel execution have access to all necessary parameters that were computed by initplans in the main process.

The function walks up the planner hierarchy to collect all initplan parameters from the current and parent query levels, then intersects this set with the external parameters required by the subtree to determine which parameters need to be passed down to parallel workers.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : The Plan node (must be either Gather or GatherMerge) that needs parameter initialization

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - [bms_add_member](../b/bms_add_member.md)
  - lfirst_int
  - [bms_intersect](../b/bms_intersect.md)
- Called from (representative examples):
  - fix_scan_list (src/backend/optimizer/plan/setrefs.c:165)
  - [set_plan_refs](set_plan_refs.md) (src/backend/optimizer/plan/setrefs.c:855)

## Notes and Other Information
- Only applies to Gather and GatherMerge nodes, which are the entry points for parallel execution
- The function asserts that the input plan is either a Gather or GatherMerge node
- Parameter collection traverses the entire planner hierarchy (root and all parent_root levels)
- The initParam field stores the intersection of external parameters needed by the subtree and parameters available from initplans
- This mechanism is crucial for proper parameter passing in parallel query execution